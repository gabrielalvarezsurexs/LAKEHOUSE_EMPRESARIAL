"""Tablero como codigo (bi/): export valido; cada dataset y grafica corre en DuckDB con el esquema de Oro.

El SQL de bi/sql se prueba contra tablas en memoria creadas desde los contratos (sin MinIO ni Superset).
Responsable: Emilio Rendon Trias (Analista de Datos / BI)
"""

import importlib.util
import io
import json
import re
import sys
import zipfile
from pathlib import Path

import duckdb
import pytest
import yaml

from lakehouse.governance import lineage
from lakehouse.governance.contracts import DELTA_TABLES

ROOT = Path(__file__).resolve().parents[2]
EXPORT = ROOT / "ENTREGABLES" / "05_tablero_bi" / "tablero_export.zip"
GOLD_SCAN = re.compile(r"delta_scan\('s3://gold/(\w+)'\)")

# El contenedor lakehouse solo monta src/ y tests/: aqui corre en local y en el job sin Docker de CI.
if not (ROOT / "bi" / "dashboard.py").exists():
    pytest.skip("bi/ no esta montado (contenedor)", allow_module_level=True)


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses lo necesita registrado
    spec.loader.exec_module(module)
    return module


dash = load("bi_dashboard", ROOT / "bi" / "dashboard.py")

DUCK_TYPES = {
    "STRING": "VARCHAR",
    "INT": "INTEGER",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "ARRAY<STRING>": "VARCHAR[]",
}
SAMPLE = {
    "VARCHAR": "'x'",
    "INTEGER": "1",
    "BIGINT": "1",
    "DOUBLE": "0.5",
    "BOOLEAN": "true",
    "DATE": "DATE '2024-01-01'",
    "TIMESTAMP": "TIMESTAMP '2024-01-01 10:00:00'",
    "VARCHAR[]": "['x']",
}


@pytest.fixture(scope="module")
def gold():
    """Oro en memoria: cada tabla con el esquema de su contrato y una fila que cruza con las demas."""
    con = duckdb.connect()
    for t in DELTA_TABLES:
        if t.layer != "gold":
            continue
        cols = [(c.name, DUCK_TYPES[c.dtype]) for c in t.columns]
        con.execute(f"CREATE TABLE gold_{t.name} ({', '.join(f'{n} {ty}' for n, ty in cols)})")
        con.execute(f"INSERT INTO gold_{t.name} VALUES ({', '.join(SAMPLE[ty] for _, ty in cols)})")
    yield con
    con.close()


def local_sql(sql: str) -> str:
    return GOLD_SCAN.sub(lambda m: f"gold_{m.group(1)}", sql)


def test_every_gold_table_in_bi_sql_exists_and_matches_lineage():
    used = {m for ds in dash.DATASETS.values() for m in GOLD_SCAN.findall(ds.sql())}
    gold_names = {t.name for t in DELTA_TABLES if t.layer == "gold"}
    assert used <= gold_names, "bi/sql lee una tabla de Oro que no tiene contrato"
    assert all("s3://" not in GOLD_SCAN.sub("", ds.sql()) for ds in dash.DATASETS.values()), "solo s3://gold"
    bi_node = next(n for n in lineage.nodes() if n.id == "consumer.bi")
    assert set(bi_node.sources) == {f"gold.{t}" for t in used}


@pytest.mark.parametrize("name", list(dash.DATASETS))
def test_dataset_sql_gives_the_declared_columns(gold, name):
    ds = dash.DATASETS[name]
    described = gold.execute(f"DESCRIBE {local_sql(ds.sql())}").fetchall()
    assert {row[0]: row[1] for row in described} == ds.columns
    assert ds.dttm in ds.columns and ds.columns[ds.dttm] == "DATE"


@pytest.mark.parametrize("key", [c.key for c in dash.CHARTS])
def test_chart_query_runs_on_gold(gold, key):
    chart = dash.CHARTS_BY_KEY[key]
    ds = dash.DATASETS[chart.dataset]
    assert set(chart.all_columns()) <= set(ds.columns)
    rows = gold.execute(dash.test_query(chart, local_sql(ds.sql()))).fetchall()
    # Con una fila por tabla toda grafica regresa algo, salvo las de filtro fijo (split = 'test').
    assert rows or chart.where


def test_saved_query_applies_the_time_grain():
    """Sin BASE_AXIS la consulta guardada agrupa por dia (1008 filas en 'Horas por mes')."""
    for chart in (c for c in dash.CHARTS if c.time_grain):
        columns = json.loads(dash.query_context(chart))["queries"][0]["columns"]
        axis = [c for c in columns if isinstance(c, dict)]
        assert axis == [dash.query_columns(chart)[columns.index(axis[0])]], chart.key
        assert axis[0]["timeGrain"] == chart.time_grain and axis[0]["columnType"] == "BASE_AXIS"
        assert chart.x_axis not in columns  # no tambien como columna simple


def test_layout_places_every_chart_once_within_12_columns():
    placed = [key for items, _ in dash.LAYOUT for key, _ in items if key != "intro"]
    assert sorted(placed) == sorted(c.key for c in dash.CHARTS)
    assert all(sum(width for _, width in items) <= 12 for items, _ in dash.LAYOUT)


def test_bundle_is_a_valid_superset_export():
    files = {path: yaml.safe_load(text) for path, text in dash.bundle().items()}
    prefix = f"{dash.ROOT}/"
    assert all(p.startswith(prefix) for p in files)
    docs = {p[len(prefix) :]: doc for p, doc in files.items()}
    assert docs["metadata.yaml"]["type"] == "Dashboard"
    databases = [d for p, d in docs.items() if p.startswith("databases/")]
    datasets = [d for p, d in docs.items() if p.startswith("datasets/")]
    charts = [d for p, d in docs.items() if p.startswith("charts/")]
    (dashboard,) = [d for p, d in docs.items() if p.startswith("dashboards/")]
    uuids = [d["uuid"] for d in databases + datasets + charts + [dashboard]]
    assert len(uuids) == len(set(uuids))
    assert {d["database_uuid"] for d in datasets} == {databases[0]["uuid"]}
    assert {c["dataset_uuid"] for c in charts} <= {d["uuid"] for d in datasets}
    in_layout = {
        n["meta"]["uuid"]
        for n in dashboard["position"].values()
        if isinstance(n, dict) and n.get("type") == "CHART"
    }
    assert in_layout == {c["uuid"] for c in charts}
    for chart in charts:
        metrics = {
            m["metric_name"] for d in datasets if d["uuid"] == chart["dataset_uuid"] for m in d["metrics"]
        }
        params = chart["params"]
        used = set(params.get("metrics", [])) | ({params["metric"]} if "metric" in params else set())
        assert used <= metrics, chart["slice_name"]


def test_native_filters_target_columns_of_plays():
    plays = dash.DATASETS["plays"]
    for f in dash.native_filters():
        for target in f["targets"]:
            if target:
                assert target["datasetUuid"] == plays.uuid and target["column"]["name"] in plays.columns


def test_dataset_sql_drops_the_header_comments():
    for ds in dash.DATASETS.values():
        assert not ds.sql().startswith("--") and ds.sql().upper().startswith("SELECT")


def test_entregables_export_is_up_to_date():
    """La copia del export en ENTREGABLES es la que genera el codigo (regenerar: ver bi/dashboard.py)."""
    assert EXPORT.exists(), "Falta el export: uv run python bi/dashboard.py --out " + str(
        EXPORT.relative_to(ROOT)
    )
    with zipfile.ZipFile(io.BytesIO(EXPORT.read_bytes())) as zf:
        saved = {name: zf.read(name).decode("utf-8") for name in zf.namelist()}
    assert saved == dash.bundle(), (
        "Regenera: uv run python bi/dashboard.py --out ENTREGABLES/05_tablero_bi/tablero_export.zip"
    )


def test_superset_config_builds_the_minio_secret(monkeypatch):
    monkeypatch.setenv("SUPERSET_SECRET_KEY", "k")
    monkeypatch.setenv("MINIO_SECRET_BI", "it's")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    from sqlalchemy import event
    from sqlalchemy.engine import Engine

    config = load("superset_config_test", ROOT / "infra" / "superset" / "superset_config.py")
    try:
        sql = config.minio_secret_sql()
        assert "KEY_ID 'svc-bi'" in sql and "SECRET 'it''s'" in sql
        assert "ENDPOINT 'minio:9000'" in sql and "USE_SSL false" in sql and "SCOPE 's3://gold'" in sql
    finally:
        event.remove(Engine, "connect", config._duckdb_reads_gold)
