"""Catalogo, linaje y reporte (sin Spark): completos, al dia en ENTREGABLES/ y sin datos reales.

Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse.governance import contracts as c
from lakehouse.governance import lineage
from lakehouse.governance.access import Probe
from lakehouse.governance.catalog import render_catalog
from lakehouse.governance.quality import ERROR, WARN, CheckResult, QualityRun
from lakehouse.governance.report import render_report

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "ENTREGABLES" / "03_calidad_y_linaje"
EM_DASH = "\u2014"


def test_every_source_is_a_known_node():
    assert lineage.unknown_sources() == []


def test_mermaid_has_every_table_and_edge():
    graph = lineage.mermaid()
    for t in c.ALL_TABLES:
        assert t.qualified_name.replace(".", "_") in graph
    assert "bronze_spotify_streams --> silver_plays" in graph
    assert "gold_fact_plays --> consumer_ml" in graph


def test_column_lineage_points_to_real_columns():
    def check(ref):
        layer, table, column = ref.split(".", 2)
        contract = c.by_name(f"{layer}.{table}")
        for name in column.split(" / "):
            if not name.startswith("("):
                assert name in contract.column_names, ref

    for item in lineage.COLUMN_LINEAGE:
        check(item.target)
        for src in item.sources:
            check(src)


def test_generated_docs_are_up_to_date():
    if not OUT.exists():
        pytest.skip("ENTREGABLES/ no esta en este entorno (contenedor)")
    hint = "corre: uv run python -m lakehouse.governance docs"
    assert (OUT / "CATALOGO.md").read_text(encoding="utf-8") == render_catalog(), hint
    assert (OUT / "LINAJE.md").read_text(encoding="utf-8") == lineage.render_lineage(), hint


def test_catalog_covers_every_table_without_em_dash():
    text = render_catalog()
    for t in c.ALL_TABLES:
        assert f"### `{t.qualified_name}`" in text
    assert EM_DASH not in text and EM_DASH not in lineage.render_lineage()


def test_report_renders_counts_only():
    run = QualityRun(
        results=[
            CheckResult("silver.plays", "unicos", "unica por (play_id)", ERROR, True),
            CheckResult("silver.plays", "regla", "ts_start <= ts_end", ERROR, False, 2, "2 de 10 filas"),
            CheckResult(
                "entre capas", "reconciliacion", "plata = oro", ERROR, True, 0, "plays = fact: 10 vs 10"
            ),
            CheckResult(
                "bronze._ingestion_log", "frescura", "ingesta reciente", WARN, False, 1, "hace 40 dias"
            ),
        ],
        row_counts={"silver.plays": 10},
    )
    probes = [Probe("bi", "gold", "escribir", False, False), Probe("bi", "gold", "listar", True, None, "X")]
    text = render_report(run, probes, root_present=False, generated_at=datetime(2026, 11, 23, tzinfo=UTC))
    assert "1 fallas" in text and "1 avisos" in text
    assert "1 de 2" in text  # pruebas de acceso como se esperaba
    assert "`svc-bi`" in text and "plata = oro" in text and "### `silver.plays` (10 filas; 1 de 2 OK)" in text
    assert "### `gold.fact_plays` (no existe" in text
    assert EM_DASH not in text
