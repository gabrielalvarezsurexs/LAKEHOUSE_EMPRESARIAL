"""Assets de Dagster: uno por tabla (de los contratos), agrupados por capa, mas el reporte y la salud.

Cada paso de steps.py es un multi_asset que corre su CLI en otro proceso; las dependencias salen de los
sources de los contratos, asi el grafo de Dagster es el mismo de LINAJE.md (D23).
Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

import json
import tempfile
import time
from pathlib import Path

import dagster as dg

from lakehouse.governance.contracts import DELTA_TABLES, ML_MODEL_NODE
from lakehouse.orchestration import steps
from lakehouse.orchestration.steps import (
    EXTERNAL_SOURCES,
    ML_MODEL_NAME,
    MUSICBRAINZ_CACHE,
    Step,
    StepError,
)

LAYER_KINDS = {
    "landing": {"minio", "json"},
    "bronze": {"spark", "delta", "minio"},
    "silver": {"spark", "delta", "minio"},
    "gold": {"spark", "delta", "minio"},
    "ml": {"mlflow", "scikitlearn"},
}
REPORT_KEY = dg.AssetKey(["governance", "calidad_y_linaje"])
HEALTH_KEY = dg.AssetKey(["monitoring", "lakehouse_health"])
HEALTH_CHECK = "sin_alertas"
RAW_LOCATIONS = {
    "landing.spotify_streams_source": "s3://landing/spotify/<fecha>/*.json",
    MUSICBRAINZ_CACHE: "s3://landing/musicbrainz/<fuente>/*.json",
    ML_MODEL_NODE: f"models:/{ML_MODEL_NAME} (MLflow, http://localhost:5000)",
}
ML_GROUP = "ml"  # features, modelo y predicciones juntos en el grafo, aunque las tablas vivan en Oro


def asset_key(node: str) -> dg.AssetKey:
    """'capa.tabla' a la llave de Dagster [capa, tabla]."""
    return dg.AssetKey(node.split(".", 1))


def _column_schema(contract) -> dg.TableSchema:
    return dg.TableSchema(
        columns=[
            dg.TableColumn(
                c.name,
                c.dtype.lower(),
                c.description,
                constraints=dg.TableColumnConstraints(nullable=c.nullable),
                tags={"privacy": c.privacy} if c.pii else None,
            )
            for c in contract.columns
        ]
    )


def table_spec(node: str, step: Step) -> dg.AssetSpec:
    """Spec de un nodo: descripcion, columnas, dueno y clasificacion salen del contrato."""
    layer, name = node.split(".", 1)
    contract = steps.contract_of(node)
    metadata: dict = {
        "step": step.name,
        "minio_user": f"svc-{step.role}",
        "location": RAW_LOCATIONS.get(node, f"s3a://{layer}/{name}"),
    }
    tags = {"layer": layer}
    description = step.description
    if contract is not None:
        description = contract.description
        metadata |= {
            "dagster/column_schema": _column_schema(contract),
            "dagster/table_name": node,
            "owner": contract.owner,
            "classification": contract.classification,
        }
        tags["classification"] = contract.classification
    return dg.AssetSpec(
        asset_key(node),
        deps=[asset_key(s) for s in steps.sources_of(node)],
        description=description,
        metadata=metadata,
        group_name=ML_GROUP if step.role == "ml" else layer,
        owners=[f"team:{step.role}"],
        tags=tags,
        kinds=LAYER_KINDS[layer],
    )


def _failure(exc: StepError) -> dg.Failure:
    tail = "\n".join(exc.tail[-15:])
    return dg.Failure(
        description=str(exc),
        metadata={"exit_code": exc.code, "last_lines": dg.MetadataValue.md(f"```\n{tail}\n```")},
    )


def step_asset(step: Step) -> dg.AssetsDefinition:
    retry = (
        dg.RetryPolicy(max_retries=step.retries, delay=60, backoff=dg.Backoff.EXPONENTIAL)
        if step.retries
        else None
    )

    @dg.multi_asset(
        name=step.name,
        specs=[table_spec(node, step) for node in step.outputs],
        description=step.description,
        retry_policy=retry,
    )
    def _run(context: dg.AssetExecutionContext):
        cmd = step.command()
        context.log.info(f"Paso {step.name} con svc-{step.role}: {' '.join(cmd[1:])}")
        start = time.monotonic()
        try:
            steps.run_command(cmd, context.log.info)
        except StepError as exc:
            raise _failure(exc) from exc
        seconds = round(time.monotonic() - start, 1)
        for node in step.outputs:
            yield dg.MaterializeResult(asset_key=asset_key(node), metadata={"step_seconds": seconds})

    return _run


def external_specs() -> list[dg.AssetSpec]:
    """Fuentes de fuera: se ven en el grafo pero no se materializan."""
    return [
        dg.AssetSpec(asset_key(node), description=text, group_name="ext", kinds={"source"})
        for node, text in EXTERNAL_SOURCES.items()
    ]


def run_json_command(context, cmd: list[str], out: Path) -> dict:
    """Corre un CLI que escribe un JSON (codigo 1 = encontro fallas, no que se cayo) y lo lee."""
    try:
        steps.run_command(cmd, context.log.info, ok_codes=(0, 1))
    except StepError as exc:
        raise _failure(exc) from exc
    if not out.exists():
        raise dg.Failure(description=f"El comando no dejo su resultado en {out.name}")
    return json.loads(out.read_text(encoding="utf-8"))


@dg.asset(
    key=REPORT_KEY,
    deps=[asset_key(c.qualified_name) for c in DELTA_TABLES],
    group_name="governance",
    owners=["team:governance"],
    kinds={"spark", "markdown"},
    description=(
        "CALIDAD_Y_LINAJE.md (entregable 3): checks de las tres capas, reconciliacion, frescura y prueba "
        "de accesos de MinIO. Falla si algo bloqueante no pasa."
    ),
)
def quality_report(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    try:
        steps.run_command(steps.python_command(steps.GOVERNANCE_MODULE, "check"), context.log.info)
    except StepError as exc:
        raise _failure(exc) from exc
    return dg.MaterializeResult(metadata={"path": "ENTREGABLES/03_calidad_y_linaje/CALIDAD_Y_LINAJE.md"})


def health_metadata(data: dict) -> dict:
    """Numeros que Dagster grafica en el tiempo, mas una tabla con el detalle (solo conteos)."""
    tables, buckets, alerts = data["tables"], data["buckets"], data["alerts"]
    metadata: dict = {
        "tables_present": sum(t["exists"] for t in tables),
        "rows_total": sum(t["rows"] or 0 for t in tables),
        "alerts_error": sum(a["level"] == "error" for a in alerts),
        "alerts_warn": sum(a["level"] == "warn" for a in alerts),
    }
    for b in buckets:
        metadata[f"mib_{b['bucket']}"] = round((b["bytes"] or 0) / 2**20, 2)
        metadata[f"objects_{b['bucket']}"] = b["objects"] or 0
    lines = [
        "| Tabla | Filas | Archivos | MiB | Version | Ultima operacion | Ultimo commit (UTC) |",
        "|---|---|---|---|---|---|---|",
    ]
    for t in tables:
        if not t["exists"]:
            lines.append(f"| {t['table']} | no existe | | | | | |")
            continue
        mib = round((t["bytes"] or 0) / 2**20, 2)
        lines.append(
            f"| {t['table']} | {t['rows']} | {t['files']} | {mib} | {t['version']} | "
            f"{t['last_operation']} | {t['last_commit']} |"
        )
    metadata["tables"] = dg.MetadataValue.md("\n".join(lines))
    if alerts:
        metadata["alerts"] = dg.MetadataValue.md(
            "\n".join(f"- **{a['level']}** {a['target']}: {a['message']}" for a in alerts)
        )
    return metadata


def health_check_result(data: dict) -> dg.AssetCheckResult:
    alerts = data["alerts"]
    errors = [a for a in alerts if a["level"] == "error"]
    return dg.AssetCheckResult(
        asset_key=HEALTH_KEY,
        check_name=HEALTH_CHECK,
        passed=not alerts,
        severity=dg.AssetCheckSeverity.ERROR if errors else dg.AssetCheckSeverity.WARN,
        metadata={"alerts": len(alerts), "errors": len(errors)},
    )


@dg.asset(
    key=HEALTH_KEY,
    deps=[REPORT_KEY],
    group_name="monitoring",
    owners=["team:devops"],
    kinds={"spark", "minio"},
    check_specs=[
        dg.AssetCheckSpec(
            HEALTH_CHECK,
            asset=HEALTH_KEY,
            description="Sin alertas: tablas presentes, sin archivos chicos ni versiones de sobra",
        )
    ],
    description=(
        "Salud del lakehouse: tamano por bucket, filas, archivos, version y ultimo commit de cada tabla "
        "Delta, con alertas en los logs."
    ),
)
def lakehouse_health(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "health.json"
        cmd = steps.python_command("lakehouse.orchestration", "health", "--json", str(out))
        data = run_json_command(context, cmd, out)
    for a in data["alerts"]:
        text = f"ALERTA [{a['level']}] {a['target']}: {a['message']}"
        (context.log.error if a["level"] == "error" else context.log.warning)(text)
    return dg.MaterializeResult(metadata=health_metadata(data), check_results=[health_check_result(data)])
