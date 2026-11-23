"""Jobs, schedules y sensores: el pipeline corre solo, sin ejecuciones manuales (rubrica).

- Sensor de la carpeta de origen: un JSON nuevo o cambiado dispara la carga a landing.
- Sensor de landing: un archivo nuevo en s3://landing/spotify (de la carga o a mano) dispara Bronce a Oro.
- Schedule diario del pipeline completo y otro de salud cada 6 horas.
- Un sensor de fallas deja la alerta en los logs.
Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

import hashlib
from pathlib import Path

import dagster as dg

from lakehouse.common.config import get_settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import SPOTIFY_STREAMS_SOURCE
from lakehouse.orchestration.assets import HEALTH_KEY, asset_key

log = get_logger("lakehouse.orchestration")

TIMEZONE = "America/Mexico_City"
SENSOR_SECONDS = 30
# Mismos valores que ingestion/landing.py (los roles no se importan entre si; una prueba los compara).
LANDING_BUCKET = "landing"
SPOTIFY_PREFIX = "spotify/"
LANDING_KEY = asset_key(SPOTIFY_STREAMS_SOURCE.qualified_name)

full_job = dg.define_asset_job(
    "lakehouse_full",
    selection=dg.AssetSelection.all(),
    description="Todo: landing, Bronce, generos, Plata, Oro, checks por capa, reporte y salud.",
)
landing_job = dg.define_asset_job(
    "ingest_landing",
    selection=dg.AssetSelection.assets(LANDING_KEY),
    description="Solo la carga de la carpeta de origen a landing.",
)
from_landing_job = dg.define_asset_job(
    "pipeline_from_landing",
    selection=dg.AssetSelection.all() - dg.AssetSelection.assets(LANDING_KEY),
    description="De landing a Oro, con checks, reporte y salud (lo dispara el sensor de landing).",
)
health_job = dg.define_asset_job(
    "health_only", selection=dg.AssetSelection.assets(HEALTH_KEY), description="Solo la salud."
)
JOBS = [full_job, landing_job, from_landing_job, health_job]

daily_schedule = dg.ScheduleDefinition(
    name="daily_pipeline",
    job=full_job,
    cron_schedule="0 6 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Todos los dias a las 6:00 (CDMX): pipeline completo (idempotente: sin cambios no escribe).",
)
health_schedule = dg.ScheduleDefinition(
    name="health_every_6h",
    job=health_job,
    cron_schedule="30 */6 * * *",
    execution_timezone=TIMEZONE,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Salud cada 6 horas: tamanos, versiones, filas y alertas.",
)


def fingerprint(items: list[str]) -> str | None:
    """Huella estable de una lista (orden incluido); None si esta vacia."""
    if not items:
        return None
    return hashlib.sha256("\n".join(items).encode("utf-8")).hexdigest()


def source_items(source_dir: str | Path) -> list[str]:
    """nombre|tamano|mtime de cada JSON de la carpeta de origen (sin leer su contenido)."""
    folder = Path(source_dir)
    if not folder.is_dir():
        return []
    files = sorted(p for p in folder.glob("*.json") if p.is_file())
    return [f"{p.name}|{p.stat().st_size}|{p.stat().st_mtime_ns}" for p in files]


def landing_items(store) -> list[str]:
    return [k for k in store.list_keys(LANDING_BUCKET, SPOTIFY_PREFIX) if k.lower().endswith(".json")]


def evaluate(cursor: str | None, current: str | None, what: str, prefix: str):
    """Decision pura de un sensor: corrida nueva si cambio la huella; la primera vez tambien corre."""
    if current is None:
        return dg.SkipReason(f"No hay JSON en {what}."), cursor
    if current == cursor:
        return dg.SkipReason(f"Sin cambios en {what}."), cursor
    return dg.RunRequest(run_key=f"{prefix}-{current[:16]}"), current


@dg.sensor(
    name="source_folder_sensor",
    job=landing_job,
    minimum_interval_seconds=SENSOR_SECONDS,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Un JSON nuevo o cambiado en SPOTIFY_DATA_DIR dispara la carga a landing.",
)
def source_folder_sensor(context: dg.SensorEvaluationContext):
    try:
        folder = get_settings().spotify_source_dir
    except LakehouseError as exc:
        return dg.SkipReason(f"Configuracion incompleta: {exc}")
    decision, cursor = evaluate(
        context.cursor, fingerprint(source_items(folder)), "la carpeta de origen", "src"
    )
    if cursor != context.cursor:
        context.update_cursor(cursor)
    return decision


@dg.sensor(
    name="landing_sensor",
    job=from_landing_job,
    minimum_interval_seconds=SENSOR_SECONDS,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Un archivo nuevo en s3://landing/spotify/ dispara Bronce, Plata, Oro, checks y reporte.",
)
def landing_sensor(context: dg.SensorEvaluationContext):
    from lakehouse.common.storage import S3ObjectStore

    try:
        # svc-ingestion: el unico usuario que puede listar landing (D17).
        store = S3ObjectStore(get_settings().for_role("ingestion"))
        current = fingerprint(landing_items(store))
    except LakehouseError as exc:
        return dg.SkipReason(f"No se pudo listar landing: {exc}")
    decision, cursor = evaluate(context.cursor, current, "landing", "landing")
    if cursor != context.cursor:
        context.update_cursor(cursor)
    return decision


@dg.run_failure_sensor(
    name="alert_on_failure",
    default_status=dg.DefaultSensorStatus.RUNNING,
    monitored_jobs=JOBS,
    description="Alerta en logs cuando una corrida falla.",
)
def alert_on_failure(context: dg.RunFailureSensorContext):
    run = context.dagster_run
    message = context.failure_event.message or "sin mensaje"
    log.error("ALERTA: la corrida %s de %s fallo: %s", run.run_id[:8], run.job_name, message)


SCHEDULES = [daily_schedule, health_schedule]
SENSORS = [source_folder_sensor, landing_sensor, alert_on_failure]
