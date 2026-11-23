"""Salud del lakehouse: tamano por bucket; filas, version y ultimo commit de cada tabla Delta; alertas.

Corre en su propio proceso (python -m lakehouse.orchestration health): Spark con svc-governance (solo
lectura) y landing con svc-ingestion, el unico que la puede leer (D17). Solo conteos.
Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lakehouse.common.config import LAYERS, Settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import DELTA_TABLES, TableContract

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from lakehouse.common.delta import TableLocator

log = get_logger("lakehouse.orchestration.health")

ERROR, WARN = "error", "warn"
# Quien puede listar cada bucket (D17): governance no ve landing, que trae la IP en claro.
BUCKET_READERS = {
    "landing": "ingestion",
    "bronze": "governance",
    "silver": "governance",
    "gold": "governance",
}
# Archivos chicos: muchos archivos de menos de 1 MiB en promedio piden OPTIMIZE.
SMALL_FILE_BYTES = 1024 * 1024
SMALL_FILE_MIN_FILES = 32
# Cada corrida agrega versiones (Plata se reescribe): muchas piden VACUUM para liberar espacio.
MANY_VERSIONS = 500


@dataclass(frozen=True)
class TableHealth:
    table: str
    exists: bool
    rows: int | None = None
    files: int | None = None
    bytes: int | None = None
    version: int | None = None
    last_operation: str | None = None
    last_commit: str | None = None  # ISO 8601 UTC


@dataclass(frozen=True)
class BucketUsage:
    bucket: str
    objects: int | None
    bytes: int | None
    error: str = ""


@dataclass(frozen=True)
class Alert:
    level: str
    target: str
    message: str


@dataclass
class HealthReport:
    generated_at: str
    tables: list[TableHealth] = field(default_factory=list)
    buckets: list[BucketUsage] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)

    @property
    def errors(self) -> list[Alert]:
        return [a for a in self.alerts if a.level == ERROR]

    def to_dict(self) -> dict:
        return asdict(self)


def table_health(spark: SparkSession, locator: TableLocator, contract: TableContract) -> TableHealth:
    """Detalle y ultimo commit de Delta mas el conteo de filas."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from lakehouse.common.delta import is_delta

    uri = locator.uri(contract)
    if not is_delta(spark, uri):
        return TableHealth(contract.qualified_name, exists=False)
    table = DeltaTable.forPath(spark, uri)
    detail = table.detail().first()
    last = (
        table.history(1).select("version", "operation", F.unix_timestamp("timestamp").alias("epoch")).first()
    )
    # Epoch en segundos: no depende de la zona horaria del proceso.
    committed = datetime.fromtimestamp(last["epoch"], UTC).isoformat() if last["epoch"] is not None else None
    return TableHealth(
        contract.qualified_name,
        exists=True,
        rows=table.toDF().count(),
        files=detail["numFiles"],
        bytes=detail["sizeInBytes"],
        version=last["version"],
        last_operation=last["operation"],
        last_commit=committed,
    )


def bucket_usage(settings: Settings, bucket: str, store_factory=None) -> BucketUsage:
    """Objetos y bytes del bucket con el usuario que lo puede leer; el error se vuelve alerta."""
    from lakehouse.common.storage import S3ObjectStore

    factory = store_factory or S3ObjectStore
    try:
        objects, size = factory(settings.for_role(BUCKET_READERS[bucket])).usage(bucket)
        return BucketUsage(bucket, objects, size)
    except LakehouseError as exc:
        return BucketUsage(bucket, None, None, str(exc))


def alerts_for(
    tables: list[TableHealth], buckets: list[BucketUsage], contracts: tuple[TableContract, ...] = DELTA_TABLES
) -> list[Alert]:
    """Reglas de alerta; error = algo falta o no se puede medir, aviso = mantenimiento pendiente."""
    allow_empty = {c.qualified_name for c in contracts if c.allow_empty}
    alerts = []
    for b in buckets:
        if b.error:
            alerts.append(Alert(ERROR, f"bucket {b.bucket}", f"no se pudo medir: {b.error}"))
    for t in tables:
        if not t.exists:
            alerts.append(Alert(ERROR, t.table, "la tabla no existe; corre su paso"))
            continue
        if t.rows == 0 and t.table not in allow_empty:
            alerts.append(Alert(WARN, t.table, "tabla vacia"))
        if t.files and t.files >= SMALL_FILE_MIN_FILES and (t.bytes or 0) / t.files < SMALL_FILE_BYTES:
            alerts.append(Alert(WARN, t.table, f"{t.files} archivos chicos: conviene OPTIMIZE"))
        if t.version is not None and t.version >= MANY_VERSIONS:
            alerts.append(Alert(WARN, t.table, f"{t.version} versiones: conviene VACUUM"))
    return alerts


def collect(settings: Settings, spark: SparkSession, locator: TableLocator, now: datetime) -> HealthReport:
    report = HealthReport(generated_at=now.isoformat())
    report.buckets = [bucket_usage(settings, b) for b in LAYERS]
    report.tables = [table_health(spark, locator, c) for c in DELTA_TABLES]
    report.alerts = alerts_for(report.tables, report.buckets)
    return report


def log_report(report: HealthReport) -> None:
    """Resumen y alertas a los logs (los ve Dagster y docker compose logs)."""
    rows = sum(t.rows or 0 for t in report.tables)
    existing = sum(t.exists for t in report.tables)
    log.info("Salud: %d de %d tablas, %d filas en total", existing, len(report.tables), rows)
    for b in report.buckets:
        if not b.error:
            log.info("Bucket %s: %d objetos, %.1f MiB", b.bucket, b.objects, (b.bytes or 0) / 2**20)
    for a in report.alerts:
        (log.error if a.level == ERROR else log.warning)("ALERTA [%s] %s: %s", a.level, a.target, a.message)
    if not report.alerts:
        log.info("Salud OK: sin alertas.")


def run_health(settings: Settings) -> HealthReport:
    from lakehouse.common.delta import TableLocator
    from lakehouse.common.spark import build_spark

    spark = build_spark("health", settings.for_role("governance"))
    try:
        report = collect(settings, spark, TableLocator.from_settings(settings), datetime.now(UTC))
    finally:
        spark.stop()
    log_report(report)
    return report
