"""Bronce: archivos de landing a Delta con esquema explicito, metadatos de ingesta, cuarentena y bitacora.

Idempotente (D15): cada archivo se identifica por su SHA-256; sus filas se escriben con replaceWhere sobre ese
hash, asi que re-correr (aunque una corrida anterior se haya caido a la mitad) nunca duplica.
Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from lakehouse.common.config import Settings
from lakehouse.common.delta import ensure_table
from lakehouse.common.log import get_logger
from lakehouse.common.storage import ObjectStore
from lakehouse.governance.contracts import (
    BRONZE_INGESTION_LOG,
    BRONZE_QUARANTINE,
    BRONZE_SPOTIFY_STREAMS,
    TableContract,
)
from lakehouse.ingestion.landing import LANDING_BUCKET, SPOTIFY_PREFIX
from lakehouse.ingestion.records import file_sha256, parse_spotify_file

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = get_logger("lakehouse.ingestion.bronze")

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class BronzeTables:
    """Rutas de las tablas de Bronce; en pruebas apuntan a un directorio local."""

    table_uri: Callable[[str], str]

    @classmethod
    def from_settings(cls, settings: Settings) -> BronzeTables:
        return cls(lambda name: settings.table_uri("bronze", name))

    @classmethod
    def at(cls, base: str) -> BronzeTables:
        return cls(lambda name: f"{base.rstrip('/')}/{name}")

    def uri(self, contract: TableContract) -> str:
        return self.table_uri(contract.name)


@dataclass
class FileResult:
    source_file: str
    rows_read: int
    rows_loaded: int
    rows_quarantined: int


@dataclass
class BronzeResult:
    batch_id: str
    files: list[FileResult]
    skipped: list[str]  # ya ingeridos (mismo SHA-256)

    @property
    def rows_loaded(self) -> int:
        return sum(f.rows_loaded for f in self.files)

    @property
    def rows_quarantined(self) -> int:
        return sum(f.rows_quarantined for f in self.files)


def new_batch_id(now: datetime) -> str:
    return f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _to_df(spark: SparkSession, contract: TableContract, rows: Sequence[dict[str, Any]]) -> DataFrame:
    names = contract.column_names
    return spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], contract.spark_schema())


def _is_delta(spark: SparkSession, uri: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, uri)


def replace_file_slice(spark: SparkSession, df: DataFrame, uri: str, sha_column: str, sha: str) -> None:
    """Reemplaza atomicamente las filas de un archivo (por su hash); si la tabla no existe, la crea."""
    if not _SHA_RE.match(sha):
        raise ValueError("SHA-256 invalido")  # nunca se arma un predicado con texto arbitrario
    writer = df.write.format("delta")
    if _is_delta(spark, uri):
        writer.mode("overwrite").option("replaceWhere", f"{sha_column} = '{sha}'").save(uri)
    else:
        writer.mode("append").save(uri)


def ingested_hashes(spark: SparkSession, tables: BronzeTables) -> set[str]:
    uri = tables.uri(BRONZE_INGESTION_LOG)
    if not _is_delta(spark, uri):
        return set()
    rows = spark.read.format("delta").load(uri).select("file_sha256").distinct().collect()
    return {r[0] for r in rows}


def _ingest_file(
    spark: SparkSession, tables: BronzeTables, key: str, content: bytes, salt: str, batch: dict[str, Any]
) -> FileResult:
    sha = file_sha256(content)
    parsed = parse_spotify_file(content, salt)
    meta = {"_source_file": key, "_file_sha256": sha, **batch}

    streams = [{**row, **meta} for row in parsed.rows]
    quarantine = [
        {"_record_index": q.record_index, "cause": q.cause, "raw_record": q.raw_record, **meta}
        for q in parsed.quarantine
    ]
    # Orden: datos, cuarentena y al final la bitacora. Si algo se cae antes de la bitacora, la siguiente
    # corrida vuelve a procesar el archivo y replaceWhere deja todo igual.
    replace_file_slice(
        spark,
        _to_df(spark, BRONZE_SPOTIFY_STREAMS, streams),
        tables.uri(BRONZE_SPOTIFY_STREAMS),
        "_file_sha256",
        sha,
    )
    replace_file_slice(
        spark,
        _to_df(spark, BRONZE_QUARANTINE, quarantine),
        tables.uri(BRONZE_QUARANTINE),
        "_file_sha256",
        sha,
    )
    if parsed.unknown_fields:
        log.warning("Campos fuera del contrato en %s: %s", key, ", ".join(sorted(parsed.unknown_fields)))
    log_row = {
        "file_sha256": sha,
        "source_file": key,
        "batch_id": batch["_batch_id"],
        "rows_read": parsed.rows_read,
        "rows_loaded": len(streams),
        "rows_quarantined": len(quarantine),
        "unknown_fields": ",".join(sorted(parsed.unknown_fields)) or None,
        "ingested_at": batch["_ingested_at"],
    }
    _to_df(spark, BRONZE_INGESTION_LOG, [log_row]).write.format("delta").mode("append").save(
        tables.uri(BRONZE_INGESTION_LOG)
    )
    return FileResult(key, parsed.rows_read, len(streams), len(quarantine))


def ingest_bronze(
    spark: SparkSession,
    store: ObjectStore,
    settings: Settings,
    tables: BronzeTables | None = None,
    force: bool = False,
) -> BronzeResult:
    """Procesa los archivos de landing/spotify que aun no estan en la bitacora (todos con force=True)."""
    salt = settings.require_ip_salt()
    tables = tables or BronzeTables.from_settings(settings)
    for contract in (BRONZE_SPOTIFY_STREAMS, BRONZE_QUARANTINE, BRONZE_INGESTION_LOG):
        # Crea desde el contrato (NOT NULL, comentarios) o alinea los metadatos de una tabla vieja (F4).
        ensure_table(spark, tables.uri(contract), contract)
    now = datetime.now(UTC)
    batch = {"_batch_id": new_batch_id(now), "_ingested_at": now}
    done = set() if force else ingested_hashes(spark, tables)

    result = BronzeResult(batch["_batch_id"], [], [])
    keys = [k for k in store.list_keys(LANDING_BUCKET, SPOTIFY_PREFIX) if k.lower().endswith(".json")]
    for key in keys:
        content = store.get_bytes(LANDING_BUCKET, key)
        sha = file_sha256(content)
        if sha in done:
            result.skipped.append(key)
            continue
        done.add(sha)  # el mismo contenido con dos llaves se procesa una vez
        file_result = _ingest_file(spark, tables, key, content, salt, batch)
        result.files.append(file_result)
        log.info(
            "Bronce %s: leidas=%d, cargadas=%d, cuarentena=%d",
            key,
            file_result.rows_read,
            file_result.rows_loaded,
            file_result.rows_quarantined,
        )
    log.info(
        "Bronce lote %s: archivos nuevos=%d, ya ingeridos=%d, filas cargadas=%d, cuarentena=%d",
        result.batch_id,
        len(result.files),
        len(result.skipped),
        result.rows_loaded,
        result.rows_quarantined,
    )
    return result


def table_counts(spark: SparkSession, tables: BronzeTables) -> dict[str, int]:
    """Conteo total de cada tabla de Bronce (para el resumen y la reconciliacion)."""
    counts = {}
    for contract in (BRONZE_SPOTIFY_STREAMS, BRONZE_QUARANTINE, BRONZE_INGESTION_LOG):
        uri = tables.uri(contract)
        counts[contract.name] = spark.read.format("delta").load(uri).count() if _is_delta(spark, uri) else 0
    return counts
