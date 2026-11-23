"""Prueba de humo de la infraestructura: buckets en MinIO y escritura/lectura Delta con Spark.

Cada bucket se revisa con un usuario de servicio que puede leerlo (D17); el root no entra al contenedor.
Uso (dentro del contenedor): python -m lakehouse.common.smoke
Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from lakehouse.common.config import LAYERS, Settings, get_settings
from lakehouse.common.errors import LakehouseError, StorageError
from lakehouse.common.log import get_logger
from lakehouse.common.spark import build_spark
from lakehouse.common.storage import S3ObjectStore

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger("lakehouse.smoke")

# Prefijo temporal: se borra al inicio y al final para no dejar basura en Bronce.
SMOKE_TABLE = "_smoke/delta_check"
# Rol con permiso de lectura en cada bucket (landing solo lo lee la ingesta: trae la IP en claro).
BUCKET_READERS = {
    "landing": "ingestion",
    "bronze": "governance",
    "silver": "governance",
    "gold": "governance",
}


def check_buckets(settings: Settings) -> None:
    """Lista cada bucket con su lector; si el bucket no existe o el usuario no sirve, falla."""
    for layer in LAYERS:
        role = BUCKET_READERS[layer]
        try:
            S3ObjectStore(settings.for_role(role)).list_keys(layer, "_smoke_probe/")
        except StorageError as exc:
            raise StorageError(f"No pude leer '{layer}' como svc-{role}. Corrio minio-init? {exc}") from exc
        log.info("Bucket %s: OK (svc-%s)", layer, role)


def _hadoop_path(spark: SparkSession, uri: str):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path, path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())


def check_delta(spark: SparkSession, settings: Settings) -> None:
    """Escribe 100 filas, agrega 50, valida conteos, historial y time travel; luego borra la tabla."""
    from delta.tables import DeltaTable

    uri = settings.table_uri("bronze", SMOKE_TABLE)
    path, fs = _hadoop_path(spark, uri)
    fs.delete(path, True)
    try:
        spark.range(100).withColumnRenamed("id", "n").write.format("delta").mode("overwrite").save(uri)
        spark.range(100, 150).withColumnRenamed("id", "n").write.format("delta").mode("append").save(uri)

        total = spark.read.format("delta").load(uri).count()
        version0 = spark.read.format("delta").option("versionAsOf", 0).load(uri).count()
        versions = DeltaTable.forPath(spark, uri).history().count()
        log.info("Delta: filas=%d, filas en version 0=%d, versiones=%d", total, version0, versions)
        if (total, version0, versions) != (150, 100, 2):
            raise StorageError("Los conteos de Delta no son los esperados (150, 100, 2).")
    finally:
        fs.delete(path, True)


def main() -> int:
    settings = get_settings()
    log.info("Smoke contra %s (master %s)", settings.s3_endpoint, settings.spark_master)
    try:
        check_buckets(settings)
        spark = build_spark("smoke", settings.for_role("ingestion"))
    except LakehouseError as exc:
        log.error("Smoke FALLO: %s", exc)
        return 1
    try:
        log.info("Spark %s", spark.version)
        check_delta(spark, settings.for_role("ingestion"))
    except LakehouseError as exc:
        log.error("Smoke FALLO: %s", exc)
        return 1
    finally:
        spark.stop()
    log.info("Smoke OK: MinIO, usuarios de servicio, Spark y Delta funcionan.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
