"""SparkSession con Delta Lake y S3A apuntando a MinIO (D4, D5).

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.common.config import Settings, get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Llaves fijas del .env (no roles de AWS ni variables AWS_*).
_CREDENTIALS_PROVIDER = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"


def spark_conf(settings: Settings) -> dict[str, str]:
    """Configuracion de Spark; funcion pura para poder probarla sin Java."""
    return {
        "spark.master": settings.spark_master,
        "spark.driver.memory": settings.spark_driver_memory,
        # Delta Lake como formato y catalogo por defecto.
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # S3A contra MinIO: sin path style, MinIO responde 403/404.
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(settings.s3_ssl).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": _CREDENTIALS_PROVIDER,
        "spark.hadoop.fs.s3a.access.key": settings.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.s3_secret_key,
        "spark.hadoop.fs.s3a.attempts.maximum": "3",
        # Todo el pipeline en UTC; la hora local se calcula explicita en Plata.
        "spark.sql.session.timeZone": "UTC",
        # Con decenas de miles de filas, 200 particiones de shuffle solo agregan overhead.
        "spark.sql.shuffle.partitions": "8",
        "spark.ui.showConsoleProgress": "false",
        # Bronce tiene ~28 columnas; con el tope de 25 Spark avisa que trunca el plan en cada corrida.
        "spark.sql.debug.maxToStringFields": "200",
    }


def build_spark(app_name: str, settings: Settings | None = None) -> SparkSession:
    """Crea (o reutiliza) la SparkSession del proceso. Los JAR de Delta y S3A vienen en la imagen."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_conf(settings or get_settings()).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
