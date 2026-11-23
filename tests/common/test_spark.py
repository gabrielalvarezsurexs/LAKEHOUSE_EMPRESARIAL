"""Pruebas de lakehouse.common.spark: la configuracion sin Java y una tabla Delta local con Spark.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import pytest

from lakehouse.common.config import load_settings
from lakehouse.common.spark import spark_conf

BASE_ENV = {"S3_ENDPOINT": "http://minio:9000", "S3_ACCESS_KEY": "user", "S3_SECRET_KEY": "secret"}


def test_spark_conf_points_to_minio_with_delta():
    conf = spark_conf(load_settings(BASE_ENV))
    assert conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert conf["spark.sql.catalog.spark_catalog"] == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.sql.session.timeZone"] == "UTC"


def test_spark_conf_values_are_strings():
    assert all(isinstance(v, str) for v in spark_conf(load_settings(BASE_ENV)).values())


@pytest.mark.spark
def test_delta_roundtrip_local(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.range(10).write.format("delta").save(path)
    spark.range(10, 15).write.format("delta").mode("append").save(path)
    assert spark.read.format("delta").load(path).count() == 15
    assert spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 10
