"""Fixtures compartidas. Las pruebas con Spark solo corren con RUN_SPARK_TESTS=1 (contenedor o CI).

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import os

import pytest

from lakehouse.common.config import load_settings

RUN_SPARK = os.environ.get("RUN_SPARK_TESTS") == "1"


def pytest_collection_modifyitems(config, items):
    if RUN_SPARK:
        return
    skip = pytest.mark.skip(reason="Necesita Spark: define RUN_SPARK_TESTS=1 (contenedor o CI)")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    """SparkSession local para pruebas con DataFrames chicos y tablas Delta en disco temporal."""
    from lakehouse.common.spark import build_spark

    # Llaves falsas: las pruebas escriben en disco local, nunca en MinIO.
    settings = load_settings(
        {"S3_ENDPOINT": "http://localhost:9000", "S3_ACCESS_KEY": "test", "S3_SECRET_KEY": "test"}
    )
    session = build_spark("tests", settings)
    yield session
    session.stop()
