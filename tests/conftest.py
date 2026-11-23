"""Fixtures compartidas. Las pruebas con Spark solo corren con RUN_SPARK_TESTS=1 (contenedor o CI).

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import os

import pytest

from lakehouse.common.config import SERVICE_ROLES, load_settings

RUN_SPARK = os.environ.get("RUN_SPARK_TESTS") == "1"
# Contrasenas falsas de los usuarios de servicio: las pruebas nunca se conectan a MinIO.
TEST_SECRETS = {f"MINIO_SECRET_{r.upper()}": f"secreto-de-prueba-{r}" for r in SERVICE_ROLES}


def pytest_collection_modifyitems(config, items):
    if RUN_SPARK:
        return
    skip = pytest.mark.skip(reason="Necesita Spark: define RUN_SPARK_TESTS=1 (contenedor o CI)")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)


class MemoryObjectStore:
    """ObjectStore en memoria (mismo contrato que S3ObjectStore) para probar sin MinIO."""

    def __init__(self):
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}

    def put_bytes(self, bucket, key, data, metadata=None):
        self.objects[(bucket, key)] = (bytes(data), dict(metadata or {}))

    def get_bytes(self, bucket, key):
        return self.objects[(bucket, key)][0]

    def list_keys(self, bucket, prefix=""):
        return sorted(k for b, k in self.objects if b == bucket and k.startswith(prefix))

    def get_metadata(self, bucket, key):
        obj = self.objects.get((bucket, key))
        return dict(obj[1]) if obj else None

    def delete(self, bucket, key):
        self.objects.pop((bucket, key), None)


@pytest.fixture
def store():
    return MemoryObjectStore()


@pytest.fixture
def settings():
    """Settings de prueba con sal valida (nunca la del .env)."""
    return load_settings(
        {"S3_ENDPOINT": "http://localhost:9000", "IP_HASH_SALT": "sal-de-prueba-0123456789", **TEST_SECRETS}
    )


@pytest.fixture(scope="session")
def spark():
    """SparkSession local para pruebas con DataFrames chicos y tablas Delta en disco temporal."""
    from lakehouse.common.spark import build_spark

    # Llaves falsas: las pruebas escriben en disco local, nunca en MinIO.
    settings = load_settings({"S3_ENDPOINT": "http://localhost:9000", **TEST_SECRETS})
    session = build_spark("tests", settings.for_role("transformation"))
    yield session
    session.stop()
