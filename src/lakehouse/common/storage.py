"""Acceso a objetos de MinIO (S3) para archivos crudos de landing; las tablas Delta van por Spark.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import NoReturn, Protocol

from lakehouse.common.config import Settings
from lakehouse.common.errors import AccessDeniedError, StorageError

_DENIED_CODES = ("AccessDenied", "403")
_MISSING_CODES = ("404", "NoSuchKey", "NotFound")


class ObjectStore(Protocol):
    """Lo minimo que necesita la ingesta; las pruebas usan una version en memoria."""

    def put_bytes(
        self, bucket: str, key: str, data: bytes, metadata: Mapping[str, str] | None = None
    ) -> None: ...

    def get_bytes(self, bucket: str, key: str) -> bytes: ...

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]: ...

    def get_metadata(self, bucket: str, key: str) -> dict[str, str] | None:
        """Metadatos del objeto, o None si no existe."""
        ...


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None) or {}
    return str(response.get("Error", {}).get("Code", ""))


def _fail(action: str, bucket: str, key: str, exc: Exception) -> NoReturn:
    """AccessDeniedError si MinIO nego el permiso (D17); StorageError en cualquier otro caso."""
    where = f"s3://{bucket}/{key}"
    if _error_code(exc) in _DENIED_CODES:
        raise AccessDeniedError(f"Permiso negado al {action} {where}") from exc
    raise StorageError(f"No pude {action} {where}: {exc}") from exc


class S3ObjectStore:
    """ObjectStore sobre boto3 apuntando a MinIO con el usuario del rol (path style, reintentos estandar)."""

    def __init__(self, settings: Settings):
        import boto3
        from botocore.config import Config

        settings.require_credentials()
        self.role = settings.role
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name="us-east-1",  # MinIO lo ignora, pero boto3 exige uno
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    def put_bytes(self, bucket, key, data, metadata=None):
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=data, Metadata=dict(metadata or {}))
        except Exception as exc:
            _fail("escribir", bucket, key, exc)

    def get_bytes(self, bucket, key):
        try:
            return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as exc:
            _fail("leer", bucket, key, exc)

    def delete(self, bucket: str, key: str) -> None:
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:
            _fail("borrar", bucket, key, exc)

    def list_keys(self, bucket, prefix=""):
        try:
            pages = self._client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            return sorted(obj["Key"] for page in pages for obj in page.get("Contents", []))
        except Exception as exc:
            _fail("listar", bucket, prefix, exc)

    def usage(self, bucket: str, prefix: str = "") -> tuple[int, int]:
        """(objetos, bytes) bajo un prefijo; incluye versiones viejas de Delta (monitoreo, F5)."""
        try:
            pages = self._client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            sizes = [obj["Size"] for page in pages for obj in page.get("Contents", [])]
        except Exception as exc:
            _fail("medir", bucket, prefix, exc)
        return len(sizes), sum(sizes)

    def get_metadata(self, bucket, key):
        try:
            return dict(self._client.head_object(Bucket=bucket, Key=key).get("Metadata", {}))
        except Exception as exc:
            if _error_code(exc) in _MISSING_CODES:
                return None
            _fail("revisar", bucket, key, exc)
