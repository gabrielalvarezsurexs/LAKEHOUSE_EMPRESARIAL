"""Acceso a objetos de MinIO (S3) para archivos crudos de landing; las tablas Delta van por Spark.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from lakehouse.common.config import Settings, get_settings
from lakehouse.common.errors import StorageError


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


class S3ObjectStore:
    """ObjectStore sobre boto3 apuntando a MinIO (path style, reintentos estandar)."""

    def __init__(self, settings: Settings | None = None):
        import boto3
        from botocore.config import Config

        settings = settings or get_settings()
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
            raise StorageError(f"No pude escribir s3://{bucket}/{key}: {exc}") from exc

    def get_bytes(self, bucket, key):
        try:
            return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as exc:
            raise StorageError(f"No pude leer s3://{bucket}/{key}: {exc}") from exc

    def list_keys(self, bucket, prefix=""):
        try:
            pages = self._client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            return sorted(obj["Key"] for page in pages for obj in page.get("Contents", []))
        except Exception as exc:
            raise StorageError(f"No pude listar s3://{bucket}/{prefix}: {exc}") from exc

    def get_metadata(self, bucket, key):
        from botocore.exceptions import ClientError

        try:
            return dict(self._client.head_object(Bucket=bucket, Key=key).get("Metadata", {}))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise StorageError(f"No pude revisar s3://{bucket}/{key}: {exc}") from exc
