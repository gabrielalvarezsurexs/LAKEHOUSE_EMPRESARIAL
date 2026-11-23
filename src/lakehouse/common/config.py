"""Configuracion del lakehouse leida de variables de entorno (el .env lo inyecta docker compose).

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import lru_cache
from urllib.parse import urlparse

from lakehouse.common.errors import ConfigError

# Un bucket por capa (D3). infra/minio/init.sh crea los mismos; el smoke verifica que coincidan.
LAYERS: tuple[str, ...] = ("landing", "bronze", "silver", "gold")


@dataclass(frozen=True)
class Settings:
    """Valores de entorno ya validados; las llaves no salen en repr ni en logs."""

    s3_endpoint: str
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)
    spark_master: str = "local[*]"
    spark_driver_memory: str = "4g"

    @property
    def s3_ssl(self) -> bool:
        return urlparse(self.s3_endpoint).scheme == "https"

    def layer_uri(self, layer: str) -> str:
        """Raiz de una capa, p. ej. s3a://bronze."""
        if layer not in LAYERS:
            raise ConfigError(f"Capa desconocida '{layer}'. Validas: {', '.join(LAYERS)}")
        return f"s3a://{layer}"

    def table_uri(self, layer: str, table: str) -> str:
        """Ruta de una tabla Delta, p. ej. s3a://silver/plays."""
        if not table or table.startswith("/") or ".." in table:
            raise ConfigError(f"Nombre de tabla invalido: '{table}'")
        return f"{self.layer_uri(layer)}/{table}"


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"Falta la variable de entorno {name}. Revisa tu .env (plantilla: .env.example).")
    return value


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Construye Settings desde un mapeo (por defecto os.environ); falla con mensaje claro."""
    env = os.environ if env is None else env
    endpoint = _require(env, "S3_ENDPOINT")
    parsed = urlparse(endpoint)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ConfigError(f"S3_ENDPOINT debe ser una URL http(s)://host:puerto, no '{endpoint}'")
    return Settings(
        s3_endpoint=endpoint.rstrip("/"),
        s3_access_key=_require(env, "S3_ACCESS_KEY"),
        s3_secret_key=_require(env, "S3_SECRET_KEY"),
        spark_master=env.get("SPARK_MASTER", "").strip() or "local[*]",
        spark_driver_memory=env.get("SPARK_DRIVER_MEMORY", "").strip() or "4g",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Settings del proceso actual (se leen una vez)."""
    return load_settings()
