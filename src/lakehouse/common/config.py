"""Configuracion del lakehouse leida de variables de entorno (el .env lo inyecta docker compose).

Cada proceso trabaja con el usuario de MinIO de su rol (D17): settings.for_role("ingestion"), etc.
Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from functools import lru_cache
from urllib.parse import urlparse

from lakehouse.common.errors import ConfigError

# Un bucket por capa (D3). infra/minio/init.sh crea los mismos; el smoke verifica que coincidan.
LAYERS: tuple[str, ...] = ("landing", "bronze", "silver", "gold")
MIN_SALT_LENGTH = 16
MIN_SECRET_LENGTH = 8  # MinIO no acepta contrasenas mas cortas
DEFAULT_MUSICBRAINZ_USER_AGENT = "lakehouse-escolar/0.1 (proyecto academico de datos)"
# Fuente de generos en la corrida orquestada (F5): la API real o el catalogo sintetico (CI y replica).
GENRES_SOURCES: tuple[str, ...] = ("api", "synthetic")

# Usuarios de servicio de MinIO (D17). Sus permisos: governance/access.py; los crea infra/minio/init.sh.
SERVICE_ROLES: tuple[str, ...] = ("ingestion", "transformation", "governance", "ml", "bi")


def service_user(role: str) -> str:
    """Usuario de MinIO del rol, p. ej. svc-ingestion."""
    if role not in SERVICE_ROLES:
        raise ConfigError(f"Rol desconocido '{role}'. Validos: {', '.join(SERVICE_ROLES)}")
    return f"svc-{role}"


def secret_env_var(role: str) -> str:
    """Variable con la contrasena del rol, igual en .env, compose e init.sh: p. ej. MINIO_SECRET_INGESTION."""
    return f"MINIO_SECRET_{role.upper()}"


@dataclass(frozen=True)
class Settings:
    """Valores de entorno ya validados; las llaves no salen en repr ni en logs."""

    s3_endpoint: str
    # Credenciales activas: vacias hasta elegir rol con for_role().
    s3_access_key: str = field(default="", repr=False)
    s3_secret_key: str = field(default="", repr=False)
    role: str = ""
    service_secrets: Mapping[str, str] = field(default_factory=dict, repr=False, hash=False, compare=False)
    spark_master: str = "local[*]"
    spark_driver_memory: str = "4g"
    # Sal para seudonimizar ip_addr (D8). Opcional aqui; la ingesta exige que exista y sea larga.
    ip_hash_salt: str = field(default="", repr=False)
    spotify_source_dir: str = "/datos_spotify"
    # Generico a proposito: sin nombres, correos ni URLs personales (regla de privacidad).
    musicbrainz_user_agent: str = DEFAULT_MUSICBRAINZ_USER_AGENT
    genres_source: str = "api"
    # Servidor de experimentos (F6); vacio fuera del compose.
    mlflow_tracking_uri: str = ""

    def for_role(self, role: str) -> "Settings":
        """Copia con las credenciales del usuario de servicio del rol; falla si falta su contrasena."""
        user = service_user(role)
        secret = self.service_secrets.get(role, "")
        if len(secret) < MIN_SECRET_LENGTH:
            raise ConfigError(
                f"Falta {secret_env_var(role)} (contrasena de {user}, minimo {MIN_SECRET_LENGTH} caracteres) "
                "en tu .env (README, paso 1)."
            )
        return replace(self, s3_access_key=user, s3_secret_key=secret, role=role)

    def require_credentials(self) -> None:
        if not (self.s3_access_key and self.s3_secret_key):
            raise ConfigError(
                "Settings sin rol de MinIO: usa settings.for_role('<rol>') antes de conectarte."
            )

    def require_ip_salt(self) -> str:
        """Devuelve la sal de IP o falla si no hay una suficientemente larga."""
        if len(self.ip_hash_salt) < MIN_SALT_LENGTH:
            raise ConfigError(
                f"IP_HASH_SALT debe tener al menos {MIN_SALT_LENGTH} caracteres. Agregala a tu .env "
                "(plantilla: .env.example)."
            )
        return self.ip_hash_salt

    def require_mlflow(self) -> str:
        """URI de MLflow o error claro si falta (el compose la pone en el contenedor)."""
        if not self.mlflow_tracking_uri:
            raise ConfigError(
                "Falta MLFLOW_TRACKING_URI (p. ej. http://mlflow:5000). Corre el ML dentro del contenedor."
            )
        return self.mlflow_tracking_uri

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
    secrets = {r: env.get(secret_env_var(r), "").strip() for r in SERVICE_ROLES}
    genres_source = env.get("GENRES_SOURCE", "").strip().lower() or "api"
    if genres_source not in GENRES_SOURCES:
        raise ConfigError(f"GENRES_SOURCE debe ser {' o '.join(GENRES_SOURCES)}, no '{genres_source}'")
    return Settings(
        s3_endpoint=endpoint.rstrip("/"),
        service_secrets={r: s for r, s in secrets.items() if s},
        spark_master=env.get("SPARK_MASTER", "").strip() or "local[*]",
        spark_driver_memory=env.get("SPARK_DRIVER_MEMORY", "").strip() or "4g",
        ip_hash_salt=env.get("IP_HASH_SALT", "").strip(),
        spotify_source_dir=env.get("SPOTIFY_SOURCE_DIR", "").strip() or "/datos_spotify",
        musicbrainz_user_agent=env.get("MUSICBRAINZ_USER_AGENT", "").strip()
        or DEFAULT_MUSICBRAINZ_USER_AGENT,
        genres_source=genres_source,
        mlflow_tracking_uri=env.get("MLFLOW_TRACKING_URI", "").strip().rstrip("/"),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Settings del proceso actual (se leen una vez)."""
    return load_settings()
