"""Configuracion de Superset para el lakehouse (F7): llave, idioma, cache y acceso de DuckDB a Oro en MinIO.

Se monta en /app/pythonpath (PYTHONPATH de la imagen). Credenciales solo del entorno (.env), nunca en el repo.
Responsable: Emilio Rendon Trias (Analista de Datos / BI)
"""

import logging
import os
from urllib.parse import urlparse

from sqlalchemy import event
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"Falta {name} en .env (README, paso 1)")
    return value


SECRET_KEY = _required("SUPERSET_SECRET_KEY")
# Metadatos en SQLite; start.sh lo borra en cada arranque (el tablero sale de bi/, D26).
SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(
    os.environ.get("SUPERSET_HOME", "/app/superset_home"), "superset.db"
)

BABEL_DEFAULT_LOCALE = "es"
LANGUAGES = {
    "es": {"flag": "mx", "name": "Espanol"},
    "en": {"flag": "us", "name": "English"},
}

# Sin Redis: cache en memoria del proceso. 2 min basta para que el tablero cargue rapido sin quedar viejo.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 120}
DATA_CACHE_CONFIG = CACHE_CONFIG
FEATURE_FLAGS = {"ALERT_REPORTS": False}
ROW_LIMIT = 50000

# --- DuckDB -> MinIO (D17): solo el usuario svc-bi, que solo puede leer el bucket gold ---
BI_USER = "svc-bi"
EXTENSION_DIR = os.environ.get("DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb/extensions")


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def minio_secret_sql() -> str:
    """Secreto temporal de DuckDB (vive en la conexion, nunca en disco) para leer s3://gold de MinIO."""
    endpoint = urlparse(os.environ.get("S3_ENDPOINT", "http://minio:9000"))
    use_ssl = "true" if endpoint.scheme == "https" else "false"
    return (
        "CREATE OR REPLACE SECRET minio_gold (TYPE s3, "
        f"KEY_ID {_quote(BI_USER)}, SECRET {_quote(_required('MINIO_SECRET_BI'))}, "
        f"ENDPOINT {_quote(endpoint.netloc)}, URL_STYLE 'path', USE_SSL {use_ssl}, "
        "REGION 'us-east-1', SCOPE 's3://gold')"
    )


@event.listens_for(Engine, "connect")
def _duckdb_reads_gold(dbapi_connection, _record) -> None:
    """Cada conexion nueva de DuckDB (tablero o SQL Lab) sale lista para delta_scan('s3://gold/...')."""
    if "duckdb" not in type(dbapi_connection).__module__:
        return  # la base de metadatos de Superset (SQLite) no se toca
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(f"SET extension_directory = {_quote(EXTENSION_DIR)}")
        cursor.execute(minio_secret_sql())
    except Exception:
        log.exception("No se pudo preparar DuckDB para leer Oro en MinIO")
        raise
    finally:
        cursor.close()
