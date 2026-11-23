"""Pruebas de lakehouse.common.config.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import pytest

from lakehouse.common.config import LAYERS, SERVICE_ROLES, load_settings, secret_env_var, service_user
from lakehouse.common.errors import ConfigError

BASE_ENV = {"S3_ENDPOINT": "http://minio:9000/", "MINIO_SECRET_INGESTION": "secreto-ingesta"}


def test_load_settings_defaults():
    settings = load_settings(BASE_ENV)
    assert settings.s3_endpoint == "http://minio:9000"
    assert settings.spark_master == "local[*]"
    assert settings.spark_driver_memory == "4g"
    assert settings.s3_ssl is False
    assert settings.role == "" and settings.s3_access_key == ""


def test_for_role_uses_the_service_user():
    settings = load_settings(BASE_ENV).for_role("ingestion")
    assert (settings.role, settings.s3_access_key, settings.s3_secret_key) == (
        "ingestion",
        "svc-ingestion",
        "secreto-ingesta",
    )


@pytest.mark.parametrize("secret", [None, "corta"])
def test_for_role_requires_a_long_secret(secret):
    env = {"S3_ENDPOINT": "http://minio:9000"}
    if secret:
        env["MINIO_SECRET_GOVERNANCE"] = secret
    with pytest.raises(ConfigError, match="MINIO_SECRET_GOVERNANCE"):
        load_settings(env).for_role("governance")


def test_without_role_there_are_no_credentials():
    with pytest.raises(ConfigError, match="for_role"):
        load_settings(BASE_ENV).require_credentials()


def test_unknown_role_raises():
    with pytest.raises(ConfigError, match="Rol desconocido"):
        load_settings(BASE_ENV).for_role("root")


def test_roles_have_users_and_variables():
    assert [service_user(r) for r in SERVICE_ROLES] == [f"svc-{r}" for r in SERVICE_ROLES]
    assert secret_env_var("ml") == "MINIO_SECRET_ML"


def test_ip_salt_is_required_long_and_hidden():
    with pytest.raises(ConfigError, match="IP_HASH_SALT"):
        load_settings(BASE_ENV).require_ip_salt()
    with pytest.raises(ConfigError, match="IP_HASH_SALT"):
        load_settings({**BASE_ENV, "IP_HASH_SALT": "corta"}).require_ip_salt()
    settings = load_settings({**BASE_ENV, "IP_HASH_SALT": "una-sal-larga-de-prueba"})
    assert settings.require_ip_salt() == "una-sal-larga-de-prueba"
    assert "una-sal" not in repr(settings)


def test_musicbrainz_user_agent_is_generic_by_default():
    agent = load_settings(BASE_ENV).musicbrainz_user_agent
    assert "@" not in agent and "github" not in agent.lower()


def test_https_enables_ssl():
    assert load_settings({**BASE_ENV, "S3_ENDPOINT": "https://s3.local:9000"}).s3_ssl is True


def test_missing_endpoint_raises():
    with pytest.raises(ConfigError, match="S3_ENDPOINT"):
        load_settings({"S3_ENDPOINT": "   "})


@pytest.mark.parametrize("endpoint", ["minio:9000", "ftp://minio:9000", "http://"])
def test_invalid_endpoint_raises(endpoint):
    with pytest.raises(ConfigError, match="S3_ENDPOINT"):
        load_settings({**BASE_ENV, "S3_ENDPOINT": endpoint})


def test_secrets_not_in_repr():
    settings = load_settings({**BASE_ENV, "MINIO_SECRET_INGESTION": "llave-secreta-y"})
    assert "llave-secreta-y" not in repr(settings)
    assert "llave-secreta-y" not in repr(settings.for_role("ingestion"))


def test_layer_and_table_uris():
    settings = load_settings(BASE_ENV)
    assert [settings.layer_uri(layer) for layer in LAYERS] == [
        "s3a://landing",
        "s3a://bronze",
        "s3a://silver",
        "s3a://gold",
    ]
    assert settings.table_uri("silver", "plays") == "s3a://silver/plays"


@pytest.mark.parametrize(
    ("layer", "table"), [("platinum", "x"), ("gold", ""), ("gold", "../x"), ("gold", "/x")]
)
def test_invalid_layer_or_table_raises(layer, table):
    with pytest.raises(ConfigError):
        load_settings(BASE_ENV).table_uri(layer, table)
