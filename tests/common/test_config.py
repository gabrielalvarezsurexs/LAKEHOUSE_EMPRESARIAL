"""Pruebas de lakehouse.common.config.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import pytest

from lakehouse.common.config import LAYERS, load_settings
from lakehouse.common.errors import ConfigError

BASE_ENV = {"S3_ENDPOINT": "http://minio:9000/", "S3_ACCESS_KEY": "user", "S3_SECRET_KEY": "secret"}


def test_load_settings_defaults():
    settings = load_settings(BASE_ENV)
    assert settings.s3_endpoint == "http://minio:9000"
    assert settings.spark_master == "local[*]"
    assert settings.spark_driver_memory == "4g"
    assert settings.s3_ssl is False


def test_https_enables_ssl():
    assert load_settings({**BASE_ENV, "S3_ENDPOINT": "https://s3.local:9000"}).s3_ssl is True


@pytest.mark.parametrize("missing", ["S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY"])
def test_missing_variable_raises(missing):
    env = {k: v for k, v in BASE_ENV.items() if k != missing}
    with pytest.raises(ConfigError, match=missing):
        load_settings(env)


def test_blank_variable_counts_as_missing():
    with pytest.raises(ConfigError, match="S3_SECRET_KEY"):
        load_settings({**BASE_ENV, "S3_SECRET_KEY": "   "})


@pytest.mark.parametrize("endpoint", ["minio:9000", "ftp://minio:9000", "http://"])
def test_invalid_endpoint_raises(endpoint):
    with pytest.raises(ConfigError, match="S3_ENDPOINT"):
        load_settings({**BASE_ENV, "S3_ENDPOINT": endpoint})


def test_secrets_not_in_repr():
    text = repr(load_settings(BASE_ENV))
    assert "user" not in text
    assert "secret" not in text


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
