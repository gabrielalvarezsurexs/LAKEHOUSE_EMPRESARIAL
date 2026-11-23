"""Reglas fijas de Plata (sin Spark): familia de plataforma, etiqueta de skip, offline_timestamp y zonas.

Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

import pytest

from lakehouse.transformation.catalogs import (
    COUNTRY_TIMEZONES,
    DEVICE_TYPES,
    PLATFORM_FAMILIES,
    offline_epoch_seconds,
    platform_family,
    skip_label,
)


@pytest.mark.parametrize(
    ("platform", "family"),
    [
        ("web_player windows 10;chrome 120;desktop", "web"),
        ("WebPlayer (websocket)", "web"),
        ("Android OS 13 API 33 (Samsung, SM-A515F)", "android"),
        ("iOS 17.4 (iPhone15,2)", "ios"),
        ("Windows 10 (10.0.19045; x64)", "windows"),
        ("OS X 10.15.7 [x86 8]", "macos"),
        ("linux", "linux"),
        ("Partner android_tv Sony", "smart_device"),  # el orden manda: partner antes que android
        ("Partner sonos_speaker", "smart_device"),
        ("google_cast", "smart_device"),
        ("algo nuevo", "other"),
        ("", None),
        (None, None),
    ],
)
def test_platform_family(platform, family):
    assert platform_family(platform) == family


def test_every_family_has_device_type():
    assert {f for f, _ in PLATFORM_FAMILIES} | {"other"} == set(DEVICE_TYPES)


@pytest.mark.parametrize(
    ("reason", "ms", "label"),
    [
        ("fwdbtn", 5_000, True),
        ("fwdbtn", 120_000, True),
        ("backbtn", 1_000, True),
        ("trackdone", 10_000, False),
        ("endplay", 29_999, True),
        ("endplay", 30_000, False),
        ("playbtn", 2_000, True),
        ("logout", 1_000, None),
        ("unexpected-exit-while-paused", 1_000, None),
        ("remote", 1_000, None),
        ("cosmos", 1_000, None),
        (None, 1_000, None),
    ],
)
def test_skip_label(reason, ms, label):
    assert skip_label(reason, ms) is label


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        (1_719_849_600, 1_719_849_600.0),
        (1_719_849_600_123, 1_719_849_600.123),
        (None, None),
        (0, None),
        (12345, None),
    ],
)
def test_offline_epoch_seconds(value, seconds):
    assert offline_epoch_seconds(value) == seconds


def test_timezones_are_valid_iana_names():
    zoneinfo = pytest.importorskip("zoneinfo")
    try:
        zoneinfo.ZoneInfo("America/Mexico_City")
    except zoneinfo.ZoneInfoNotFoundError:
        pytest.skip("Este Python no trae base de zonas (tzdata); la prueba de Spark las valida")
    for country, zone in COUNTRY_TIMEZONES.items():
        assert len(country) == 2 and country.isupper()
        zoneinfo.ZoneInfo(zone)
