"""Pruebas del parseo y la validacion contra el contrato (sin Spark).

Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

import json
from datetime import UTC, datetime

import pytest

from lakehouse.governance.contracts import BRONZE_SPOTIFY_STREAMS, SPOTIFY_STREAMS_SOURCE
from lakehouse.ingestion.records import file_sha256, hash_ip, parse_spotify_file, validate_record

SALT = "sal-de-prueba-0123456789"
IP = "192.0.2.10"


def make_record(**overrides):
    record = {name: None for name in SPOTIFY_STREAMS_SOURCE.column_names}
    record.update(
        ts="2024-05-01T12:30:00Z",
        platform="android",
        ms_played=180000,
        conn_country="MX",
        ip_addr=IP,
        master_metadata_track_name="Cancion",
        master_metadata_album_artist_name="Artista",
        spotify_track_uri="spotify:track:abc",
        reason_start="clickrow",
        reason_end="trackdone",
        shuffle=False,
        skipped=False,
        offline=False,
        incognito_mode=False,
    )
    record.update(overrides)
    return record


def encode(records):
    return json.dumps(records).encode("utf-8")


def test_valid_record_becomes_bronze_row():
    row, cause = validate_record(make_record(), SALT)
    assert cause is None
    assert row["ts"] == datetime(2024, 5, 1, 12, 30, tzinfo=UTC)
    assert "ip_addr" not in row
    assert row["ip_addr_hash"] == hash_ip(IP, SALT)
    # La fila trae justo las columnas de origen de Bronce (los metadatos se agregan al escribir).
    source_cols = {c for c in BRONZE_SPOTIFY_STREAMS.column_names if not c.startswith("_")}
    assert set(row) == source_cols


def test_ip_hash_is_stable_and_depends_on_salt():
    assert hash_ip(IP, SALT) == hash_ip(IP, SALT)
    assert hash_ip(IP, SALT) != hash_ip(IP, SALT + "x")
    assert IP not in hash_ip(IP, SALT)


@pytest.mark.parametrize(
    ("overrides", "cause"),
    [
        ({"ms_played": None}, "missing_required:ms_played"),
        ({"ts": None}, "missing_required:ts"),
        ({"ms_played": "180000"}, "bad_type:ms_played"),
        ({"ms_played": True}, "bad_type:ms_played"),
        ({"shuffle": 1}, "bad_type:shuffle"),
        ({"platform": 7}, "bad_type:platform"),
        ({"ts": "ayer"}, "bad_timestamp:ts"),
        ({"ts": "2024-05-01T12:30:00"}, "bad_timestamp:ts"),  # sin zona horaria
        ({"ms_played": -5}, "negative:ms_played"),
    ],
)
def test_invalid_records_get_a_cause(overrides, cause):
    row, got = validate_record(make_record(**overrides), SALT)
    assert row is None
    assert got == cause


def test_optional_fields_may_be_missing():
    record = make_record()
    del record["platform"], record["offline_timestamp"]
    row, cause = validate_record(record, SALT)
    assert cause is None
    assert row["platform"] is None


def test_parse_file_splits_rows_and_quarantine_without_leaking_ip():
    records = [make_record(), make_record(ms_played="x"), "no soy objeto", make_record(extra_field=1)]
    parsed = parse_spotify_file(encode(records), SALT)
    assert parsed.rows_read == 4
    assert [r["_record_index"] for r in parsed.rows] == [0, 3]
    assert [(q.record_index, q.cause) for q in parsed.quarantine] == [
        (1, "bad_type:ms_played"),
        (2, "not_an_object"),
    ]
    assert parsed.unknown_fields == {"extra_field"}
    assert all(IP not in (q.raw_record or "") for q in parsed.quarantine)


@pytest.mark.parametrize(
    ("content", "cause"),
    [
        (b"{no es json", "invalid_json"),
        (b'{"a": 1}', "not_a_json_array"),
        (b"\xff\xfe\x00", "invalid_encoding"),
    ],
)
def test_unreadable_file_is_one_quarantine_row(content, cause):
    parsed = parse_spotify_file(content, SALT)
    assert parsed.rows == []
    assert [(q.record_index, q.cause) for q in parsed.quarantine] == [(None, cause)]


def test_file_sha256_changes_with_content():
    assert file_sha256(b"a") != file_sha256(b"b")
    assert len(file_sha256(b"a")) == 64
