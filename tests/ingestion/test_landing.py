"""Pruebas de la carga a landing con un store en memoria.

Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from datetime import date

import pytest

from lakehouse.ingestion.landing import IngestionError, upload_spotify_files
from lakehouse.ingestion.records import file_sha256

DAY = date(2026, 11, 23)


def write(tmp_path, name, content=b"[]"):
    (tmp_path / name).write_bytes(content)


def test_upload_keeps_content_and_sha(tmp_path, store):
    write(tmp_path, "Streaming_History_Audio_0.json", b"[1]")
    result = upload_spotify_files(store, tmp_path, today=DAY)
    assert result.uploaded == ["spotify/2026-11-23/Streaming_History_Audio_0.json"]
    key = result.uploaded[0]
    assert store.get_bytes("landing", key) == b"[1]"
    assert store.get_metadata("landing", key)["sha256"] == file_sha256(b"[1]")


def test_rerun_does_not_duplicate_even_other_day_or_name(tmp_path, store):
    write(tmp_path, "a.json", b"[1]")
    upload_spotify_files(store, tmp_path, today=DAY)
    (tmp_path / "a.json").rename(tmp_path / "b.json")
    result = upload_spotify_files(store, tmp_path, today=date(2026, 11, 24))
    assert result.uploaded == []
    assert len(store.list_keys("landing", "spotify/")) == 1


def test_same_name_new_content_does_not_overwrite(tmp_path, store):
    write(tmp_path, "a.json", b"[1]")
    upload_spotify_files(store, tmp_path, today=DAY)
    write(tmp_path, "a.json", b"[2]")
    result = upload_spotify_files(store, tmp_path, today=DAY)
    assert len(result.uploaded) == 1
    assert result.uploaded[0] != "spotify/2026-11-23/a.json"
    assert len(store.list_keys("landing", "spotify/")) == 2


def test_empty_source_dir_fails_with_clear_message(tmp_path, store):
    with pytest.raises(IngestionError, match="No hay archivos"):
        upload_spotify_files(store, tmp_path)


def test_synthetic_and_real_never_mix(tmp_path, store):
    write(tmp_path, "Streaming_History_Audio_0.json", b"[1]")
    upload_spotify_files(store, tmp_path, today=DAY)
    other = tmp_path / "sinteticos"
    other.mkdir()
    write(other, "Streaming_History_Audio_SINTETICO_2019-2026_0.json", b"[2]")
    with pytest.raises(IngestionError, match="down -v"):
        upload_spotify_files(store, other, today=DAY)
