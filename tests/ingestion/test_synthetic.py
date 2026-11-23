"""Pruebas del generador de datos sinteticos.

Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

import json

from lakehouse.governance.contracts import SPOTIFY_STREAMS_SOURCE
from lakehouse.ingestion.__main__ import main
from lakehouse.ingestion.records import parse_spotify_file
from lakehouse.ingestion.synthetic import SyntheticSource, generate_streams, synthetic_genres, write_files

SALT = "sal-de-prueba-0123456789"


def test_exact_schema_and_count():
    rows = generate_streams(2000, seed=1)
    assert len(rows) == 2000
    assert all(list(r) == list(SPOTIFY_STREAMS_SOURCE.column_names) for r in rows)
    assert [r["ts"] for r in rows] == sorted(r["ts"] for r in rows)


def test_deterministic_by_seed():
    assert generate_streams(300, seed=7) == generate_streams(300, seed=7)
    assert generate_streams(300, seed=7) != generate_streams(300, seed=8)


def test_all_rows_pass_the_contract_and_look_like_spotify():
    rows = generate_streams(3000, seed=3)
    parsed = parse_spotify_file(json.dumps(rows).encode("utf-8"), SALT)
    assert parsed.quarantine == []
    assert parsed.unknown_fields == set()
    assert any(r["spotify_episode_uri"] for r in rows)
    assert any(r["spotify_track_uri"] is None and r["spotify_episode_uri"] is None for r in rows)
    assert any(r["reason_end"] == "fwdbtn" for r in rows)
    assert all(r["audiobook_title"] is None for r in rows)


def test_write_files_marks_synthetic(tmp_path):
    paths = write_files(generate_streams(100, seed=1), tmp_path, n_files=2)
    assert len(paths) == 2
    assert all("SINTETICO" in p.name for p in paths)
    assert sum(len(json.loads(p.read_text(encoding="utf-8"))) for p in paths) == 100


def test_cli_synthetic(tmp_path):
    assert main(["synthetic", "--out", str(tmp_path), "--rows", "50", "--files", "1"]) == 0
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_synthetic_genres_are_deterministic_and_some_missing():
    names = [f"Artista Sintetico {i:04d}" for i in range(1, 201)]
    genres = [synthetic_genres(n) for n in names]
    assert genres == [synthetic_genres(n) for n in names]
    assert 0 < sum(g is None for g in genres) < 40
    assert synthetic_genres("Artista Real") is None
    doc = SyntheticSource().fetch_doc(names[0])
    assert doc["source"] == "synthetic"
    assert doc["status"] in ("matched", "no_match")
