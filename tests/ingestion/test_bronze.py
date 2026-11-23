"""Pruebas de Bronce con Spark sobre disco local: conteos, cuarentena e idempotencia.

Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

import json

import pytest

from lakehouse.ingestion.bronze import BronzeTables, compact_tables, ingest_bronze, table_counts
from lakehouse.ingestion.musicbrainz import enrich_genres
from lakehouse.ingestion.synthetic import SyntheticSource, generate_streams

pytestmark = pytest.mark.spark


def load(store, key, rows):
    store.put_bytes("landing", f"spotify/2026-11-23/{key}", json.dumps(rows).encode("utf-8"))


def test_bronze_counts_quarantine_and_idempotency(spark, store, settings, tmp_path):
    rows = generate_streams(400, seed=11)
    bad = [dict(rows[0], ms_played="x")]
    load(store, "a.json", rows[:250])
    load(store, "b.json", rows[250:] + bad)
    load(store, "roto.json", [])
    store.put_bytes("landing", "spotify/2026-11-23/c.json", b"{no es json")
    tables = BronzeTables.at(str(tmp_path / "bronze"))

    first = ingest_bronze(spark, store, settings, tables)
    expected = {"spotify_streams": 400, "_quarantine": 2, "_ingestion_log": 4}
    assert (first.rows_loaded, first.rows_quarantined) == (400, 2)
    assert table_counts(spark, tables) == expected
    # Compactado al final (H-009): un archivo por tabla, sin perder filas.
    assert set(compact_tables(spark, tables).values()) <= {0, 1}

    second = ingest_bronze(spark, store, settings, tables)
    assert (len(second.files), len(second.skipped)) == (0, 4)
    assert table_counts(spark, tables) == expected

    ingest_bronze(spark, store, settings, tables, force=True)
    counts = table_counts(spark, tables)
    assert counts["spotify_streams"] == 400 and counts["_quarantine"] == 2  # replaceWhere no duplica

    streams = spark.read.format("delta").load(tables.table_uri("spotify_streams"))
    assert "ip_addr" not in streams.columns
    assert streams.where("ip_addr_hash IS NULL").count() == 0


def test_genres_from_synthetic_source(spark, store, settings, tmp_path):
    rows = generate_streams(300, seed=5)
    load(store, "a.json", rows)
    tables = BronzeTables.at(str(tmp_path / "bronze"))
    ingest_bronze(spark, store, settings, tables)

    artists = {r["master_metadata_album_artist_name"] for r in rows} - {None}
    result = enrich_genres(spark, store, settings, SyntheticSource(), tables)
    assert result.fetched == len(artists)
    again = enrich_genres(spark, store, settings, SyntheticSource(), tables)
    assert (again.fetched, again.already_cached) == (0, len(artists))
    table = spark.read.format("delta").load(tables.table_uri("musicbrainz_artists"))
    assert table.count() == len(artists)
    assert table.where("status = 'matched' AND size(genres) > 0").count() == result.matched
