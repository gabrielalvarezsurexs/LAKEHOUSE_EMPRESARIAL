"""Checks de calidad con Spark: el pipeline sintetico pasa todo y los errores sembrados se detectan.

Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

import json

import pytest

from lakehouse.common.delta import TableLocator, overwrite, read_table
from lakehouse.governance.contracts import (
    GOLD_DIM_ARTIST,
    GOLD_DIM_TRACK,
    GOLD_FACT_PLAYS,
    SILVER_ARTISTS,
)
from lakehouse.governance.quality import run_quality, table_checks
from lakehouse.ingestion.bronze import BronzeTables, ingest_bronze
from lakehouse.ingestion.musicbrainz import enrich_genres
from lakehouse.ingestion.synthetic import SyntheticSource, generate_streams
from lakehouse.transformation.gold import build_gold
from lakehouse.transformation.silver import build_silver

pytestmark = pytest.mark.spark


@pytest.fixture
def lakehouse(spark, store, settings, tmp_path):
    """landing > Bronce > Plata > Oro con sinteticos en tmp_path."""
    rows = generate_streams(600, seed=7)
    store.put_bytes("landing", "spotify/2026-11-23/a.json", json.dumps(rows).encode("utf-8"))
    bronze = BronzeTables.at(str(tmp_path / "bronze"))
    ingest_bronze(spark, store, settings, bronze)
    enrich_genres(spark, store, settings, SyntheticSource(), bronze)
    locator = TableLocator.at(str(tmp_path))
    build_silver(spark, locator)
    build_gold(spark, locator)
    return locator


def _failed(results):
    return {(r.kind, r.check): r.failed for r in results if not r.ok}


def test_synthetic_pipeline_passes_every_check(spark, lakehouse):
    run = run_quality(spark, lakehouse)
    assert run.ok, [(r.table, r.check, r.detail) for r in run.errors]
    assert not [r for r in run.warnings if r.kind in ("esquema", "metadatos", "frescura")], run.warnings
    assert run.row_counts["bronze.spotify_streams"] == 600
    kinds = {r.kind for r in run.results}
    assert {"no_nulos", "valores", "rango", "regla", "unicos", "referencia", "reconciliacion"} <= kinds


def test_seeded_errors_are_caught(spark, lakehouse, tmp_path):
    from pyspark.sql import functions as F

    artists = read_table(spark, lakehouse, SILVER_ARTISTS)
    first = artists.limit(1)
    bad = artists.unionByName(first).unionByName(first.withColumn("mb_status", F.lit("inventado")))
    bad_locator = TableLocator.at(str(tmp_path / "malo"))
    overwrite(spark, bad.localCheckpoint(), bad_locator, SILVER_ARTISTS)
    failed = _failed(table_checks(spark, bad_locator, SILVER_ARTISTS)[1])
    assert failed[("unicos", "unica por (artist_name)")] == 2
    assert failed[("valores", "mb_status en {matched, no_match, not_queried}")] == 1


def test_orphan_keys_and_ip_columns_are_caught(spark, lakehouse, tmp_path):
    from pyspark.sql import functions as F

    # Oro con un artista que no esta en dim_artist y una columna de IP que no deberia existir.
    bad = TableLocator.at(str(tmp_path / "malo"))
    for contract in (GOLD_DIM_ARTIST, GOLD_DIM_TRACK):
        overwrite(spark, read_table(spark, lakehouse, contract), bad, contract)
    fact = read_table(spark, lakehouse, GOLD_FACT_PLAYS)
    orphan = fact.limit(3).withColumn("artist_key", F.lit("0" * 16))
    fact = fact.unionByName(orphan).withColumn("ip_addr_hash", F.lit("x"))
    fact.write.format("delta").partitionBy("year").save(bad.uri(GOLD_FACT_PLAYS))

    total, results = table_checks(spark, bad, GOLD_FACT_PLAYS)
    failed = _failed(results)
    assert failed[("referencia", "artist_key existe en gold.dim_artist.artist_key")] == 3
    assert failed[("privacidad", "sin IP ni seudonimo donde no toca")] == 1
    assert failed[("esquema", "sin columnas fuera del contrato")] == 1
    assert failed[("unicos", "unica por (play_id)")] == 3
    assert ("referencia", "date_key existe en gold.dim_date.date_key") in failed  # dim_date no se copio


def test_missing_table_is_reported(spark, tmp_path):
    total, results = table_checks(spark, TableLocator.at(str(tmp_path)), SILVER_ARTISTS)
    assert total is None and [(r.kind, r.ok) for r in results] == [("existe", False)]
