"""Pipeline con sinteticos (landing > Bronce > Plata > Oro): reconciliacion, MERGE incremental y KPIs.

Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

import json

import pytest

from lakehouse.common.delta import TableLocator, overwrite, read_table
from lakehouse.governance.contracts import (
    GOLD_DIM_DATE,
    GOLD_FACT_PLAYS,
    GOLD_KPI_ARTIST_RETENTION,
    GOLD_KPI_GENRES_MONTHLY,
    GOLD_KPI_TOP_ARTISTS_MONTHLY,
    SILVER_PLAYS,
)
from lakehouse.ingestion.bronze import BronzeTables, ingest_bronze
from lakehouse.ingestion.musicbrainz import enrich_genres
from lakehouse.ingestion.synthetic import SyntheticSource, generate_streams
from lakehouse.transformation.gold import build_gold
from lakehouse.transformation.reconcile import reconcile
from lakehouse.transformation.silver import build_silver

pytestmark = pytest.mark.spark


@pytest.fixture
def pipeline(spark, store, settings, tmp_path):
    rows = generate_streams(800, seed=3)
    store.put_bytes("landing", "spotify/2026-11-23/a.json", json.dumps(rows).encode("utf-8"))
    bronze = BronzeTables.at(str(tmp_path / "bronze"))
    ingest_bronze(spark, store, settings, bronze)
    enrich_genres(spark, store, settings, SyntheticSource(), bronze)
    locator = TableLocator.at(str(tmp_path))
    silver = build_silver(spark, locator)
    return locator, silver, build_gold(spark, locator)


def test_gold_reconciles_and_merge_is_incremental(spark, pipeline):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    locator, silver, gold = pipeline
    assert gold.merge.inserted == silver.plays > 0 and gold.optimized
    assert silver.artists_with_genre > 0
    result = reconcile(spark, locator)
    assert result.ok, [c.detail for c in result.checks if not c.ok]

    again = build_gold(spark, locator)
    assert not again.merge.changed and not again.optimized

    # Plata cambia: se van 2 reproducciones y otra cambia su etiqueta. El MERGE toca solo esas 3.
    plays = read_table(spark, locator, SILVER_PLAYS)
    gone = [r["play_id"] for r in plays.orderBy("play_id").limit(2).collect()]
    target = plays.where(~F.col("play_id").isin(gone) & (F.col("is_skip") == F.lit(False)))
    target = target.orderBy("play_id").first()["play_id"]
    changed = plays.where(~F.col("play_id").isin(gone)).withColumn(
        "is_skip", F.when(F.col("play_id") == target, F.lit(True)).otherwise(F.col("is_skip"))
    )
    overwrite(spark, changed.localCheckpoint(), locator, SILVER_PLAYS)
    third = build_gold(spark, locator)
    assert (third.merge.inserted, third.merge.updated, third.merge.deleted) == (0, 1, 2)
    checks = {c.name: c.ok for c in reconcile(spark, locator).checks}
    assert checks["plata = oro"] and not checks["bronce = plata + descartes"]  # se borro a mano en Plata

    detail = DeltaTable.forPath(spark, locator.uri(GOLD_FACT_PLAYS)).detail().first()
    assert list(detail["partitionColumns"]) == ["year"]


def test_gold_marts_are_consistent(spark, pipeline):
    from pyspark.sql import functions as F

    locator, _, _ = pipeline
    dates = read_table(spark, locator, GOLD_DIM_DATE)
    first, last, days = dates.agg(F.min("date"), F.max("date"), F.count("*")).first()
    assert days == (last - first).days + 1

    top = read_table(spark, locator, GOLD_KPI_TOP_ARTISTS_MONTHLY)
    months = top.select("year_month").distinct().count()
    assert top.where("rank_in_month = 1").count() == months

    shares = (
        read_table(spark, locator, GOLD_KPI_GENRES_MONTHLY).groupBy("year_month").agg(F.sum("minutes_share"))
    )
    assert all(abs(r[1] - 1) < 1e-9 for r in shares.collect())

    retention = read_table(spark, locator, GOLD_KPI_ARTIST_RETENTION)
    assert retention.where("months_since = 0 AND retention_rate != 1").count() == 0
    assert retention.where("artists_active > cohort_size").count() == 0
