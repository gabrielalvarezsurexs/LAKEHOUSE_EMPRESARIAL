"""Features sin fuga y pipeline de ML completo con Spark (contenedor o CI): features, entrenamiento,
registro en un MLflow local (SQLite) e inferencia por lote, con los checks de gobernanza en verde.

Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

import json
from datetime import UTC, datetime, timedelta

import pytest

from lakehouse.common.delta import TableLocator, read_table
from lakehouse.governance.contracts import (
    GOLD_DIM_ARTIST,
    GOLD_FACT_PLAYS,
    GOLD_ML_SKIP_FEATURES,
    GOLD_ML_SKIP_PREDICTIONS,
)

pytestmark = pytest.mark.spark

START = datetime(2025, 1, 6, 12, 0, tzinfo=UTC)  # lunes


def fact_row(i, minutes, track, artist, session, seq, is_skip):
    ts_start = START + timedelta(minutes=minutes)
    return {
        "play_id": f"p{i}",
        "date_key": int(ts_start.strftime("%Y%m%d")),
        "ts_start": ts_start,
        "ts_end": ts_start + timedelta(minutes=3),
        "local_hour": 6,
        "local_weekday": 1,
        "track_key": track,
        "artist_key": artist,
        "platform_family": "android",
        "conn_country": "MX",
        "session_id": session,
        "session_seq": seq,
        "ms_played": 180_000,
        "minutes_played": 3.0,
        "is_skip": is_skip,
        "reason_start": "clickrow",
        "reason_end": "trackdone",
        "shuffle": False,
        "offline": False,
        "incognito_mode": False,
        "year": ts_start.year,
    }


def _write(spark, locator, contract, rows):
    names = contract.column_names
    df = spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], contract.spark_schema())
    df.write.format("delta").mode("overwrite").save(locator.uri(contract))


def test_features_only_use_earlier_plays(spark, tmp_path):
    from lakehouse.ml.features import skip_features

    locator = TableLocator.at(str(tmp_path))
    rows = [
        fact_row(1, 0, "t:a", "art1", "s1", 1, True),
        fact_row(2, 4, "t:b", "art1", "s1", 2, False),
        fact_row(3, 8, "t:a", "art1", "s1", 3, None),  # sin etiqueta
        fact_row(4, 60 * 24 * 2, "t:a", "art1", "s2", 1, False),  # dos dias despues
    ]
    _write(spark, locator, GOLD_FACT_PLAYS, rows)
    artists = [{"artist_key": "art1", "artist_name": "X", "primary_genre": "rock", "mb_status": "matched"}]
    _write(spark, locator, GOLD_DIM_ARTIST, artists)
    fact = read_table(spark, locator, GOLD_FACT_PLAYS)
    out = {
        r["play_id"]: r for r in skip_features(fact, read_table(spark, locator, GOLD_DIM_ARTIST)).collect()
    }

    first = out["p1"]
    assert (first["track_prior_plays"], first["track_prior_skip_rate"]) == (0, None)
    assert first["days_since_track_first"] == 0 and first["prev_is_skip"] is None
    assert first["user_recent_skip_rate"] is None and first["primary_genre"] == "rock"

    third = out["p3"]  # t:a ya sono una vez (skip): su propia etiqueta (null) no cuenta
    assert (third["track_prior_plays"], third["track_prior_skip_rate"]) == (1, 1.0)
    assert third["prev_is_skip"] is False and third["prev_minutes_played"] == 3.0
    assert third["session_minutes"] == 8.0 and third["artist_prior_plays"] == 2
    assert third["user_recent_skip_rate"] == 0.5

    fourth = out["p4"]  # la tercera no tiene etiqueta: la tasa sigue 1 de 1
    assert (fourth["track_prior_plays"], fourth["track_prior_skip_rate"]) == (2, 1.0)
    assert fourth["days_since_track_first"] == 2.0 and fourth["session_minutes"] == 0.0
    assert fourth["is_weekend"] is False


@pytest.fixture
def gold(spark, store, settings, tmp_path):
    from lakehouse.ingestion.bronze import BronzeTables, ingest_bronze
    from lakehouse.ingestion.musicbrainz import enrich_genres
    from lakehouse.ingestion.synthetic import SyntheticSource, generate_streams
    from lakehouse.transformation.gold import build_gold
    from lakehouse.transformation.silver import build_silver

    rows = generate_streams(1200, seed=5)
    store.put_bytes("landing", "spotify/2026-11-26/a.json", json.dumps(rows).encode("utf-8"))
    bronze = BronzeTables.at(str(tmp_path / "bronze"))
    ingest_bronze(spark, store, settings, bronze)
    enrich_genres(spark, store, settings, SyntheticSource(), bronze)
    locator = TableLocator.at(str(tmp_path))
    build_silver(spark, locator)
    build_gold(spark, locator)
    return locator


def test_ml_pipeline_features_train_predict(spark, gold, tmp_path):
    from lakehouse.governance.quality import run_quality
    from lakehouse.ml.__main__ import predict_step, train_step
    from lakehouse.ml.features import build_features
    from lakehouse.ml.report import REPORT_FILE

    rows = build_features(spark, gold)
    assert rows == read_table(spark, gold, GOLD_FACT_PLAYS).count()

    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    report_dir = tmp_path / "reporte"
    train_step(spark, gold, report_dir, uri)
    assert (report_dir / REPORT_FILE).exists()
    assert predict_step(spark, gold, uri) == rows

    predictions = read_table(spark, gold, GOLD_ML_SKIP_PREDICTIONS)
    splits = {r["split"] for r in predictions.select("split").distinct().collect()}
    assert {"train", "validation", "test"} <= splits

    run = run_quality(spark, gold, (GOLD_ML_SKIP_FEATURES, GOLD_ML_SKIP_PREDICTIONS), cross_layer=False)
    assert run.ok, [(r.table, r.check, r.detail) for r in run.errors]
