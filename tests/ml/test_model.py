"""Modelo de skip sin Spark: preparacion, split temporal, baseline, entrenamiento, reporte y MLflow local.

Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

import json
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from lakehouse.governance.contracts import GOLD_ML_SKIP_FEATURES
from lakehouse.ml import model as m
from lakehouse.ml import registry, report
from lakehouse.orchestration import steps

EM_DASH = "\u2014"


def features_frame(n=1500, seed=0, unlabeled=0.1):
    """Features falsas con senal en reason_start y shuffle; el baseline (track_prior_skip_rate) es ruido."""
    rng = np.random.default_rng(seed)
    reason = rng.choice(["clickrow", "fwdbtn", "trackdone", None], n)
    shuffle = rng.random(n) < 0.4
    p = 0.15 + 0.5 * (reason == "fwdbtn") + 0.2 * shuffle
    label = (rng.random(n) < p).astype(object)
    label[rng.random(n) < unlabeled] = None
    df = pd.DataFrame(
        {
            "play_id": [f"p{i:05d}" for i in range(n)],
            "ts_epoch": np.sort(rng.integers(1_550_000_000, 1_750_000_000, n)),
            "is_skip": label,
            "platform_family": rng.choice(["android", "ios", "windows"], n),
            "reason_start": reason,
            "primary_genre": rng.choice(["rock", "pop", None], n),
            "is_weekend": rng.choice([True, False, None], n),
            "shuffle": shuffle,
            "offline": rng.random(n) < 0.1,
            "incognito_mode": False,
            "prev_is_skip": rng.choice([True, False, None], n),
            "local_hour": rng.integers(0, 24, n),
            "local_weekday": rng.integers(1, 8, n),
            "session_seq": rng.integers(1, 20, n),
            "session_minutes": rng.random(n) * 90,
            "prev_minutes_played": np.where(rng.random(n) < 0.2, None, rng.random(n) * 4),
            "track_prior_plays": rng.integers(0, 30, n),
            "track_prior_skip_rate": np.where(rng.random(n) < 0.3, None, rng.random(n)),
            "days_since_track_first": rng.random(n) * 300,
            "artist_prior_plays": rng.integers(0, 100, n),
            "artist_prior_skip_rate": rng.random(n),
            "user_recent_skip_rate": np.where(rng.random(n) < 0.05, None, rng.random(n)),
        }
    )
    return df


@pytest.fixture(scope="module")
def trained():
    df = features_frame()
    return df, m.train(df)


def test_features_are_contract_columns():
    names = set(GOLD_ML_SKIP_FEATURES.column_names)
    assert set(m.FEATURES) <= names
    # El resultado de la propia reproduccion nunca es feature (fuga).
    assert not {"ms_played", "minutes_played", "reason_end", "is_skip"} & set(m.FEATURES)
    assert names - set(m.FEATURES) == {"play_id", "ts_start", "date_key", "is_skip"}


def test_prepare_keeps_nulls_as_missing_not_zero():
    df = features_frame(n=5)
    df.loc[0, "reason_start"], df.loc[0, "prev_is_skip"], df.loc[0, "track_prior_skip_rate"] = (
        None,
        None,
        None,
    )
    X = m.prepare(df)
    assert list(X.columns) == list(m.FEATURES)
    assert X.loc[0, "reason_start"] == m.MISSING
    assert np.isnan(X.loc[0, "prev_is_skip"]) and np.isnan(X.loc[0, "track_prior_skip_rate"])
    assert set(X["shuffle"].unique()) <= {0.0, 1.0}


def test_temporal_split_never_puts_the_future_in_train():
    df = features_frame()
    labeled = df[df["is_skip"].notna()]
    cuts = m.temporal_cuts(labeled["ts_epoch"])
    split = cuts.assign(df["ts_epoch"], df["is_skip"].notna())
    ts = df["ts_epoch"]
    assert ts[split == m.TRAIN].max() < ts[split == m.VALIDATION].min()
    assert ts[split == m.VALIDATION].max() < ts[split == m.TEST].min()
    assert (split[df["is_skip"].isna()] == m.UNLABELED).all()
    share = (split[df["is_skip"].notna()] == m.TRAIN).mean()
    assert 0.65 < share < 0.75


def test_too_few_labels_fail_with_a_clear_error():
    with pytest.raises(m.TrainingError, match="al menos"):
        m.train(features_frame(n=150))


def test_baseline_falls_back_from_track_to_user_to_train_rate():
    df = pd.DataFrame({"track_prior_skip_rate": [0.8, None, None], "user_recent_skip_rate": [0.1, 0.3, None]})
    assert m.baseline_scores(df, 0.42).tolist() == [0.8, 0.3, 0.42]


def test_model_beats_a_noise_baseline_and_reports_metrics(trained):
    _, result = trained
    assert result.beats_baseline
    test = result.metrics["model"]["test"]
    assert test["tn"] + test["fp"] + test["fn"] + test["tp"] == test["rows"] == result.split_rows[m.TEST]
    assert 0 < result.threshold < 1
    assert result.importance[0]["feature"] in {"reason_start", "shuffle"}
    assert {r["feature"] for r in result.importance} == set(m.FEATURES)


def test_scores_are_probabilities_for_labeled_and_unlabeled(trained):
    df, result = trained
    p = m.score(result.pipeline, df)
    assert len(p) == len(df) and ((p >= 0) & (p <= 1)).all()


def test_report_has_only_aggregates(trained, tmp_path):
    _, result = trained
    figures = report.write_figures(result, tmp_path)
    assert all(p.exists() and p.stat().st_size > 0 for p in figures)
    path = report.write_report(result, tmp_path, "a" * 32, "3", datetime(2026, 11, 26, tzinfo=UTC))
    text = path.read_text(encoding="utf-8")
    assert "le gana al baseline" in text and "versi\u00f3n 3" in text and "`champion`" in text
    assert EM_DASH not in text and "p0" not in text  # ningun play_id
    doc = json.loads((tmp_path / report.METRICS_FILE).read_text(encoding="utf-8"))
    assert doc["beats_baseline"] is True and doc["split_rows"] == result.split_rows


def test_run_params_have_what_inference_reads(trained):
    _, result = trained
    params = registry.run_params(result)
    for key in ("threshold", "train_rate", "train_end_epoch", "validation_end_epoch"):
        assert key in params
    flat = registry.flat_metrics(result)
    assert flat["model_test_roc_auc"] == result.metrics["model"]["test"]["roc_auc"]


def test_model_name_matches_orchestration():
    assert steps.ML_MODEL_NAME == registry.MODEL_NAME


def test_mlflow_roundtrip_with_local_store(trained, tmp_path):
    """Registra, pone champion y la inferencia carga esa version con su umbral y cortes."""
    df, result = trained
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    sample = m.prepare(df.head(20))
    run_id, version = registry.log_training(uri, result, sample, [])
    loaded = registry.load_for_inference(uri)
    assert (loaded.version, loaded.run_id) == (version, run_id)
    assert loaded.threshold == pytest.approx(result.threshold)
    assert loaded.cuts == result.cuts
    assert np.allclose(m.score(loaded.pipeline, df.head(50)), m.score(result.pipeline, df.head(50)))
