"""Modelo de skip sin Spark: matriz de features, split temporal, baseline, entrenamiento y metricas (F6).

Split temporal sobre las reproducciones etiquetadas: 70% mas antiguo entrena, 15% siguiente elige el
umbral (validacion) y el 15% mas reciente mide (prueba). El baseline es la tasa de skip historica.
Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lakehouse.common.errors import LakehouseError

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    from sklearn.pipeline import Pipeline

CATEGORICAL = ("platform_family", "reason_start", "primary_genre")
BOOLEAN = ("is_weekend", "shuffle", "offline", "incognito_mode", "prev_is_skip")
NUMERIC = (
    "local_hour",
    "local_weekday",
    "session_seq",
    "session_minutes",
    "prev_minutes_played",
    "track_prior_plays",
    "track_prior_skip_rate",
    "days_since_track_first",
    "artist_prior_plays",
    "artist_prior_skip_rate",
    "user_recent_skip_rate",
)
FEATURES = CATEGORICAL + BOOLEAN + NUMERIC
MISSING = "(sin dato)"  # categoria para null: el modelo aprende que falta, no se inventa un valor
TRAIN_SHARE, VALIDATION_SHARE = 0.70, 0.15
MIN_LABELED = 200
SEED = 42
TRAIN, VALIDATION, TEST, UNLABELED = "train", "validation", "test", "unlabeled"
MODEL_PARAMS = {
    "learning_rate": 0.05,
    "max_iter": 400,
    "max_leaf_nodes": 31,
    "min_samples_leaf": 40,
    "l2_regularization": 1.0,
    "early_stopping": True,
    "n_iter_no_change": 20,
    "random_state": SEED,
}
ENCODER_PARAMS = {"min_frequency": 20, "max_categories": 12}


class TrainingError(LakehouseError):
    """No hay datos suficientes (o de las dos clases) para entrenar o medir el modelo."""


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Columnas del modelo en su orden: categorias como texto, booleanos y numeros como float (NaN = null)."""
    import pandas as pd

    out = pd.DataFrame(index=df.index)
    for col in CATEGORICAL:
        out[col] = df[col].astype(object).where(df[col].notna(), MISSING).astype(str)
    for col in BOOLEAN:
        out[col] = df[col].map({True: 1.0, False: 0.0}).astype(float)
    for col in NUMERIC:
        out[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    return out[list(FEATURES)]


@dataclass(frozen=True)
class SplitCuts:
    """Cortes en epoch (segundos UTC): train < train_end <= validation < validation_end <= test."""

    train_end: int
    validation_end: int

    def assign(self, ts_epoch: pd.Series, labeled: pd.Series) -> pd.Series:
        import numpy as np
        import pandas as pd

        split = np.select(
            [~labeled.to_numpy(bool), ts_epoch < self.train_end, ts_epoch < self.validation_end],
            [UNLABELED, TRAIN, VALIDATION],
            default=TEST,
        )
        return pd.Series(split, index=ts_epoch.index)


def temporal_cuts(ts_epoch: pd.Series) -> SplitCuts:
    """Cortes por cuantiles de tiempo de las etiquetadas (70/15/15): el futuro nunca entra a train."""
    ordered = ts_epoch.sort_values().to_numpy()
    n = len(ordered)
    if n < MIN_LABELED:
        raise TrainingError(f"Solo hay {n} reproducciones etiquetadas; se necesitan al menos {MIN_LABELED}")
    train_end = int(ordered[int(n * TRAIN_SHARE)])
    validation_end = int(ordered[int(n * (TRAIN_SHARE + VALIDATION_SHARE))])
    if not ordered[0] < train_end < validation_end:
        raise TrainingError("Los cortes temporales no separan train, validacion y prueba")
    return SplitCuts(train_end, validation_end)


def baseline_scores(df: pd.DataFrame, train_rate: float) -> np.ndarray:
    """Tasa de skip historica: la de la cancion; si no tiene, la reciente del usuario; si no, la de train."""
    import pandas as pd

    track = pd.to_numeric(df["track_prior_skip_rate"], errors="coerce")
    user = pd.to_numeric(df["user_recent_skip_rate"], errors="coerce")
    return track.fillna(user).fillna(train_rate).clip(0, 1).to_numpy(float)


def build_pipeline() -> Pipeline:
    """One-hot de categorias (las raras se juntan) + gradient boosting, que acepta NaN en los numeros."""
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    encoder = OneHotEncoder(handle_unknown="infrequent_if_exist", sparse_output=False, **ENCODER_PARAMS)
    columns = ColumnTransformer(
        [("categorias", encoder, list(CATEGORICAL))], remainder="passthrough", verbose_feature_names_out=False
    )
    return Pipeline([("features", columns), ("modelo", HistGradientBoostingClassifier(**MODEL_PARAMS))])


def best_threshold(y: np.ndarray, scores: np.ndarray) -> float:
    """Umbral que maximiza F1 (se elige en validacion, nunca en prueba)."""
    import numpy as np
    from sklearn.metrics import precision_recall_curve

    precision, recall, thresholds = precision_recall_curve(y, scores)
    f1 = np.divide(
        2 * precision * recall, precision + recall, out=np.zeros_like(precision), where=precision + recall > 0
    )
    return float(np.clip(thresholds[int(np.argmax(f1[:-1]))], 0, 1))


def classification_metrics(y: np.ndarray, scores: np.ndarray, threshold: float) -> dict[str, float | int]:
    """Metricas de una particion; solo numeros agregados (van al repo y a MLflow)."""
    import numpy as np
    from sklearn.metrics import (
        accuracy_score,
        average_precision_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    predicted = (scores >= threshold).astype(int)
    tn, fp, fn, tp = (int(v) for v in confusion_matrix(y, predicted, labels=[0, 1]).ravel())
    return {
        "rows": int(len(y)),
        "skip_rate": float(np.mean(y)),
        "roc_auc": float(roc_auc_score(y, scores)),
        "pr_auc": float(average_precision_score(y, scores)),
        "f1": float(f1_score(y, predicted, zero_division=0)),
        "precision": float(precision_score(y, predicted, zero_division=0)),
        "recall": float(recall_score(y, predicted, zero_division=0)),
        "accuracy": float(accuracy_score(y, predicted)),
        "threshold": float(threshold),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
    }


@dataclass
class TrainResult:
    """Modelo entrenado, umbrales, cortes y metricas de modelo y baseline por particion."""

    pipeline: Pipeline
    threshold: float
    baseline_threshold: float
    cuts: SplitCuts
    train_rate: float
    metrics: dict[str, dict[str, dict]]  # {"model"|"baseline": {"validation"|"test": {...}}}
    importance: list[dict[str, float | str]]
    split_rows: dict[str, int]
    test_curves: dict[str, np.ndarray] = field(repr=False, default_factory=dict)

    @property
    def beats_baseline(self) -> bool:
        """Le gana en prueba en las dos metricas de ranking (ROC-AUC y PR-AUC)."""
        model, base = self.metrics["model"]["test"], self.metrics["baseline"]["test"]
        return model["roc_auc"] > base["roc_auc"] and model["pr_auc"] > base["pr_auc"]


def _labels(df: pd.DataFrame) -> np.ndarray:
    return df["is_skip"].map({True: 1, False: 0}).to_numpy(int)


def permutation_ranking(pipeline: Pipeline, X: pd.DataFrame, y: np.ndarray) -> list[dict[str, float | str]]:
    """Cuanto baja el ROC-AUC de prueba al revolver cada feature (por columna original, sin valores)."""
    from sklearn.inspection import permutation_importance

    result = permutation_importance(pipeline, X, y, scoring="roc_auc", n_repeats=5, random_state=SEED)
    ranking = [
        {"feature": name, "importance": float(mean), "std": float(std)}
        for name, mean, std in zip(X.columns, result.importances_mean, result.importances_std, strict=True)
    ]
    return sorted(ranking, key=lambda r: r["importance"], reverse=True)


def train(df: pd.DataFrame) -> TrainResult:
    """Entrena con las etiquetadas de df (columnas FEATURES, is_skip y ts_epoch) y mide contra el baseline."""
    labeled = df[df["is_skip"].notna()].sort_values("ts_epoch")
    cuts = temporal_cuts(labeled["ts_epoch"])
    split = cuts.assign(labeled["ts_epoch"], labeled["is_skip"].notna())
    X, y = prepare(labeled), _labels(labeled)
    masks = {name: (split == name).to_numpy() for name in (TRAIN, VALIDATION, TEST)}
    for name, mask in masks.items():
        if len(set(y[mask])) < 2:
            raise TrainingError(f"La particion {name} no tiene skips y no skips; no se puede medir")

    train_rate = float(y[masks[TRAIN]].mean())
    pipeline = build_pipeline().fit(X[masks[TRAIN]], y[masks[TRAIN]])
    model = {k: pipeline.predict_proba(X[m])[:, 1] for k, m in masks.items()}
    base = {k: baseline_scores(labeled[m], train_rate) for k, m in masks.items()}
    threshold = best_threshold(y[masks[VALIDATION]], model[VALIDATION])
    base_threshold = best_threshold(y[masks[VALIDATION]], base[VALIDATION])
    metrics = {
        "model": {k: classification_metrics(y[masks[k]], model[k], threshold) for k in (VALIDATION, TEST)},
        "baseline": {
            k: classification_metrics(y[masks[k]], base[k], base_threshold) for k in (VALIDATION, TEST)
        },
    }
    return TrainResult(
        pipeline=pipeline,
        threshold=threshold,
        baseline_threshold=base_threshold,
        cuts=cuts,
        train_rate=train_rate,
        metrics=metrics,
        importance=permutation_ranking(pipeline, X[masks[TEST]], y[masks[TEST]]),
        split_rows={k: int(m.sum()) for k, m in masks.items()} | {UNLABELED: int(df["is_skip"].isna().sum())},
        test_curves={"y": y[masks[TEST]], "model": model[TEST], "baseline": base[TEST]},
    )


def score(pipeline: Pipeline, df: pd.DataFrame) -> np.ndarray:
    """Probabilidad de skip para cada fila de df (con o sin etiqueta)."""
    return pipeline.predict_proba(prepare(df))[:, 1]
