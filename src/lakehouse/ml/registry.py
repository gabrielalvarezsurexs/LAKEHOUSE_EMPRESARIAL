"""MLflow: registra cada entrenamiento (parametros, metricas, graficas y modelo) y carga el de inferencia.

La version que le gana al baseline en prueba recibe el alias champion; la inferencia usa champion y, si
nunca hubo uno (p. ej. con sinteticos), la version mas reciente. No se guardan filas de ejemplo (privacidad).
Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.ml.model import ENCODER_PARAMS, FEATURES, MODEL_PARAMS, SplitCuts, TrainResult

if TYPE_CHECKING:
    import pandas as pd
    from sklearn.pipeline import Pipeline

log = get_logger("lakehouse.ml.registry")

EXPERIMENT = "lakehouse-skip"
MODEL_NAME = "lakehouse_skip"
CHAMPION = "champion"


class ModelNotFoundError(LakehouseError):
    """No hay modelo registrado en MLflow: hay que entrenar antes de predecir."""


@dataclass(frozen=True)
class LoadedModel:
    pipeline: Pipeline
    name: str
    version: str
    run_id: str
    threshold: float
    train_rate: float
    cuts: SplitCuts


def _mlflow(uri: str):
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")  # sin el aviso de "skills" en cada corrida
    import mlflow

    mlflow.set_tracking_uri(uri)
    return mlflow


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def flat_metrics(result: TrainResult) -> dict[str, float]:
    """{'model_test_roc_auc': ..., 'baseline_validation_f1': ...}."""
    return {
        f"{who}_{split}_{name}": float(value)
        for who, by_split in result.metrics.items()
        for split, values in by_split.items()
        for name, value in values.items()
    }


def run_params(result: TrainResult) -> dict[str, str | int | float]:
    """Lo que la inferencia necesita leer de vuelta (umbral, tasa de train, cortes) y la configuracion."""
    return {
        "threshold": result.threshold,
        "baseline_threshold": result.baseline_threshold,
        "train_rate": result.train_rate,
        "train_end_epoch": result.cuts.train_end,
        "validation_end_epoch": result.cuts.validation_end,
        "train_end": _iso(result.cuts.train_end),
        "validation_end": _iso(result.cuts.validation_end),
        "features": ",".join(FEATURES),
        **{f"rows_{k}": v for k, v in result.split_rows.items()},
        **{f"model_{k}": v for k, v in MODEL_PARAMS.items()},
        **{f"encoder_{k}": v for k, v in ENCODER_PARAMS.items()},
    }


def log_training(
    uri: str, result: TrainResult, sample: pd.DataFrame, artifacts: list[Path]
) -> tuple[str, str]:
    """Registra el entrenamiento y el modelo; regresa (run_id, version). sample solo infiere la firma."""
    mlflow = _mlflow(uri)
    from mlflow import MlflowClient
    from mlflow.models import infer_signature

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f"skip-{datetime.now(UTC):%Y%m%d-%H%M%S}") as run:
        mlflow.log_params(run_params(result))
        mlflow.log_metrics(flat_metrics(result))
        mlflow.set_tags(
            {"beats_baseline": str(result.beats_baseline).lower(), "source": "gold.ml_skip_features"}
        )
        mlflow.log_dict({"importance": result.importance}, "feature_importance.json")
        for path in artifacts:
            mlflow.log_artifact(str(path), "reporte")
        signature = infer_signature(sample, result.pipeline.predict_proba(sample)[:, 1])
        info = mlflow.sklearn.log_model(
            result.pipeline,
            name="model",
            registered_model_name=MODEL_NAME,
            signature=signature,
            # Sin input_example: serian filas reales. cloudpickle: skops pediria otra dependencia.
            serialization_format="cloudpickle",
        )
    version = str(info.registered_model_version)
    client = MlflowClient(uri)
    client.set_model_version_tag(MODEL_NAME, version, "beats_baseline", str(result.beats_baseline).lower())
    if result.beats_baseline:
        client.set_registered_model_alias(MODEL_NAME, CHAMPION, version)
    log.info(
        "MLflow: corrida %s, %s v%s%s",
        run.info.run_id[:8],
        MODEL_NAME,
        version,
        f" ({CHAMPION})" if result.beats_baseline else " (no le gano al baseline: sin alias)",
    )
    return run.info.run_id, version


def _inference_version(client):
    from mlflow.exceptions import MlflowException

    try:
        return client.get_model_version_by_alias(MODEL_NAME, CHAMPION)
    except MlflowException:
        versions = client.search_model_versions(f"name = '{MODEL_NAME}'")
        if not versions:
            raise ModelNotFoundError(
                f"No hay versiones de {MODEL_NAME} en MLflow: corre antes `python -m lakehouse.ml train`."
            ) from None
        latest = max(versions, key=lambda v: int(v.version))
        log.warning("Sin alias %s: se usa la version mas reciente (v%s)", CHAMPION, latest.version)
        return latest


def load_for_inference(uri: str) -> LoadedModel:
    mlflow = _mlflow(uri)
    from mlflow import MlflowClient

    client = MlflowClient(uri)
    mv = _inference_version(client)
    params = client.get_run(mv.run_id).data.params
    pipeline = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{mv.version}")
    return LoadedModel(
        pipeline=pipeline,
        name=MODEL_NAME,
        version=str(mv.version),
        run_id=mv.run_id,
        threshold=float(params["threshold"]),
        train_rate=float(params["train_rate"]),
        cuts=SplitCuts(int(params["train_end_epoch"]), int(params["validation_end_epoch"])),
    )
