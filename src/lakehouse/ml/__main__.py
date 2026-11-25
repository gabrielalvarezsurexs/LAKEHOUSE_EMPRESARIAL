"""CLI de ML. Dentro del contenedor: python -m lakehouse.ml <comando> (ver --help).

Comandos: features (Oro a gold.ml_skip_features), train (entrena, mide contra el baseline, registra en
MLflow y escribe ML_METRICAS.md), predict (inferencia por lote a gold.ml_skip_predictions) y all.
Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from lakehouse.common.config import get_settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

    from lakehouse.common.delta import TableLocator

log = get_logger("lakehouse.ml")

DEFAULT_REPORT_DIR = "ENTREGABLES/04_modelo_ml"
STEPS = {
    "features": ["features"],
    "train": ["train"],
    "predict": ["predict"],
    "all": ["features", "train", "predict"],
}


def load_features(spark: SparkSession, locator: TableLocator) -> pd.DataFrame:
    """gold.ml_skip_features a pandas (~15k filas); el tiempo va en epoch para no depender de zonas."""
    import pandas as pd
    from pyspark.sql import functions as F

    from lakehouse.common.delta import read_table
    from lakehouse.governance.contracts import GOLD_ML_SKIP_FEATURES

    df = read_table(spark, locator, GOLD_ML_SKIP_FEATURES)
    columns = [c for c in df.columns if c != "ts_start"]
    rows = df.select(*columns, F.col("ts_start").cast("long").alias("ts_epoch")).collect()
    return pd.DataFrame([r.asDict() for r in rows], columns=[*columns, "ts_epoch"])


def train_step(spark: SparkSession, locator: TableLocator, report_dir: Path, mlflow_uri: str) -> None:
    from lakehouse.ml.model import FEATURES, prepare, train
    from lakehouse.ml.registry import log_training
    from lakehouse.ml.report import write_figures, write_report

    features = load_features(spark, locator)
    result = train(features)
    figures = write_figures(result, report_dir)
    sample = prepare(features.head(50))
    run_id, version = log_training(mlflow_uri, result, sample[list(FEATURES)], figures)
    path = write_report(result, report_dir, run_id, version)
    test, base = result.metrics["model"]["test"], result.metrics["baseline"]["test"]
    log.info(
        "Modelo: prueba ROC-AUC %.3f (baseline %.3f), PR-AUC %.3f (baseline %.3f), F1 %.3f; %s",
        test["roc_auc"],
        base["roc_auc"],
        test["pr_auc"],
        base["pr_auc"],
        test["f1"],
        "le gana al baseline" if result.beats_baseline else "NO le gana al baseline",
    )
    log.info("Reporte: %s", path)


def predict_step(spark: SparkSession, locator: TableLocator, mlflow_uri: str) -> int:
    from pyspark.sql import functions as F

    from lakehouse.common.delta import overwrite, read_table
    from lakehouse.governance.contracts import GOLD_ML_SKIP_FEATURES, GOLD_ML_SKIP_PREDICTIONS
    from lakehouse.ml.model import baseline_scores, score
    from lakehouse.ml.registry import load_for_inference

    loaded = load_for_inference(mlflow_uri)
    features = load_features(spark, locator)
    probability = score(loaded.pipeline, features)
    baseline = baseline_scores(features, loaded.train_rate)
    split = loaded.cuts.assign(features["ts_epoch"], features["is_skip"].notna())
    # Tipos de Python (no numpy) para que Spark arme el DataFrame sin sorpresas.
    rows = [
        (pid, s, float(p), float(loaded.threshold), bool(p >= loaded.threshold), float(b))
        for pid, s, p, b in zip(features["play_id"], split, probability, baseline, strict=True)
    ]
    schema = "play_id STRING, split STRING, skip_probability DOUBLE, threshold DOUBLE, "
    schema += "predicted_skip BOOLEAN, baseline_probability DOUBLE"
    scored = spark.createDataFrame(rows, schema)
    source = read_table(spark, locator, GOLD_ML_SKIP_FEATURES).select(
        "play_id", "ts_start", "date_key", "is_skip"
    )
    predictions = (
        source.join(scored, "play_id")
        .withColumn("model_name", F.lit(loaded.name))
        .withColumn("model_version", F.lit(loaded.version))
        .withColumn("mlflow_run_id", F.lit(loaded.run_id))
        .withColumn("predicted_at", F.current_timestamp())
    )
    overwrite(spark, predictions, locator, GOLD_ML_SKIP_PREDICTIONS)
    written = read_table(spark, locator, GOLD_ML_SKIP_PREDICTIONS)
    total, skips = written.count(), written.where("predicted_skip").count()
    if total != len(features):
        raise LakehouseError(f"ml_skip_predictions tiene {total} filas y ml_skip_features {len(features)}")
    log.info(
        "Predicciones: %d filas con %s v%s (%d marcadas como skip)", total, loaded.name, loaded.version, skips
    )
    return total


def run(steps: list[str], report_dir: Path) -> None:
    from lakehouse.common.delta import TableLocator
    from lakehouse.common.spark import build_spark
    from lakehouse.ml.features import build_features

    # svc-ml: lee Oro y escribe solo gold/ml_* (D17).
    settings = get_settings().for_role("ml")
    mlflow_uri = settings.require_mlflow() if {"train", "predict"} & set(steps) else ""
    locator = TableLocator.from_settings(settings)
    spark = build_spark("ml", settings)
    try:
        if "features" in steps:
            build_features(spark, locator)
        if "train" in steps:
            train_step(spark, locator, report_dir, mlflow_uri)
        if "predict" in steps:
            predict_step(spark, locator, mlflow_uri)
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m lakehouse.ml", description=__doc__.split("\n")[0])
    parser.add_argument(
        "--report-dir",
        default=os.environ.get("ML_REPORT_DIR") or DEFAULT_REPORT_DIR,
        help="Carpeta de ML_METRICAS.md y las graficas (default: ML_REPORT_DIR)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("features", help="Oro a gold.ml_skip_features (sin fuga)")
    sub.add_parser("train", help="Split temporal, modelo vs baseline, MLflow y ML_METRICAS.md")
    sub.add_parser("predict", help="Inferencia por lote a gold.ml_skip_predictions")
    sub.add_parser("all", help="features + train + predict")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run(STEPS[args.command], Path(args.report_dir))
    except LakehouseError as exc:
        log.error("ML FALLO: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
