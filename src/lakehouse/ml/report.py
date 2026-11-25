"""Reporte del modelo (entregable 4): ML_METRICAS.md, metrics.json y graficas, solo con numeros agregados.

Lo escribe cada entrenamiento en ML_REPORT_DIR (ENTREGABLES/04_modelo_ml). Acentos como escapes: codigo ASCII.
Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from lakehouse.ml.model import TEST, TRAIN, UNLABELED, VALIDATION, TrainResult

REPORT_FILE = "ML_METRICAS.md"
METRICS_FILE = "metrics.json"
FIGURES = {
    "curves": "roc_pr.png",
    "confusion": "confusion_matrix.png",
    "importance": "feature_importance.png",
}
METRIC_LABELS = (
    ("roc_auc", "ROC-AUC"),
    ("pr_auc", "PR-AUC"),
    ("f1", "F1"),
    ("precision", "Precisi\u00f3n"),
    ("recall", "Recall"),
    ("accuracy", "Exactitud"),
    ("threshold", "Umbral"),
)
TOP_FEATURES = 15
MODEL_COLOR, BASELINE_COLOR = "#1f6feb", "#8b949e"


def _day(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%d")


def _figure_curves(result: TrainResult, path: Path) -> None:
    import matplotlib.pyplot as plt
    from sklearn.metrics import precision_recall_curve, roc_curve

    curves = result.test_curves
    fig, (roc, pr) = plt.subplots(1, 2, figsize=(11, 4.5))
    for key, label, color in (("model", "Modelo", MODEL_COLOR), ("baseline", "Baseline", BASELINE_COLOR)):
        test = result.metrics[key][TEST]
        fpr, tpr, _ = roc_curve(curves["y"], curves[key])
        roc.plot(fpr, tpr, color=color, label=f"{label} (AUC {test['roc_auc']:.3f})")
        precision, recall, _ = precision_recall_curve(curves["y"], curves[key])
        pr.plot(recall, precision, color=color, label=f"{label} (AP {test['pr_auc']:.3f})")
    roc.plot([0, 1], [0, 1], linestyle=":", color="#d0d7de")
    pr.axhline(result.metrics["model"][TEST]["skip_rate"], linestyle=":", color="#d0d7de")
    roc.set(title="Curva ROC (prueba)", xlabel="Falsos positivos", ylabel="Verdaderos positivos")
    pr.set(title="Precisi\u00f3n y recall (prueba)", xlabel="Recall", ylabel="Precisi\u00f3n")
    for ax in (roc, pr):
        ax.legend(loc="lower right" if ax is roc else "upper right")
        ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _figure_confusion(result: TrainResult, path: Path) -> None:
    import matplotlib.pyplot as plt

    m = result.metrics["model"][TEST]
    cells = [[m["tn"], m["fp"]], [m["fn"], m["tp"]]]
    fig, ax = plt.subplots(figsize=(4.8, 4.2))
    ax.imshow(cells, cmap="Blues")
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{cells[i][j]:,}", ha="center", va="center", fontsize=13)
    ax.set_xticks([0, 1], ["No skip", "Skip"])
    ax.set_yticks([0, 1], ["No skip", "Skip"])
    ax.set(
        title=f"Matriz de confusi\u00f3n (prueba, umbral {m['threshold']:.2f})",
        xlabel="Predicho",
        ylabel="Real",
    )
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def _figure_importance(result: TrainResult, path: Path) -> None:
    import matplotlib.pyplot as plt

    top = result.importance[:TOP_FEATURES][::-1]
    fig, ax = plt.subplots(figsize=(7.5, 0.35 * len(top) + 1.2))
    ax.barh(
        [r["feature"] for r in top],
        [r["importance"] for r in top],
        xerr=[r["std"] for r in top],
        color=MODEL_COLOR,
    )
    ax.set(
        title="Importancia por permutaci\u00f3n (prueba)",
        xlabel="Ca\u00edda de ROC-AUC al revolver la feature",
    )
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def write_figures(result: TrainResult, out_dir: Path) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")  # sin pantalla: contenedor y CI
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {k: out_dir / name for k, name in FIGURES.items()}
    _figure_curves(result, paths["curves"])
    _figure_confusion(result, paths["confusion"])
    _figure_importance(result, paths["importance"])
    return list(paths.values())


def metrics_document(result: TrainResult, run_id: str, version: str, generated_at: datetime) -> dict:
    """metrics.json: lo mismo que el .md, para el notebook y el tablero."""
    return {
        "generated_at": generated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mlflow": {"run_id": run_id, "model_version": version},
        "beats_baseline": result.beats_baseline,
        "train_end": _day(result.cuts.train_end),
        "validation_end": _day(result.cuts.validation_end),
        "train_skip_rate": result.train_rate,
        "split_rows": result.split_rows,
        "metrics": result.metrics,
        "importance": result.importance,
    }


def _f(value: float) -> str:
    return f"{value:.3f}"


def render_report(result: TrainResult, run_id: str, version: str, generated_at: datetime) -> str:
    model, base, rows = result.metrics["model"], result.metrics["baseline"], result.split_rows
    t_model, t_base = model[TEST], base[TEST]
    verdict = "le gana" if result.beats_baseline else "NO le gana"
    train_end, val_end = _day(result.cuts.train_end), _day(result.cuts.validation_end)
    lines = [
        "# M\u00e9tricas del modelo de skip",
        "",
        f"> Generado por `python -m lakehouse.ml train` (paso `ml_train` de Dagster) el "
        f"{generated_at:%Y-%m-%d %H:%M} UTC. No se edita a mano. Solo n\u00fameros agregados: ninguna fila, "
        "canci\u00f3n ni artista.",
        "",
        "## Veredicto",
        "",
        f"**El modelo {verdict} al baseline en el periodo de prueba:** "
        f"ROC-AUC {_f(t_model['roc_auc'])} contra {_f(t_base['roc_auc'])} y "
        f"PR-AUC {_f(t_model['pr_auc'])} contra {_f(t_base['pr_auc'])}.",
        "",
        f"- MLflow: modelo `lakehouse_skip` versi\u00f3n {version}, corrida `{run_id[:8]}`"
        + (
            " (alias `champion`: la usa la inferencia)."
            if result.beats_baseline
            else " (sin alias `champion`)."
        ),
        "- Modelo: `HistGradientBoostingClassifier` (scikit-learn) con one-hot de plataforma, motivo de "
        "inicio y g\u00e9nero. El umbral maximiza F1 en validaci\u00f3n.",
        "- Baseline: tasa de skip hist\u00f3rica de la canci\u00f3n. Si la canci\u00f3n es nueva, usa la "
        "tasa reciente del usuario y, si tampoco hay, la de entrenamiento.",
        "",
        "## Datos y split temporal",
        "",
        "Solo entrenan las reproducciones con etiqueta. El corte es por fecha, nunca al azar: el modelo no "
        "ve el futuro.",
        "",
        "| Partici\u00f3n | Periodo (UTC) | Filas | Tasa de skip |",
        "|---|---|---|---|",
        f"| Entrenamiento | antes del {train_end} | {rows[TRAIN]:,} | {_f(result.train_rate)} |",
        f"| Validaci\u00f3n | {train_end} al {val_end} | {rows[VALIDATION]:,} | "
        f"{_f(model[VALIDATION]['skip_rate'])} |",
        f"| Prueba | desde el {val_end} | {rows[TEST]:,} | {_f(t_model['skip_rate'])} |",
        f"| Sin etiqueta | (no entrena; s\u00ed recibe predicci\u00f3n) | {rows[UNLABELED]:,} | |",
        "",
        "## Modelo contra baseline",
        "",
        "| M\u00e9trica | Modelo (validaci\u00f3n) | Baseline (validaci\u00f3n) | Modelo (prueba) "
        "| Baseline (prueba) |",
        "|---|---|---|---|---|",
    ]
    for key, label in METRIC_LABELS:
        values = (model[VALIDATION][key], base[VALIDATION][key], t_model[key], t_base[key])
        lines.append(f"| {label} | " + " | ".join(_f(v) for v in values) + " |")
    lines += [
        "",
        f"## Matriz de confusi\u00f3n (prueba, modelo, umbral {_f(result.threshold)})",
        "",
        "| | Predicho no skip | Predicho skip |",
        "|---|---|---|",
        f"| **Real no skip** | {t_model['tn']:,} | {t_model['fp']:,} |",
        f"| **Real skip** | {t_model['fn']:,} | {t_model['tp']:,} |",
        "",
        "## Importancia de features (permutaci\u00f3n, prueba)",
        "",
        "Cu\u00e1nto baja el ROC-AUC de prueba al revolver cada feature. Cerca de 0 = el modelo casi no "
        "la usa.",
        "",
        "| Feature | Ca\u00edda de ROC-AUC | Desviaci\u00f3n |",
        "|---|---|---|",
    ]
    lines += [f"| `{r['feature']}` | {r['importance']:.4f} | {r['std']:.4f} |" for r in result.importance]
    lines += [
        "",
        "## Gr\u00e1ficas",
        "",
        f"![Curvas ROC y PR]({FIGURES['curves']})",
        "",
        f"![Matriz de confusi\u00f3n]({FIGURES['confusion']})",
        "",
        f"![Importancia de features]({FIGURES['importance']})",
        "",
        "## C\u00f3mo se regenera",
        "",
        "Con el pipeline de Dagster (pasos `ml_features`, `ml_train` y `ml_predict`) o a mano dentro del "
        "contenedor: `python -m lakehouse.ml all`.",
    ]
    return "\n".join(lines) + "\n"


def write_report(
    result: TrainResult, out_dir: Path, run_id: str, version: str, generated_at: datetime | None = None
) -> Path:
    """ML_METRICAS.md y metrics.json en out_dir (las graficas ya deben estar ahi)."""
    when = generated_at or datetime.now(UTC)
    out_dir.mkdir(parents=True, exist_ok=True)
    doc = json.dumps(metrics_document(result, run_id, version, when), ensure_ascii=True, indent=1)
    (out_dir / METRICS_FILE).write_text(doc + "\n", encoding="utf-8", newline="\n")
    path = out_dir / REPORT_FILE
    path.write_text(render_report(result, run_id, version, when), encoding="utf-8", newline="\n")
    return path
