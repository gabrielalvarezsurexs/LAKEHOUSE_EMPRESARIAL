# Modelo de ML: predicción de skip

Entregable 4. Con lo que se sabe al empezar una canción, el modelo predice si el usuario la va a saltar (D11).
Lee solo de Oro y escribe sus predicciones de vuelta en Oro.

| Archivo | Qué es |
|---|---|
| [`ML_METRICAS.md`](ML_METRICAS.md) | Veredicto, split temporal, métricas del modelo contra el baseline, matriz de confusión e importancia de features. Lo genera cada entrenamiento |
| [`metrics.json`](metrics.json) | Los mismos números, para el notebook y el tablero |
| `roc_pr.png`, `confusion_matrix.png`, `feature_importance.png` | Gráficas del periodo de prueba |

Los archivos generados no se editan a mano y solo traen números agregados: ninguna fila, canción ni artista.

## Dónde está el código

| Pieza | Archivo |
|---|---|
| Features sin fuga (Spark) | [`src/lakehouse/ml/features.py`](../../src/lakehouse/ml/features.py) |
| Split temporal, baseline, modelo y métricas | [`src/lakehouse/ml/model.py`](../../src/lakehouse/ml/model.py) |
| Registro en MLflow y carga del modelo para la inferencia | [`src/lakehouse/ml/registry.py`](../../src/lakehouse/ml/registry.py) |
| Reporte y gráficas | [`src/lakehouse/ml/report.py`](../../src/lakehouse/ml/report.py) |
| CLI (`features`, `train`, `predict`, `all`) | [`src/lakehouse/ml/__main__.py`](../../src/lakehouse/ml/__main__.py) |
| Pasos de Dagster `ml_features`, `ml_train` y `ml_predict` | [`src/lakehouse/orchestration/steps.py`](../../src/lakehouse/orchestration/steps.py) |
| Contratos de `gold.ml_skip_features` y `gold.ml_skip_predictions` | [`src/lakehouse/governance/contracts.py`](../../src/lakehouse/governance/contracts.py) |
| Notebook de presentación | [`notebooks/`](../../notebooks/) |

## Cómo se ve

- **MLflow** (<http://localhost:5000>): experimento `lakehouse-skip`, con una corrida por entrenamiento que guarda
  parámetros, métricas y gráficas. El modelo registrado es `lakehouse_skip`. La versión que le gana al baseline
  recibe el alias `champion`.
- **Dagster** (<http://localhost:3300>): grupo `ml` en el grafo, con los checks de contrato de las dos tablas.
