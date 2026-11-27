# Fernando Peña Meade: Ingeniero de ML

## Tu parte en 30 segundos

Tú respondes una pregunta: **cuando empieza una canción, ¿la va a saltar?** (skip). Es clasificación binaria.

- **Lees solo de Oro:** armas `gold.ml_skip_features`, con una fila por reproducción (15,628).
- **Entrenas y comparas:** un modelo de gradient boosting contra un baseline simple, con un corte por fechas.
- **Registras en MLflow:** cada entrenamiento deja métricas, gráficas y una versión del modelo `lakehouse_skip`.
- **Escribes de vuelta en Oro:** las predicciones van a `gold.ml_skip_predictions`, y el tablero de BI las muestra contra la realidad.

Todo corre solo dentro del pipeline de Dagster (pasos `ml_features`, `ml_train` y `ml_predict`).

## Resultado (lo que tienes que poder decir de memoria)

**El modelo le gana al baseline en el periodo de prueba**, que nunca vio al entrenar:

| Prueba | Modelo | Baseline |
|---|---|---|
| ROC-AUC | **0.746** | 0.619 |
| PR-AUC | **0.511** | 0.289 |
| F1 | **0.512** | 0.396 |

- **Qué pesa más:** cómo empezó la canción (`reason_start`: tú la elegiste, vino sola, le diste "siguiente"...).
  Le sigue la tasa de skip histórica de esa canción.
- **Tasa de skip en prueba:** 21.5%. Por eso la PR-AUC del modelo (0.51) hay que leerla contra 0.215, que es lo
  que sacaría un modelo al azar.
- Los números completos y las gráficas están en
  [ML_METRICAS.md](../ENTREGABLES/04_modelo_ml/ML_METRICAS.md).

## Lo que tienes que saber (máx. 5 conceptos)

- **La etiqueta (`is_skip`):** sale de Plata, del motivo con que terminó la canción (`reason_end`).
  - Si le diste "siguiente" o "atrás", es skip. Si terminó sola, no lo es.
  - En los casos dudosos cuenta si sonó menos de 30 segundos.
  - Si no se puede saber, queda `null`: esas 1,454 reproducciones no entrenan, pero sí reciben predicción.
- **Sin fuga de información (leakage):** cada feature usa solo lo que se sabía *antes* de que empezara la canción.
  - Contexto: hora, día, plataforma, shuffle, cómo empezó, género del artista.
  - Sesión: lugar en la sesión, minutos que lleva la sesión y si saltaste la canción anterior.
  - Historia: cuántas veces sonó antes esa canción o ese artista y qué tanto los saltabas; tu tasa de skip en
    las últimas 100.
  - **Nunca** la duración ni el motivo de fin de esa misma reproducción: eso *es* la respuesta.
  - En Spark: ventanas que terminan en la fila anterior (`rowsBetween(unboundedPreceding, -1)`).
- **Split temporal, no al azar:**
  - 70% más viejo para entrenar (hasta mar 2020), 15% para validar (hasta ene 2022) y 15% más reciente para
    probar.
  - Así se simula usar el modelo en el futuro. Al azar, el modelo vería el futuro y saldría inflado.
- **Baseline:** la tasa de skip histórica de la canción. Si la canción es nueva, tu tasa reciente; si tampoco hay,
  la de entrenamiento. Un modelo solo vale si le gana a la regla obvia.
- **MLflow y el alias `champion`:**
  - Cada entrenamiento es una corrida con parámetros, métricas y gráficas.
  - El modelo queda registrado con número de versión.
  - Solo la versión que le gana al baseline recibe el alias `champion`, y la inferencia usa esa.

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| [features.py](../src/lakehouse/ml/features.py) | Features con ventanas de Spark | `before = (unboundedPreceding, -1)`: solo el pasado |
| [model.py](../src/lakehouse/ml/model.py) | Split, baseline, modelo y métricas | `temporal_cuts`, `baseline_scores`, `best_threshold` |
| [registry.py](../src/lakehouse/ml/registry.py) | MLflow: registro y alias | `log_training` y `load_for_inference` |
| [report.py](../src/lakehouse/ml/report.py) | `ML_METRICAS.md`, `metrics.json` y gráficas | Solo agregados, nunca filas |
| [ml_skip.ipynb](../notebooks/ml_skip.ipynb) | Notebook de presentación | Lee `metrics.json`; no entrena |
| [contracts.py](../src/lakehouse/governance/contracts.py) | Contratos de las 2 tablas de ML | `GOLD_ML_SKIP_FEATURES` y sus reglas |

## Cómo verlo funcionando

El entorno lo levanta Gabriel. En la presentación se muestra:

- **MLflow** (<http://localhost:5000>):
  - arriba a la izquierda, cambia a **Model training**;
  - **Runs**: cada entrenamiento; en **Artifacts**, las tres gráficas;
  - **Models → `lakehouse_skip`**: las versiones y cuál es `champion`.
- **Dagster** (<http://localhost:3300>): el grupo `ml` en el grafo, con los checks de contrato de las dos tablas
  en verde.
- **El notebook** [ml_skip.ipynb](../notebooks/ml_skip.ipynb): tabla de modelo contra baseline, split, matriz de
  confusión, importancia y gráficas.

## Tu minuto en el video (guion)

> "Mi parte es el modelo: cuando empieza una canción, ¿la vas a saltar? Leo solo de Oro y armo una fila por
> reproducción, con lo que se sabía *antes* de que empezara: la hora, la plataforma, cómo empezó, cuántas veces la
> habías saltado antes y si saltaste la anterior. Nunca uso cuánto duró, porque eso es la respuesta. *(pantalla:
> notebook)*. Corto por fechas: entreno con lo más viejo y pruebo con lo más reciente, como si el modelo se usara
> en el futuro. Y lo comparo contra una regla simple: la tasa de skip histórica de la canción. En el periodo de
> prueba el modelo le gana: ROC-AUC de 0.75 contra 0.62, y casi el doble de PR-AUC. Lo que más pesa es cómo empezó
> la canción. *(pantalla: MLflow con la corrida y el alias champion)*. Cada entrenamiento queda en MLflow, y la
> versión que le gana al baseline es la que usa la inferencia, que escribe las predicciones de vuelta en Oro para
> el tablero."

## Preguntas que te pueden hacer

- **¿Por qué gradient boosting y no una red neuronal?** Son ~14 mil filas tabulares. Gradient boosting
  (`HistGradientBoostingClassifier`) es lo más fuerte para tablas chicas, maneja nulos solo y entrena en
  segundos.
- **¿Por qué scikit-learn y no Spark MLlib?** 15 mil filas caben en memoria. Spark sí se usa para armar las
  features sobre Delta (D12).
- **¿Por qué ROC-AUC y PR-AUC y no exactitud?** Solo ~20% son skips. Un modelo que diga siempre "no skip" tiene
  ~80% de exactitud y no sirve. PR-AUC se fija justo en la clase rara.
- **¿Cómo eligieron el umbral?** El que maximiza F1 en validación (0.283). Se aplica tal cual en prueba, sin
  volver a ajustarlo.
- **¿Cómo saben que no hay fuga?** Hay una prueba con datos hechos a mano que revisa que cada feature solo cuente
  reproducciones anteriores. Además, el split es por fecha.
- **¿Qué pasa si un día el modelo nuevo es peor?** No recibe `champion` y la inferencia sigue con la versión
  anterior.
- **¿Hay datos personales en MLflow o en el repo?** No. Solo métricas agregadas y el modelo; no se guardan
  ejemplos de entrada.
