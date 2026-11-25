# Entregables

Índice de lo que pide el proyecto. Aquí vive lo que **no es código**; el código está en la raíz del repo. Se
completa conforme avanza el [roadmap](../DOCS/ROADMAP.md) y queda final en F8.

| # | Entregable | Aquí | Código relacionado | Estado |
|---|---|---|---|---|
| 1 | Diagrama de arquitectura | [`01_diagrama_arquitectura/`](01_diagrama_arquitectura/DIAGRAMA.md) | | v1 (final en F8) |
| 2 | Scripts de despliegue e infraestructura | [`02_infraestructura/`](02_infraestructura/README.md) | `docker-compose.yml`, `infra/`, `.env.example`, `.github/workflows/ci.yml` | v2 con Dagster, MLflow y CI (crece en F7) |
| 3 | Reporte de calidad y linaje | `03_calidad_y_linaje/` ([CALIDAD_Y_LINAJE.md](03_calidad_y_linaje/CALIDAD_Y_LINAJE.md), [CATALOGO.md](03_calidad_y_linaje/CATALOGO.md), [LINAJE.md](03_calidad_y_linaje/LINAJE.md), [linaje_dagster.png](03_calidad_y_linaje/linaje_dagster.png)) | `src/lakehouse/governance/`, `src/lakehouse/orchestration/`, `infra/minio/policies/` | Listo (F4 y F5; Dagster regenera el reporte en cada corrida) |
| 4 | Modelo de ML | [`04_modelo_ml/`](04_modelo_ml/) (`ML_METRICAS.md`, `metrics.json`, gráficas) | [`src/lakehouse/ml/`](../src/lakehouse/ml/), [`notebooks/ml_skip.ipynb`](../notebooks/ml_skip.ipynb) | Listo (F6): le gana al baseline en prueba |
| 5 | Tablero de BI | `05_tablero_bi/` | `bi/`, `infra/superset/` | Pendiente (F7) |
| 6 | Video de presentación (máx. 10 min) | `06_video/` | | Pendiente (F9) |
