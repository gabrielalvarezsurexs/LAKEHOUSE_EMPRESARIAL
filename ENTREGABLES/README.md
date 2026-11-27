# Entregables

Índice de lo que pide el proyecto ([instrucciones](../DOCS/INSTRUCCIONES_PROYECTO.pdf)), contra la rúbrica. Aquí
vive lo que **no es código**; el código está en la raíz del repo y cada fila apunta a él. Cómo replicar todo:
[README raíz](../README.md).

| # | Entregable | Aquí | Código relacionado | Estado |
|---|---|---|---|---|
| 1 | Diagrama de arquitectura | [`01_diagrama_arquitectura/`](01_diagrama_arquitectura/DIAGRAMA.md) (Mermaid e imagen) | | Final (F8) |
| 2 | Scripts de despliegue e infraestructura | [`02_infraestructura/`](02_infraestructura/README.md) | `docker-compose.yml`, `infra/`, `.env.example`, `.github/workflows/ci.yml`, SQL en `bi/sql/` | Final (F8): 5 servicios, CI y prueba de réplica |
| 3 | Reporte de calidad y linaje | `03_calidad_y_linaje/` ([CALIDAD_Y_LINAJE.md](03_calidad_y_linaje/CALIDAD_Y_LINAJE.md), [CATALOGO.md](03_calidad_y_linaje/CATALOGO.md), [LINAJE.md](03_calidad_y_linaje/LINAJE.md), [linaje_dagster.png](03_calidad_y_linaje/linaje_dagster.png)) | `src/lakehouse/governance/`, `src/lakehouse/orchestration/`, `infra/minio/policies/` | Listo; Dagster regenera el reporte en cada corrida |
| 4 | Modelo de ML | [`04_modelo_ml/`](04_modelo_ml/) (`ML_METRICAS.md`, `metrics.json`, gráficas) | [`src/lakehouse/ml/`](../src/lakehouse/ml/), [`notebooks/ml_skip.ipynb`](../notebooks/ml_skip.ipynb) | Listo: le gana al baseline en prueba |
| 5 | Tablero de BI | [`05_tablero_bi/`](05_tablero_bi/README.md) (cómo abrirlo, export y capturas) | [`bi/`](../bi/), [`infra/superset/`](../infra/superset/) | Código listo (F7); faltan las capturas aprobadas |
| 6 | Video de presentación (máx. 10 min) | [`06_video/`](06_video/README.md) | Guion: [`MDS_DE_ESTUDIO/00_GUION_VIDEO.md`](../MDS_DE_ESTUDIO/00_GUION_VIDEO.md) | Pendiente: grabarlo (F9) |

## Rúbrica (10 pts): dónde está la evidencia

| Criterio | Pts | Evidencia |
|---|---|---|
| Arquitectura y Lakehouse | 2 | Diagrama (1); buckets por capa y tablas Delta; contratos de esquema en `src/lakehouse/governance/contracts.py`; decisiones en [`DOCS/ARQUITECTURA.md`](../DOCS/ARQUITECTURA.md) |
| Pipelines y orquestación | 2 | `src/lakehouse/` por rol; cuarentena, reintentos e idempotencia; Dagster con sensores, schedules y checks que bloquean (`src/lakehouse/orchestration/`) |
| Gobernanza y calidad | 1.5 | Reporte, catálogo y linaje (3); usuarios de MinIO con mínimo privilegio probados en vivo; IP seudonimizada |
| Machine Learning | 1.5 | Métricas contra baseline (4); MLflow con alias `champion`; predicciones de vuelta a Oro |
| BI y analítica | 1.5 | Tablero de Superset sobre Oro (5), con filtros y tiempos por gráfica (`bi/check.py`) |
| Documentación y DevOps | 1 | [README](../README.md) paso a paso; `infra/replica.ps1` desde un clon limpio; CI en verde en cada push |
| Presentación | 0.5 | Video (6) con los 7 integrantes; guías en [`MDS_DE_ESTUDIO/`](../MDS_DE_ESTUDIO/) |
