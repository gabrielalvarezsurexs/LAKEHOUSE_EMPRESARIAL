# Entregables

Índice de lo que pide el proyecto. Aquí vive lo que **no es código**; el código está en la raíz del repo. Se
completa conforme avanza el [roadmap](../DOCS/ROADMAP.md) y queda final en F8.

| # | Entregable | Aquí | Código relacionado | Estado |
|---|---|---|---|---|
| 1 | Diagrama de arquitectura | [`01_diagrama_arquitectura/`](01_diagrama_arquitectura/DIAGRAMA.md) | | v1 (final en F8) |
| 2 | Scripts de despliegue e infraestructura | [`02_infraestructura/`](02_infraestructura/README.md) | `docker-compose.yml`, `infra/`, `.env.example` | v1 (crece en F5 y F7) |
| 3 | Reporte de calidad y linaje | `03_calidad_y_linaje/` ([CALIDAD_Y_LINAJE.md](03_calidad_y_linaje/CALIDAD_Y_LINAJE.md), [CATALOGO.md](03_calidad_y_linaje/CATALOGO.md), [LINAJE.md](03_calidad_y_linaje/LINAJE.md)) | `src/lakehouse/governance/`, `infra/minio/policies/` | Listo (F4); falta la captura del linaje en Dagster (F5) |
| 4 | Modelo de ML | `04_modelo_ml/` | `src/lakehouse/ml/`, `notebooks/` | Pendiente (F6) |
| 5 | Tablero de BI | `05_tablero_bi/` | `bi/`, `infra/superset/` | Pendiente (F7) |
| 6 | Video de presentación (máx. 10 min) | `06_video/` | | Pendiente (F9) |
