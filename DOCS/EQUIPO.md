# Equipo

Proyecto Final de Datos Masivos (UP): Lakehouse Empresarial. Un rol principal por integrante; cada quien es
responsable directo de los entregables de su rol. La implementación la hacen Gabriel y Claude; cada integrante
estudia y presenta su parte con su guía de `MDS_DE_ESTUDIO/`. Todos los commits los hace Gabriel.

| # | Rol | Integrante | ID UP |
|---|---|---|---|
| 1 | Arquitecto de Datos / Líder | Gabriel Álvarez Mendoza | 0261445 |
| 2 | Ingeniero de Datos (Ingesta) | Ricardo Zerón del Valle | 0261606 |
| 3 | Ingeniero de Datos (Transformación) | Juan Pablo Ramirez Carlón | 0262576 |
| 4 | Oficial de Gobernanza y Seguridad | Victor Gabriel Torres Guerrero | 0265786 |
| 5 | Analista de Datos / Desarrollador BI | Emilio Rendón Trias | 0267019 |
| 6 | Ingeniero de Machine Learning | Fernando Peña Meade | 0261604 |
| 7 | Ingeniero DevOps / MLOps | Nora Valentina Frías Alpízar | 0268966 |

## Qué le toca a cada quien en el repo

Código en la raíz; lo que no es código va en `ENTREGABLES/`. Se confirma al implementar cada fase.

| Integrante | Código (raíz) | En `ENTREGABLES/` | Entregable del PDF |
|---|---|---|---|
| Gabriel (Arquitecto) | `docker-compose.yml`, `.env.example`, `pyproject.toml`, `infra/app/`, `infra/minio/` (buckets), `src/lakehouse/common/`, `README.md`, `.github/CODEOWNERS` | `01_diagrama_arquitectura/`, `02_infraestructura/`, `README.md` | 1 Diagrama, 2 Infraestructura |
| Ricardo (Ingesta) | `src/lakehouse/ingestion/` (landing, Bronce, MusicBrainz, generador sintético) | | Pipelines de ingesta |
| Juan Pablo (Transformación) | `src/lakehouse/transformation/` (Plata y Oro, SQL de Spark) | | Pipelines de transformación |
| Victor (Gobernanza) | `src/lakehouse/governance/` (contratos, calidad, catálogo, linaje, seudonimización), `infra/minio/policies/` | `03_calidad_y_linaje/` (reporte, catálogo, linaje) | 3 Reporte de calidad y linaje |
| Emilio (BI) | `bi/` (Superset, vistas SQL sobre Oro, export del tablero), `infra/superset/` | `05_tablero_bi/` (capturas, export, cómo abrirlo) | 5 Tablero de BI |
| Fernando (ML) | `src/lakehouse/ml/`, `notebooks/` | `04_modelo_ml/` (métricas y gráficas) | 4 Código del modelo |
| Nora (DevOps/MLOps) | `src/lakehouse/orchestration/` (Dagster: jobs, schedules, sensores, checks, monitoreo), `.github/workflows/`, MLflow en el compose | | Orquestación, CI/CD, monitoreo |
| Todos | | `06_video/` (link) | Video |
