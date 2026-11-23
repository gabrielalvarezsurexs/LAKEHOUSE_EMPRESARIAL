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

Rutas relativas a `ENTREGABLES/` salvo que se diga otra cosa. Se confirman al implementar cada fase.

| Integrante | Carpetas y archivos | Entregable del PDF |
|---|---|---|
| Gabriel (Arquitecto) | `docker-compose.yml`, `.env.example`, `infra/app/`, `infra/minio/` (buckets), `src/lakehouse/common/`, `diagramas/`, `README.md` (raíz y de `ENTREGABLES/`), `.github/CODEOWNERS` | 1 Diagrama, 2 Infraestructura |
| Ricardo (Ingesta) | `src/lakehouse/ingestion/` (landing, Bronce, MusicBrainz, generador sintético) | Pipelines de ingesta |
| Juan Pablo (Transformación) | `src/lakehouse/transformation/` (Plata y Oro, SQL de Spark) | Pipelines de transformación |
| Victor (Gobernanza) | `src/lakehouse/governance/` (contratos, calidad, catálogo, linaje, seudonimización), `infra/minio/policies/`, `reportes/CALIDAD_Y_LINAJE.md`, `reportes/CATALOGO.md` | 3 Reporte de calidad y linaje |
| Emilio (BI) | `bi/` (Superset, vistas SQL de Oro, tablero exportado, capturas) | 5 Tablero de BI |
| Fernando (ML) | `src/lakehouse/ml/`, `notebooks/`, `reportes/ML_METRICAS.md` | 4 Código del modelo |
| Nora (DevOps/MLOps) | `orchestration/` (Dagster: jobs, schedules, sensores, checks), `monitoring/`, `.github/workflows/` (raíz) | Orquestación, CI/CD, monitoreo |
