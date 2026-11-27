# Scripts de despliegue e infraestructura

Entregable 2. Toda la infraestructura es código en la raíz del repo; aquí está qué es cada pieza y dónde vive.

| Pieza | Archivo | Qué hace |
|---|---|---|
| Stack completo | [`docker-compose.yml`](../../docker-compose.yml) | Levanta MinIO (buckets y usuarios), el lakehouse con Dagster, MLflow y Superset |
| Variables | [`.env.example`](../../.env.example) | Plantilla del `.env` (credenciales y ajustes); el `.env` real no se versiona |
| Imagen del lakehouse | [`infra/app/Dockerfile`](../../infra/app/Dockerfile) | Python 3.11 + Java 17 + PySpark y Delta instalados con uv desde `uv.lock` |
| JARs de Spark | [`infra/app/fetch_jars.sh`](../../infra/app/fetch_jars.sh) | Baja Delta y S3A en las versiones exactas del lock y verifica su SHA-1 |
| Buckets | [`infra/minio/init.sh`](../../infra/minio/init.sh) | Espera a MinIO, crea `landing`, `bronze`, `silver`, `gold` y los usuarios `svc-*` con su política |
| Políticas de MinIO | [`infra/minio/policies/`](../../infra/minio/policies/) | Una por servicio, generadas desde `src/lakehouse/governance/access.py` (D17) |
| Dependencias | [`pyproject.toml`](../../pyproject.toml), `uv.lock`, `.python-version` | Mismas versiones en local, contenedor y CI |
| Sesión de Spark | [`src/lakehouse/common/spark.py`](../../src/lakehouse/common/spark.py) | Delta + S3A apuntando a MinIO |
| Prueba de humo | [`src/lakehouse/common/smoke.py`](../../src/lakehouse/common/smoke.py) | Verifica buckets y escritura/lectura Delta con time travel |
| Orquestación | [`src/lakehouse/orchestration/`](../../src/lakehouse/orchestration/) | Dagster: un asset por tabla, checks por paso, sensores, schedules y salud (D24) |
| Configuración de Dagster | [`infra/dagster/dagster.yaml`](../../infra/dagster/dagster.yaml) | Historial local en un volumen y una corrida a la vez |
| CI | [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) | En cada push: lint y pruebas; luego MinIO + pipeline completo por Dagster con sintéticos y MLflow, y el tablero de Superset con cada gráfica probada |
| Imagen de Superset | [`infra/superset/Dockerfile`](../../infra/superset/Dockerfile) | Superset 6.1 (digest fijado) + DuckDB con las extensiones `delta` y `httpfs` |
| Configuración de Superset | [`infra/superset/superset_config.py`](../../infra/superset/superset_config.py), [`start.sh`](../../infra/superset/start.sh) | Llave y admin del `.env`; DuckDB entra a `gold` con `svc-bi`; al arrancar importa el tablero |
| SQL y tablero | [`bi/`](../../bi/) | SQL de los datasets sobre Oro, tablero como código (D26) y su prueba en vivo |

## Servicios

| Servicio | Imagen | Puertos |
|---|---|---|
| `minio` | `cgr.dev/chainguard/minio` (fijada por digest) | 9000 API S3, 9001 consola |
| `minio-init` | `cgr.dev/chainguard/minio-client` (fijada por digest) | corre una vez y termina |
| `lakehouse` | propia (`infra/app/Dockerfile`), corre `dagster dev` | 3300 Dagster (`DAGSTER_PORT`), 4040 Spark UI |
| `mlflow` | `ghcr.io/mlflow/mlflow:v3.18.0` (fijada por digest) | 5000 |
| `superset` | propia (`infra/superset/Dockerfile`), Superset 6.1.0 + DuckDB 1.5.6 | 8088 (`SUPERSET_PORT`) |

Cómo levantar todo: [README raíz](../../README.md).
