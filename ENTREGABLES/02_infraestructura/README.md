# Scripts de despliegue e infraestructura

Entregable 2. Toda la infraestructura es código en la raíz del repo; aquí está qué es cada pieza y dónde vive.

| Pieza | Archivo | Qué hace |
|---|---|---|
| Stack completo | [`docker-compose.yml`](../../docker-compose.yml) | Levanta MinIO, crea los buckets y construye el contenedor del lakehouse |
| Variables | [`.env.example`](../../.env.example) | Plantilla del `.env` (credenciales y ajustes); el `.env` real no se versiona |
| Imagen del lakehouse | [`infra/app/Dockerfile`](../../infra/app/Dockerfile) | Python 3.11 + Java 17 + PySpark y Delta instalados con uv desde `uv.lock` |
| JARs de Spark | [`infra/app/fetch_jars.sh`](../../infra/app/fetch_jars.sh) | Baja Delta y S3A en las versiones exactas del lock y verifica su SHA-1 |
| Buckets | [`infra/minio/init.sh`](../../infra/minio/init.sh) | Espera a MinIO y crea `landing`, `bronze`, `silver`, `gold` |
| Dependencias | [`pyproject.toml`](../../pyproject.toml), `uv.lock`, `.python-version` | Mismas versiones en local, contenedor y CI |
| Sesión de Spark | [`src/lakehouse/common/spark.py`](../../src/lakehouse/common/spark.py) | Delta + S3A apuntando a MinIO |
| Prueba de humo | [`src/lakehouse/common/smoke.py`](../../src/lakehouse/common/smoke.py) | Verifica buckets y escritura/lectura Delta con time travel |

## Servicios

| Servicio | Imagen | Puertos |
|---|---|---|
| `minio` | `cgr.dev/chainguard/minio` (fijada por digest) | 9000 API S3, 9001 consola |
| `minio-init` | `cgr.dev/chainguard/minio-client` (fijada por digest) | corre una vez y termina |
| `lakehouse` | propia (`infra/app/Dockerfile`) | 4040 Spark UI |

Se agregan en fases siguientes: Dagster (F5), MLflow (F5) y Superset (F7). Cómo levantar todo: [README raíz](../../README.md).
