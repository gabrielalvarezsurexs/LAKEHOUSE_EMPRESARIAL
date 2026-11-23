# Gabriel Álvarez Mendoza: Arquitecto de Datos / Líder

> Borrador de F1. Se completa en F8 con el diagrama final y la prueba de réplica.

## Tu parte en 30 segundos

Diseñaste la casa donde vive todo: MinIO como almacenamiento S3 local con un bucket por capa, Spark con Delta Lake
como motor y formato de tabla, y un contenedor que cualquiera levanta con un solo comando. Además pusiste las
reglas del juego: cómo se dividen los módulos por rol, cómo se configuran y cómo se prueba que la infraestructura
funciona antes de mover un solo dato.

## Lo que tienes que saber (máx. 5 conceptos)

- **Lakehouse:** une lo barato y flexible de un data lake (archivos en S3) con lo confiable de un data warehouse
  (tablas con esquema y transacciones). Aquí: archivos en MinIO + tablas Delta.
- **Medallion (landing > bronze > silver > gold):** cada capa agrega calidad. Landing es el crudo intacto, Bronce
  lo vuelve tabla, Plata lo limpia y Oro lo deja listo para BI y ML. Un bucket por capa permite dar permisos por
  capa.
- **Delta Lake:** Parquet + un `_delta_log` con cada cambio. Da ACID, esquema forzado, MERGE y *time travel* (leer
  una versión anterior). El smoke lo demuestra: escribe 100 filas, agrega 50 y lee la versión 0 (100 filas).
- **S3A + path style:** Spark habla con MinIO como si fuera S3 usando el conector S3A de Hadoop. MinIO necesita
  `path.style.access=true` (URLs `host/bucket/llave`), si no responde 403/404.
- **Reproducibilidad:** uv fija versiones exactas en `uv.lock`; la imagen Docker instala desde ese mismo lock y
  baja los JAR de Delta y S3A que corresponden. MinIO se fija por digest (MinIO dejó de dar imágenes gratis y se
  usa la de Chainguard).

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| `docker-compose.yml` | Define los servicios | `minio`, `minio-init` (crea buckets) y `lakehouse`; el orden con `depends_on` |
| `infra/app/Dockerfile` | Imagen de Spark + Delta | Java 17, `uv sync --frozen`, por qué el venv está en `/opt/venv` |
| `infra/app/fetch_jars.sh` | Baja los JAR | Que las versiones salen del lock y se verifica el SHA-1 |
| `infra/minio/init.sh` | Crea los 4 buckets | Espera a MinIO con reintentos antes de crear |
| `src/lakehouse/common/spark.py` | Configura la SparkSession | Extensiones de Delta y las llaves `fs.s3a.*` |
| `src/lakehouse/common/config.py` | Lee variables de entorno | Falla con mensaje claro si falta algo; las llaves no salen en logs |
| `src/lakehouse/common/smoke.py` | Prueba de humo | Revisa buckets, conteos, historial y time travel |
| `DOCS/ARQUITECTURA.md` | Decisiones D1 a D19 | El porqué de cada pieza y su alternativa |

## Cómo verlo funcionando

```powershell
docker compose up -d --build
docker compose exec lakehouse python -m lakehouse.common.smoke
```

Debe terminar con `Smoke OK`. En <http://localhost:9001> (consola de MinIO) se ven los 4 buckets.

## Tu minuto en el video (guion)

"Yo diseñé la arquitectura. Todo corre local y gratis con Docker Compose: MinIO hace de S3 y tiene un bucket por
capa del Medallion: landing, bronce, plata y oro. Encima, Spark con Delta Lake: cada tabla es Parquet más un log
de transacciones que nos da ACID, esquema forzado y *time travel*. [Mostrar el diagrama.] Cada integrante tiene su
módulo y se comunican solo por tablas Delta, nunca por imports, así la autoría de cada quien queda separada.
[Mostrar `src/lakehouse/`.] Para que cualquiera lo replique, las versiones están fijadas con uv y la imagen se
construye desde el mismo lock. Con un comando se levanta todo y una prueba de humo confirma que MinIO, Spark y
Delta funcionan. [Mostrar `Smoke OK` y la consola de MinIO.]"

## Preguntas que te pueden hacer

- **¿Por qué Delta y no Iceberg?** Delta funciona con Spark sin catálogo extra; Iceberg necesita un catálogo REST
  o Hive, más piezas para el mismo resultado.
- **¿Por qué un bucket por capa y no prefijos?** Para dar permisos por capa: BI solo lee `gold`.
- **¿Por qué Spark en modo local y no un cluster?** Son ~16 mil filas; un cluster solo agrega RAM y piezas. El
  código es el mismo: cambiar `SPARK_MASTER` lo manda a un cluster.
- **¿Qué pasó con la imagen de MinIO?** MinIO dejó de publicarla gratis (Docker Hub en 2025, quay.io en 2026);
  usamos la de Chainguard, que la compila del código fuente, fijada por digest.
- **¿Cómo sé que es reproducible?** `uv.lock` + imagen fijada + datos sintéticos; en F8 se prueba desde un clon
  limpio.
