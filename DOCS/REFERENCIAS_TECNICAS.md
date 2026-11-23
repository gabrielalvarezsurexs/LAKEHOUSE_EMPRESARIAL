# Referencias técnicas

Lo investigado para el stack, con trampas conocidas. Las versiones marcadas "verificar en F1" se fijan al construir
la imagen y se actualizan aquí.

## Entorno de Gabo (2026-11-22)

Windows 11, 31 GB RAM, 24 hilos, Docker 29.5 + Compose 5.1 (el daemon se prende a mano), Python 3.14 local, uv
0.11, Java 11 local (no se usa: Spark corre en contenedor).

## uv (dependencias)

- Proyecto con `pyproject.toml` (layout `src/`) y `uv.lock` versionados; `.python-version` = 3.11 (uv baja ese
  Python solo, sin tocar el 3.14 de la máquina).
- Grupos de dependencias para no cargar todo en todos lados: base (pyspark, delta-spark, dagster), `ml`
  (scikit-learn, mlflow), `dev` (pytest, ruff). Se agregan con `uv add <pkg>` / `uv add --group dev <pkg>`.
- Comandos: `uv sync`, `uv run pytest`, `uv run ruff check .`, `uv run python -m lakehouse.<modulo>`.
- Dockerfile: copia `pyproject.toml` y `uv.lock`, corre `uv sync --frozen --no-install-project` (capa
  cacheada), luego copia `src/` y `tests/` y sincroniza el proyecto. Incluye el grupo `dev` para correr pytest
  dentro del contenedor. El venv vive en `/opt/venv` para que montar `./src` (compose) no lo tape. uv se toma con
  `COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /uvx /bin/`.
- Pruebas con Spark: marcador `@pytest.mark.spark`; solo corren con `RUN_SPARK_TESTS=1` (el contenedor lo trae).
- Ver el avance de un build: `docker compose --progress plain build lakehouse` (`--progress` va antes de
  `build`). No canalizar a `Select-Object -Last`: no imprime nada hasta terminar.
- Avisos normales del smoke (no son errores): `NativeCodeLoader`, `MetricsConfig` de S3A y
  `DeltaHistoryManager ... timestamp greater than` (dos commits en el mismo segundo; Delta ajusta el orden).
- ¿Por qué Java? Spark es un motor de la JVM; PySpark solo lo controla desde Python. La alternativa sin Java
  (delta-rs + Polars/DuckDB) se descartó porque el curso usa Spark.
- Superset es imagen aparte (no comparte el lock): solo agrega `duckdb-engine` con versión fijada.

## MinIO

- En oct 2025 MinIO dejó de publicar imágenes comunitarias en Docker Hub y en sep 2026 cerró también el acceso
  anónimo a `quay.io/minio/*` (responde 401; verificado el 2026-11-22, ver H-002). Su último release libre trae
  CVE-2026-40344.
- **Se usa Chainguard** (gratis, compilada desde el código fuente y mantenida): `cgr.dev/chainguard/minio` y
  `cgr.dev/chainguard/minio-client:latest-dev` (la `-dev` trae shell para `init.sh`). El plan gratuito solo da
  `latest`, así que se **fijan por digest** en `docker-compose.yml`. Para actualizar: tomar el digest nuevo y
  correr el smoke.
- Ambas corren como el usuario 65532 sin shell (la de MinIO); `minio` corre con `user: "0:0"` para poder escribir
  en el volumen nuevo, y no tiene healthcheck (no trae curl ni `mc`): `minio-init` espera con reintentos.
- Fuentes: [Chainguard MinIO](https://www.chainguard.dev/docs/chainguard/containers/getting-started/web-and-data-services/minio),
  [IBM: imagen retirada de quay.io](https://www.ibm.com/support/pages/node/7289585),
  [MinIO CE en 2026](https://dev.to/rosgluk/minio-ce-in-2026-retired-upstream-source-only-and-what-to-use-1k02).

## Spark + Delta + S3A

- Fijado en F1 (uv.lock): **PySpark 3.5.10 + delta-spark 3.3.3**, con hadoop-aws 3.3.4 + aws-java-sdk-bundle
  1.12.262, Java 17 (Debian bookworm), Python 3.11.18 (`python:3.11.18-slim-bookworm`), uv 0.11.16.
- Los JAR los baja `infra/app/fetch_jars.sh` al construir la imagen, **derivando las versiones del lock** (Delta
  de `delta-spark`, Hadoop y Scala de los JAR que trae PySpark) y verificando el SHA-1 de Maven Central. Si se
  actualiza PySpark o Delta con `uv lock`, los JAR se ajustan solos al reconstruir.
- Configuración clave de la SparkSession:
  - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
  - `spark.hadoop.fs.s3a.endpoint=http://minio:9000`, `fs.s3a.path.style.access=true`,
    `fs.s3a.connection.ssl.enabled=false`, `fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
  - Llaves por variables de entorno, nunca en código.
- Trampas: hadoop-aws debe coincidir con la versión de Hadoop que trae PySpark (3.3.4 para Spark 3.5); sin
  `path.style.access` MinIO responde 403/404; un 403 casi siempre son llaves que no corresponden al bucket.
- `OPTIMIZE ... ZORDER BY` y `VACUUM` están disponibles en delta-spark open source.
- Escritura idempotente por archivo (F2): `mode("overwrite").option("replaceWhere", "_file_sha256 = '<hash>'")`
  reemplaza solo las filas de ese archivo en una sola transacción; si la tabla no existe se crea con `append`.
  Delta valida que todas las filas escritas cumplan el predicado.
- `spark.createDataFrame(filas, contrato.spark_schema())`: el esquema sale del DDL del contrato
  (`_parse_datatype_string`, necesita SparkSession activa). Las fechas deben ir como `datetime` con zona UTC.

## boto3 (archivos crudos en landing)

- Para objetos sueltos (subir JSON, caché de MusicBrainz) se usa boto3, no Spark: `endpoint_url` de MinIO,
  `addressing_style: path`, `signature_version: s3v4`, reintentos `standard`. Envoltura en
  `lakehouse.common.storage.S3ObjectStore`; las pruebas usan una versión en memoria (`tests/conftest.py`).
- Los metadatos de objeto (`sha256`, `original-name`) viajan como headers: solo ASCII, y boto3 los regresa en
  minúsculas.

## Dagster

- Software-defined assets (una por tabla), `AssetCheck` para calidad, `ScheduleDefinition` y `sensor` sobre
  landing (archivo nuevo dispara la ingesta): cubre "sin ejecuciones manuales".
- La UI muestra el grafo de linaje y el historial de checks: evidencia para Gobernanza y para el video.
- `DAGSTER_HOME` con almacenamiento local en un volumen (gitignored).

## DuckDB para BI

- La extensión `delta` de DuckDB salió de experimental en mayo 2026 (lectura y escritura, time travel). Para BI
  solo se lee: `delta_scan('s3://gold/...')` con un `SECRET` de S3 apuntando a MinIO (`ENDPOINT`, `URL_STYLE
  'path'`, `USE_SSL false`).
- Superset se conecta con `duckdb-engine` (SQLAlchemy). Las vistas sobre Oro se crean al iniciar.
- Fuentes: [DuckDB delta](https://duckdb.org/docs/current/core_extensions/delta),
  [Delta en DuckDB, mayo 2026](https://duckdb.org/2026/05/07/delta-uc-updates).

## Superset

- Imagen oficial `apache/superset` fijada (verificar en F7) más `pip install duckdb-engine` en una imagen propia.
- El tablero se exporta como ZIP/YAML al repo (`bi/`) y se importa al arrancar: el tablero vive en el repo.

## MusicBrainz

- `GET https://musicbrainz.org/ws/2/artist/?query=artist:"<nombre>"&fmt=json` y luego
  `GET /ws/2/artist/<mbid>?inc=genres&fmt=json`. `inc=genres` no requiere autenticación.
- User-Agent obligatorio (si falta, se trata como anónimo y se limita); ritmo seguro: 1 petición por segundo;
  503 = demasiadas, reintentar con espera.
- Implementado en `lakehouse.ingestion.musicbrainz`: búsqueda con `limit=5` y la frase escapada para Lucene
  (`"` y `\`); se acepta un candidato solo con `score >= 90` y nombre (o alias) igual tras quitar acentos,
  mayúsculas y signos. Reintenta 429/5xx y errores de red con espera 2, 4, 8 s (o `Retry-After`); 404 y otros
  4xx no se reintentan. Los errores no se cachean, así que la siguiente corrida los vuelve a pedir.
- En la práctica la API responde 503 en ~1 de cada 10 peticiones aun a 1 req/s; los reintentos van a nivel
  DEBUG y el resumen final trae `reintentos=N`. Para verlos uno por uno: `LOG_LEVEL=DEBUG`.
- Aviso normal de Spark en la ingesta: `SparkStringUtils: Truncated the string representation of a plan` (el
  esquema tiene muchas columnas; no afecta los datos).
- Fuentes: [API](https://musicbrainz.org/doc/MusicBrainz_API), [Rate limiting](https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting).

## Spotify Web API (plan B, no se usa)

- Feb 2026: el modo desarrollo exige Premium, una app por desarrollador y 5 usuarios; quitó `popularity` y
  `followers` de artistas y varios endpoints de lote. `genres` reportado vacío o en desuso (no oficial).
- Fuentes: [Changelog feb 2026](https://developer.spotify.com/documentation/web-api/references/changes/february-2026),
  [Guía de migración](https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide),
  [TechCrunch](https://techcrunch.com/2026/02/06/spotify-changes-developer-mode-api-to-require-premium-accounts-limits-test-users/).

## Historial extendido de Spotify

- Se pide en la página de privacidad de la cuenta; tarda hasta 30 días. Formato y campos en
  [DATOS_FUENTE.md](DATOS_FUENTE.md). Guía: [Spotify: entender tus datos](https://support.spotify.com/us/article/understanding-your-data/).
