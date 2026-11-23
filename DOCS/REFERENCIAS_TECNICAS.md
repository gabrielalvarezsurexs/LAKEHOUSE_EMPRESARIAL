# Referencias técnicas

Lo investigado para el stack, con trampas conocidas. Las versiones marcadas "verificar en F1" se fijan al construir
la imagen y se actualizan aquí.

## Entorno de Gabo (2026-11-22)

Windows 11, 31 GB RAM, 24 hilos, Docker 29.5 + Compose 5.1 (el daemon se prende a mano), Python 3.14 local, uv
0.11, Java 11 local (no se usa: Spark corre en contenedor).

## MinIO

- En oct 2025 MinIO dejó de publicar imágenes comunitarias en Docker Hub (`docker pull minio/minio` falla) y el
  proyecto entró en modo mantenimiento en dic 2025.
- Se usa un tag fijado de `quay.io/minio/minio` (último comunitario conocido: `RELEASE.2025-09-07T16-13-09Z`;
  verificar en F1 que baje). Plan B: `cgr.dev/chainguard/minio` (gratis, mantenida).
- Cliente `mc` también fijado (`quay.io/minio/mc`), para crear buckets, usuarios y políticas en `minio-init`.
- Fuentes: [Minimus: cambios de la imagen](https://www.minimus.io/post/minio-docker-image-changes-how-to-find-a-secure-minio-alternative),
  [MinIO CE en 2026](https://dev.to/rosgluk/minio-ce-in-2026-retired-upstream-source-only-and-what-to-use-1k02),
  [Chainguard MinIO](https://edu.chainguard.dev/chainguard/containers/getting-started/minio/).

## Spark + Delta + S3A (verificar en F1)

- Combinación objetivo, muy documentada: **PySpark 3.5.x + delta-spark 3.3.x + hadoop-aws 3.3.4 +
  aws-java-sdk-bundle 1.12.262**, Java 17, Python 3.11. Los JAR se bajan al construir la imagen (no en cada corrida).
- Configuración clave de la SparkSession:
  - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
  - `spark.hadoop.fs.s3a.endpoint=http://minio:9000`, `fs.s3a.path.style.access=true`,
    `fs.s3a.connection.ssl.enabled=false`, `fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
  - Llaves por variables de entorno, nunca en código.
- Trampas: hadoop-aws debe coincidir con la versión de Hadoop que trae PySpark (3.3.4 para Spark 3.5); sin
  `path.style.access` MinIO responde 403/404; un 403 casi siempre son llaves que no corresponden al bucket.
- `OPTIMIZE ... ZORDER BY` y `VACUUM` están disponibles en delta-spark open source.

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
