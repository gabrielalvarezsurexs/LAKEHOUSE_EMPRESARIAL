# Arquitectura y decisiones

Diseño del lakehouse. Las decisiones llevan id (`D1`, `D2`...) para citarlas en código y docs. El diagrama final
(entregable 1) vive en `ENTREGABLES/01_diagrama_arquitectura/`; este es el borrador de trabajo.

## 1. Vista general

```mermaid
flowchart LR
  subgraph Fuentes
    S[JSON historial extendido<br/>Spotify]
    MB[API MusicBrainz<br/>géneros]
  end
  subgraph MinIO["MinIO (S3 local, Docker)"]
    L[(landing<br/>crudo tal cual)]
    B[(bronze<br/>Delta)]
    P[(silver<br/>Delta)]
    O[(gold<br/>Delta)]
  end
  subgraph Spark["Spark 3.5 + Delta Lake (contenedor)"]
    I[Ingesta]
    T1[Bronce a Plata]
    T2[Plata a Oro]
    Q[Checks de calidad]
  end
  S --> I
  MB --> I
  I --> L --> B
  B --> T1 --> P --> T2 --> O
  Q -.-> B & P & O
  O --> ML[ML: predicción de skip<br/>scikit-learn + MLflow]
  ML -->|predicciones| O
  O --> BI[Superset<br/>vía DuckDB]
  D[Dagster: schedules, sensores,<br/>linaje y monitoreo] -.orquesta.-> I & T1 & T2 & Q & ML
  GH[GitHub Actions CI] -.valida.-> Spark
```

## 2. Decisiones

| Id | Decisión | Por qué | Alternativa descartada |
|---|---|---|---|
| D1 | Todo local en Docker Compose, $0 | Lo permite el PDF; replicable con un solo comando | Nube (costo, cuentas) |
| D2 | **MinIO** como almacenamiento S3, imagen fijada a un tag de 2025 | Lo nombra la rúbrica. MinIO dejó de publicar imágenes comunitarias en oct 2025; un tag fijado es estable | SeaweedFS (no lo nombra la rúbrica) |
| D3 | **Un bucket por capa**: `landing`, `bronze`, `silver`, `gold` | Permite políticas de acceso por capa (mínimo privilegio) | Un bucket con prefijos |
| D4 | **Delta Lake** como formato de tabla | Rúbrica; ACID, esquema forzado, MERGE, time travel, OPTIMIZE | Iceberg (más piezas: catálogo REST) |
| D5 | **Spark (PySpark) en modo local dentro de un contenedor** | Lo vimos en clase. En contenedor se evita winutils/Hadoop de Windows | Cluster master/worker (más RAM, mismo resultado con 15k filas) |
| D6 | **Dagster** como orquestador | Assets por capa con grafo de linaje visual, asset checks (calidad por capa), schedules y sensores: evidencia directa para 3 criterios de la rúbrica | Airflow (más pesado, sin linaje de datos nativo) |
| D7 | La lógica vive en `src/lakehouse/` como funciones puras con CLI; Dagster solo las envuelve | Separa autoría (Nora orquesta, cada rol su lógica) y permite probar sin orquestador | Lógica dentro de los assets |
| D8 | **Seudonimización de `ip_addr` en Bronce** (SHA-256 con sal), se elimina en Plata | Privacidad desde la primera capa tabular; el crudo solo existe en landing (bucket restringido) | Guardar la IP en claro en Bronce |
| D9 | Géneros con **MusicBrainz**, respuestas crudas cacheadas en landing | Gratis, sin llave; la API de Spotify se cerró en 2026 (ver DATOS_FUENTE §2) | Spotify API, dataset de Kaggle |
| D10 | **Oro en modelo estrella** + marts de KPIs | BI rápido (consultas sobre tablas chicas y agregadas) y ML con features listas | Una tabla ancha |
| D11 | ML: **predicción de skip** (clasificación binaria), split temporal | Un solo usuario: no hay deserción entre usuarios; el skip tiene valor de negocio (curación de playlists) y miles de etiquetas | Recomendador (sin otros usuarios), clustering (métricas débiles) |
| D12 | ML lee Oro con Spark y entrena con scikit-learn; tracking con **MLflow** | Métricas ricas y trazables; 15k filas no necesitan MLlib | Spark MLlib |
| D13 | BI con **Apache Superset** en el compose, leyendo Oro por **DuckDB** (`delta_scan` sobre MinIO); el tablero se exporta al repo | Está en la lista del PDF, vive en el repo y se levanta solo; DuckDB responde en milisegundos sin levantar Spark | Power BI (solo Windows, no lee Delta de MinIO fácil), Spark Thrift (lento) |
| D14 | Datos sintéticos con el mismo esquema para CI y réplica | El repo es público: nadie más tiene nuestros datos | Subir una muestra real (privacidad) |
| D15 | Ingesta idempotente por hash SHA-256 de archivo + bitácora de ingesta | Re-correr no duplica; registro auditable | Sobrescribir todo cada vez |
| D16 | Registros malformados a **cuarentena** con causa, no se tiran ni rompen la corrida | Manejo de errores (rúbrica) y reconciliación de conteos | Fallar toda la corrida |
| D17 | Usuarios de MinIO por servicio con políticas mínimas (ingesta, transformación, BI solo lectura de `gold`) | Seguridad demostrable para Gobernanza | Usar el root en todo |
| D18 | **uv** para todo lo local (`pyproject.toml` + `uv.lock` versionados, Python 3.11); la imagen Docker instala con `uv sync --frozen` del mismo lock | Un solo manejador y versiones idénticas en local, contenedor y CI | pip/requirements por separado |
| D19 | **Código en la raíz**; `ENTREGABLES/` solo con lo que se entrega y no es código, numerado como el PDF | El revisor encuentra cada entregable sin buscar; el código queda como un proyecto Python normal | Todo dentro de `ENTREGABLES/` |

## 2.1 Estructura del repo

```text
/                         README (cómo replicar), CLAUDE.md, pyproject.toml, uv.lock, .python-version,
                          .env.example, docker-compose.yml
src/lakehouse/
  common/                 config, SparkSession, logging, errores           (Gabriel)
  ingestion/              landing, Bronce, MusicBrainz, sintéticos          (Ricardo)
  transformation/         Plata y Oro                                       (Juan Pablo)
  governance/             contratos, calidad, catálogo, linaje, seudónimos  (Victor)
  ml/                     features, entrenamiento, inferencia               (Fernando)
  orchestration/          Dagster: assets, checks, schedules, sensores, monitoreo (Nora)
infra/                    app/ (Dockerfile), minio/ (buckets y políticas), superset/
bi/                       vistas SQL sobre Oro y export del tablero         (Emilio)
notebooks/                presentación del modelo                           (Fernando)
tests/                    espejo de src/lakehouse/
.github/                  workflows/ (Nora), CODEOWNERS (Gabriel)
ENTREGABLES/              01_diagrama_arquitectura/ 02_infraestructura/ 03_calidad_y_linaje/
                          04_modelo_ml/ 05_tablero_bi/ 06_video/ y README.md (índice contra la rúbrica)
DOCS/  MDS_DE_ESTUDIO/    docs de trabajo y guías por integrante
```

## 3. Tablas por capa (borrador; se cierra en F2 a F4)

| Capa | Tabla | Grano | Notas |
|---|---|---|---|
| landing | `spotify/<fecha>/<archivo>.json`, `musicbrainz/<artista>.json` | archivo | Crudo intacto |
| bronze | `spotify_streams` | reproducción | Campos fuente + `ip_addr_hash` + `_source_file`, `_file_sha256`, `_batch_id`, `_ingested_at` |
| bronze | `musicbrainz_artists` | artista consultado | Respuesta cruda + metadatos |
| bronze | `_ingestion_log`, `_quarantine` | archivo / registro | Auditoría y errores |
| silver | `plays` | reproducción de música | Tipado, dedup, hora local, familia de plataforma, sesión, `is_skip` |
| silver | `episodes` | reproducción de podcast | |
| silver | `artists` | artista | Género principal y lista; `null` si no hubo match confiable |
| gold | `dim_track`, `dim_artist`, `dim_date`, `dim_platform` | dimensión | |
| gold | `fact_plays` | reproducción | Particionada por año, OPTIMIZE + Z-ORDER por fecha |
| gold | `kpi_listening_daily`, `kpi_top_artists_monthly`, `kpi_genres_monthly`, `kpi_artist_retention` | agregado | Lo que consume el tablero |
| gold | `ml_skip_features`, `ml_skip_predictions` | reproducción | Entrada y salida del modelo |

## 4. Servicios del compose (borrador)

| Servicio | Imagen | Puerto | Dueño |
|---|---|---|---|
| `minio` | `quay.io/minio/minio` (tag fijado) | 9000 API, 9001 consola | Gabriel |
| `minio-init` | `quay.io/minio/mc` (tag fijado) | | Gabriel (buckets), Victor (usuarios y políticas) |
| `lakehouse` | propia: Python 3.11 + Java 17 + PySpark + delta-spark + Dagster | 3000 Dagster, 4040 Spark UI | Gabriel (imagen), Nora (Dagster) |
| `mlflow` | `ghcr.io/mlflow/mlflow` (tag fijado) | 5000 | Nora |
| `superset` | propia: `apache/superset` + `duckdb-engine` | 8088 | Emilio |
