# Lakehouse Empresarial

Proyecto Final de **Datos Masivos** (Universidad Panamericana). Un Data Lakehouse local y de costo cero sobre el
historial extendido de reproducciones de Spotify:

- **Almacenamiento:** MinIO (S3 local en Docker), un bucket por capa.
- **Formato y cómputo:** Delta Lake con Apache Spark (PySpark), arquitectura Medallion (Bronce > Plata > Oro).
- **Orquestación:** Dagster (schedules, sensores, checks de calidad y linaje).
- **Gobernanza:** contratos de esquema, catálogo, linaje, pruebas de calidad por capa y seudonimización de datos
  personales.
- **Machine Learning:** predicción de skips leyendo de la capa Oro, con seguimiento en MLflow.
- **BI:** tablero en Apache Superset sobre la capa Oro.
- **Dependencias:** uv (Python 3.11) en local; Docker Compose para los servicios.

> **Estado (2026-11-23):** en construcción. Infraestructura (F1), ingesta (F2), transformación (F3) y gobernanza
> (F4) listas; sigue orquestación (F5). Avance en
> [DOCS/ROADMAP.md](DOCS/ROADMAP.md). Entrega: 30 de noviembre de 2026.

## Estructura

| Carpeta | Contenido |
|---|---|
| [`src/lakehouse/`](src/lakehouse/) | Código del pipeline, un subpaquete por rol (`common`, `ingestion`, `transformation`, `governance`, `ml`, `orchestration`) |
| [`infra/`](infra/), [`docker-compose.yml`](docker-compose.yml) | Imagen de Spark + Delta, buckets de MinIO y el stack |
| [`tests/`](tests/) | Pruebas, espejo de `src/lakehouse/` |
| `bi/`, `notebooks/` | Tablero y notebooks del modelo (F6 y F7) |
| [`ENTREGABLES/`](ENTREGABLES/) | Entregables que no son código: diagrama, reportes, tablero, video |
| [`DOCS/`](DOCS/) | Arquitectura, decisiones, datos fuente, roadmap |
| [`MDS_DE_ESTUDIO/`](MDS_DE_ESTUDIO/) | Guía de cada integrante sobre su parte |

## Cómo replicar

Requisitos: [Docker Desktop](https://www.docker.com/products/docker-desktop/) (prendido) y
[uv](https://docs.astral.sh/uv/). Los datos reales no se publican (privacidad); el pipeline corre igual con
datos sintéticos del mismo esquema (paso 2).

### 1. Infraestructura (MinIO + Spark + Delta)

```powershell
Copy-Item .env.example .env          # luego cambia la contraseña de MinIO en .env
# Sal para seudonimizar IPs (se agrega a .env sin imprimirse):
$b = New-Object byte[] 24; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b)
Add-Content .env ("IP_HASH_SALT=" + (-join ($b | ForEach-Object { $_.ToString('x2') })))
# Contraseñas de los 5 usuarios de servicio de MinIO (una sola vez):
foreach ($r in 'INGESTION','TRANSFORMATION','GOVERNANCE','ML','BI') {
  $b = New-Object byte[] 24; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b)
  Add-Content .env ("MINIO_SECRET_$r=" + (-join ($b | ForEach-Object { $_.ToString('x2') }))) }
docker compose up -d --build         # la primera vez tarda (baja Java, PySpark y los JARs)
docker compose exec lakehouse python -m lakehouse.common.smoke
```

El smoke debe terminar con `Smoke OK`. Consola de MinIO: <http://localhost:9001> (usuario y contraseña del `.env`),
con los buckets `landing`, `bronze`, `silver` y `gold`, y los usuarios `svc-*` de cada servicio (permisos mínimos).

### 2. Ingesta (landing > Bronce + géneros)

Sin datos reales, con sintéticos:

```powershell
docker compose exec lakehouse python -m lakehouse.ingestion synthetic --out /tmp/sinteticos --rows 5000
docker compose exec lakehouse python -m lakehouse.ingestion all --source-dir /tmp/sinteticos --genres synthetic
```

Con el historial real (carpeta `SPOTIFY_DATA_DIR` del `.env`): `... ingestion all` y luego `... ingestion genres`
(consulta MusicBrainz una sola vez, ~1 hora). Re-correr no duplica nada. No mezcles sintéticos y reales en el
mismo MinIO: para cambiar, `docker compose down -v`.

### 3. Transformación (Bronce > Plata > Oro)

```powershell
docker compose exec lakehouse python -m lakehouse.transformation all
```

Limpia y enriquece en Plata, arma el modelo estrella y los KPIs en Oro, y reconcilia conteos entre capas
(termina con cuatro líneas `[OK]`; si algo no cuadra, el comando falla). Re-correrlo no escribe filas nuevas en
`fact_plays` (MERGE incremental).

### 4. Gobernanza (calidad, accesos, catálogo y linaje)

```powershell
docker compose exec lakehouse python -m lakehouse.governance check
```

Corre los checks de calidad sobre Bronce, Plata y Oro, prueba los permisos de cada usuario de MinIO y escribe
[`ENTREGABLES/03_calidad_y_linaje/CALIDAD_Y_LINAJE.md`](ENTREGABLES/03_calidad_y_linaje/CALIDAD_Y_LINAJE.md)
(solo conteos). El catálogo y el linaje se generan desde los contratos con
`uv run python -m lakehouse.governance docs`.

### 5. Pruebas

```powershell
uv sync                              # instala Python 3.11 y las dependencias del uv.lock
uv run ruff check .
uv run pytest                        # las de Spark se saltan en local
docker compose exec lakehouse pytest # todas, incluidas las de Spark
```

Los pasos del pipeline completo se agregan conforme cierra cada fase.

## Equipo

| Rol | Integrante | ID |
|---|---|---|
| Arquitecto de Datos / Líder | Gabriel Álvarez Mendoza | 0261445 |
| Ingeniero de Datos (Ingesta) | Ricardo Zerón del Valle | 0261606 |
| Ingeniero de Datos (Transformación) | Juan Pablo Ramirez Carlón | 0262576 |
| Oficial de Gobernanza y Seguridad | Victor Gabriel Torres Guerrero | 0265786 |
| Analista de Datos / BI | Emilio Rendón Trias | 0267019 |
| Ingeniero de ML | Fernando Peña Meade | 0261604 |
| Ingeniera DevOps / MLOps | Nora Valentina Frías Alpízar | 0268966 |

Qué parte del repo le toca a cada quien: [DOCS/EQUIPO.md](DOCS/EQUIPO.md).
