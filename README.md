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

> **Estado (2026-11-22):** en construcción. Base del proyecto lista; sigue la infraestructura (F1). Avance en
> [DOCS/ROADMAP.md](DOCS/ROADMAP.md). Entrega: 30 de noviembre de 2026.

## Estructura

| Carpeta | Contenido |
|---|---|
| `src/lakehouse/` | Código del pipeline, un subpaquete por rol (se crea en F1) |
| `infra/`, `docker-compose.yml` | Infraestructura (se crea en F1) |
| `bi/`, `notebooks/`, `tests/` | Tablero, notebooks del modelo y pruebas |
| [`ENTREGABLES/`](ENTREGABLES/) | Entregables que no son código: diagrama, reportes, tablero, video |
| [`DOCS/`](DOCS/) | Arquitectura, decisiones, datos fuente, roadmap |
| [`MDS_DE_ESTUDIO/`](MDS_DE_ESTUDIO/) | Guía de cada integrante sobre su parte |

## Cómo replicar

Se completa en F1 a F8. Requisitos previstos: Docker Desktop y [uv](https://docs.astral.sh/uv/). Los datos reales
no se publican (privacidad); el repo incluirá un generador de datos sintéticos con el mismo esquema para correr
todo el pipeline.

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
