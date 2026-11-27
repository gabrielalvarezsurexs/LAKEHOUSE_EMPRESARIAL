# Tablero de BI

Tablero de Apache Superset sobre la capa Oro, leída directo de MinIO con DuckDB. Se define como código en
[`bi/`](../../bi/) y se importa solo al levantar el stack (decisión D26 en
[ARQUITECTURA](../../DOCS/ARQUITECTURA.md)).

## Cómo abrirlo

1. Levanta el stack: `docker compose up -d --build` (README raíz, paso 1). Superset tarda ~2 min la primera vez.
2. Abre <http://localhost:8088/superset/dashboard/spotify-lakehouse/>. Usuario `admin`; la contraseña es
   `SUPERSET_ADMIN_PASSWORD` del `.env`.
3. Si todavía no hay datos en Oro, las gráficas marcan error hasta que Dagster termine la primera corrida.

Prueba en vivo (cada gráfica corre su consulta y se mide su tiempo):

```powershell
docker compose exec superset python /opt/lakehouse/bi/check.py
```

## Qué trae

| Sección | Gráficas | Fuente en Oro |
|---|---|---|
| KPIs | Horas escuchadas, reproducciones, artistas distintos, tasa de skip | `fact_plays` + dimensiones |
| Tendencias | Horas por mes; tendencia semanal de horas y sesiones | `fact_plays`; `kpi_listening_daily` |
| Consumo | Plataformas, top 10 artistas, géneros más escuchados | `fact_plays` + dimensiones |
| Géneros en el tiempo | Géneros por mes (top 8), artista #1 de cada mes | `kpi_genres_monthly`, `kpi_top_artists_monthly` |
| Hábitos y retención | Mapa de calor hora x día; retención de artistas por cohorte (hasta 12 meses) | `fact_plays`; `kpi_artist_retention` |
| Modelo de ML | Skip real vs modelo vs baseline (prueba); desempeño por conjunto | `ml_skip_predictions`, `ml_skip_features` |

Filtros: fecha, género y plataforma.

## Archivos

| Archivo | Qué es |
|---|---|
| [`tablero_export.zip`](tablero_export.zip) | Export de Superset (YAML), generado por `bi/dashboard.py`; una prueba revisa que esté al día |
| `captura_*.png` | Capturas del tablero con los datos reales, **aprobadas por el dueño de los datos** antes de subirlas |

Para regenerar el export después de cambiar `bi/`:
`uv run python bi/dashboard.py --out ENTREGABLES/05_tablero_bi/tablero_export.zip`.
