# Emilio Rendón Trias: Analista de Datos / BI

## Tu parte en 30 segundos

Tú conviertes la capa Oro en **un tablero que se entiende de un vistazo**. Es un tablero de Apache Superset con 15
gráficas y 3 filtros (fecha, género y plataforma) sobre el historial de Spotify, más una sección que compara el
modelo de ML contra la realidad. Lees **solo Oro**, directo de MinIO, con DuckDB: no levantas Spark. Eres el último
paso del flujo: landing > bronce > plata > oro > **BI**.

## Lo que tienes que saber (máx. 5 conceptos)

- **Leer Oro sin Spark:**
  - Superset se conecta a DuckDB, un motor SQL que corre dentro del mismo proceso y responde en milisegundos.
  - DuckDB lee las tablas Delta directo del bucket `gold` con `delta_scan('s3://gold/fact_plays')`.
  - Entra con el usuario `svc-bi`, que **solo puede leer `gold`**: lo dice la matriz de Victor y se prueba en vivo.
- **Modelo estrella + marts:**
  - **Las gráficas con filtros** usan `fact_plays` unida a sus dimensiones (fecha, artista, canción, plataforma).
  - **Las de tendencia, géneros, ranking y retención** usan los marts que ya vienen agregados de Oro
    (`kpi_listening_daily`, `kpi_genres_monthly`, `kpi_top_artists_monthly`, `kpi_artist_retention`).
  - **Ganancia:** se consultan tablas chicas y precalculadas, sin repetir el cálculo en cada clic.
- **Tablero como código (D26):**
  - El tablero no se arma a mano. `bi/dashboard.py` define los datasets, las gráficas, los filtros y el acomodo, y
    genera el export oficial de Superset.
  - Al arrancar, el contenedor lo importa. Si alguien borra todo, se levanta igual desde un clon limpio.
- **Filtros nativos:**
  - Fecha, género y plataforma aplican a todas las gráficas que tienen esa columna.
  - Si un mart no tiene la columna (por ejemplo, retención no tiene plataforma), Superset lo marca como "no aplica"
    en vez de dar un resultado falso.
- **Métricas bien agregadas:**
  - La tasa de skip y la retención se calculan como **suma entre suma**, no como promedio de tasas. Así salen bien
    al juntar días o cohortes.
  - "sin genero" significa que MusicBrainz no tiene género para ese artista (H-004); no se inventa.

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| [bi/sql/](../bi/sql/) | SQL de los 6 datasets | `plays.sql`: la estrella unida; los demás: marts de Oro |
| [bi/dashboard.py](../bi/dashboard.py) | Datasets, métricas, 15 gráficas, filtros y acomodo | `CHARTS` y `LAYOUT` (qué se ve y dónde) |
| [bi/check.py](../bi/check.py) | Prueba en vivo: corre cada gráfica por la API y mide su tiempo | La línea final `15 de 15 graficas OK` |
| [infra/superset/](../infra/superset/) | Imagen (Superset 6.1 + DuckDB), config y arranque | `superset_config.py`: el secreto de `svc-bi` |
| [tests/bi/test_dashboard.py](../tests/bi/test_dashboard.py) | Corre el SQL del tablero en DuckDB contra el esquema de Oro | Que cada gráfica corra sin Superset |
| [ENTREGABLES/05_tablero_bi/](../ENTREGABLES/05_tablero_bi/) | Export, capturas y cómo abrirlo | El README |

## Cómo verlo funcionando

- **Tablero:** <http://localhost:8088/superset/dashboard/spotify-lakehouse/>, usuario `admin` (la contraseña está en
  el `.env` de Gabriel).
- **Prueba en vivo:** `docker compose exec superset python /opt/lakehouse/bi/check.py`. Debe terminar con
  `15 de 15 graficas OK` y el tiempo de cada una.
- **Qué mostrar:**
  - cambia el filtro de fecha a un año y todo se recalcula;
  - elige un género y se ajustan los KPIs, el top de artistas y la sección del modelo.

## Tu minuto en el video (guion)

> "Yo hice el tablero, que es donde todo el lakehouse se vuelve útil. Está en Superset y lee solo la capa Oro,
> directo de MinIO, con DuckDB: no hace falta levantar Spark y cada gráfica responde en segundos.
> *(pantalla: el tablero completo)*. Arriba, los KPIs: horas escuchadas, reproducciones, artistas y la tasa de skip.
> Luego la tendencia por mes, las plataformas, el top de artistas y de géneros, y los hábitos por hora y día.
> *(pantalla: el mapa de calor)*. Esta es la retención de artistas: de los que descubrió en un mes, qué parte sigue
> escuchando meses después. *(pantalla: retención)*. Y aquí se conecta con ML: la tasa de skip real contra lo que
> predice el modelo y el baseline en el periodo de prueba. Con los filtros de fecha, género y plataforma todo se
> recalcula. *(pantalla: aplicar un filtro)*. El tablero está definido como código: se levanta solo desde el repo
> y CI prueba cada gráfica en cada cambio."

## Preguntas que te pueden hacer

- **¿Por qué DuckDB y no Spark para el tablero?** Spark tarda segundos en arrancar una sesión; DuckDB responde en
  milisegundos y lee Delta directo. Spark sigue haciendo el trabajo pesado (Plata y Oro); el tablero solo lee.
- **¿Qué pasa si cambian los datos?** Las gráficas leen Oro en cada consulta, con 2 minutos de caché. Cuando Dagster
  corre el pipeline, el tablero muestra lo nuevo sin tocar nada.
- **¿Cómo sabes que los filtros no se rompen?** El filtro de género y el de plataforma apuntan a columnas que
  existen. Las pruebas revisan que cada gráfica use columnas de su dataset, y `check.py` corre las 15 en vivo.
- **¿Por qué hay tanto "sin genero"?** MusicBrainz solo tiene géneros para ~52% de los artistas. Preferimos mostrar
  el hueco a inventar un género.
- **¿El tablero puede escribir en el lakehouse?** No. `svc-bi` solo tiene permiso de lectura en `gold`, y la prueba
  de accesos de gobernanza lo verifica.
