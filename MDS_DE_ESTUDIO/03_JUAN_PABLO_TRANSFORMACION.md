# Juan Pablo Ramirez Carlón: Ingeniero de Datos (Transformación)

## Tu parte en 30 segundos

Tomas Bronce (los datos tal como llegaron, ya tipados) y los conviertes en información confiable. En **Plata**
limpias: quitas duplicados, separas música de podcast, calculas la hora local, agrupas en sesiones y etiquetas
los skips. En **Oro** armas el **modelo estrella** (dimensiones + tabla de hechos) y los **marts de KPIs** que
consume el tablero de Emilio; el modelo de Fernando lee de tu `fact_plays`. Y demuestras con conteos que no se
perdió ninguna fila en el camino.

## Lo que tienes que saber (máx. 5 conceptos)

- **Plata = reglas de negocio, medidas antes de fijarlas.** Duplicado = mismo `(ts, canción, ms_played)`:
  medimos 27 (22 eran filas idénticas). `is_skip` es verdadero si el usuario saltó (`fwdbtn`, `backbtn`) o
  cambió de canción antes de 30 s; es `null` si la canción terminó por algo ajeno al usuario (cerró sesión, error):
  **mejor vacío que falso**. Resultado: 14,174 reproducciones con etiqueta y 18% de skips.
- **Hora local y sesiones.** Spotify da la hora en UTC; con el país se saca la zona horaria (`MX` a Ciudad de
  México). Si el país viene como `ZZ` (desconocido) pero la sesión tiene un solo país, se usa ese; solo 10 filas
  quedan sin hora local. Una **sesión** se corta cuando pasan más de 30 min sin escuchar: 1,619 sesiones.
- **Modelo estrella.** `fact_plays` (una fila por reproducción, con llaves y medidas) rodeada de dimensiones:
  `dim_date`, `dim_artist` (con género), `dim_track`, `dim_platform`. Los **marts** son tablas ya agregadas
  (escucha diaria, top artistas por mes, géneros por mes, retención) para que el tablero responda al instante.
- **MERGE incremental + OPTIMIZE/Z-ORDER.** `fact_plays` no se reescribe: un `MERGE` por `play_id` inserta lo
  nuevo, actualiza solo lo que cambió y borra lo que ya no está. Re-correr escribe 0 filas. Si algo cambió, se
  compacta con `OPTIMIZE ... ZORDER BY date_key` (archivos ordenados por fecha: los filtros por fecha leen menos).
  Está particionada por año.
- **Reconciliación.** Cada fila de Bronce termina en un lugar con causa: `15,743 = 15,628 canciones + 50
  episodios + 65 descartadas (27 duplicadas, 38 sin contenido)`, y Plata = Oro (15,628). Si no cuadra, el
  comando falla.

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| `src/lakehouse/transformation/catalogs.py` | Reglas fijas: zonas por país, familias de plataforma, skip | `skip_label` y `PLATFORM_FAMILIES` (el orden importa) |
| `src/lakehouse/transformation/silver.py` | Bronce a Plata | `dedup`, `add_sessions` (ventana con hueco de 30 min), `add_local_time`, `build_artists` |
| `src/lakehouse/transformation/gold.py` | Plata a Oro | `merge_fact` (MERGE), `optimize_fact` (Z-ORDER), los `kpi_*` |
| `src/lakehouse/transformation/reconcile.py` | Conteos entre capas | Las 4 igualdades que deben cumplirse |
| `src/lakehouse/governance/contracts.py` | Esquema de tus tablas (de Victor) | `SILVER_PLAYS`, `GOLD_FACT_PLAYS` (particionada por `year`) |
| `tests/transformation/` | Pruebas | `test_silver.py` (cada regla con su caso) y `test_gold.py` (MERGE: 0 cambios al re-correr) |

## Cómo verlo funcionando

```powershell
docker compose exec lakehouse python -m lakehouse.transformation all
```

Debe imprimir los conteos de Plata, `MERGE fact_plays insertadas=15628` y cuatro líneas `[OK]` de
reconciliación. Si lo corres otra vez: `insertadas=0, actualizadas=0, borradas=0` y
`OPTIMIZE+ZORDER=no (sin cambios)`. En MinIO (<http://localhost:9001>) se ven las tablas en `silver/` y `gold/`.

## Tu minuto en el video (guion)

"Yo hice la transformación. En Plata limpiamos Bronce con reglas que primero medimos: encontramos 27
duplicados y los quitamos, separamos canciones de podcasts y calculamos la hora local de cada reproducción con su
zona horaria. [Mostrar el log de Plata.] Agrupamos en sesiones cuando hay más de 30 minutos sin escuchar, y
etiquetamos cada skip: si el usuario saltó o cambió antes de 30 segundos; si la canción se cortó por otra razón,
la dejamos vacía en vez de inventar. En Oro armamos un modelo estrella: una tabla de hechos con cada
reproducción y dimensiones de fecha, artista con su género, canción y plataforma, más tablas de KPIs que usa el
tablero. [Mostrar `gold/` en MinIO.] La tabla de hechos se actualiza con MERGE: si la corro otra vez, no escribe
nada. [Correrla de nuevo: insertadas=0.] Y al final reconciliamos: las 15,743 filas de Bronce están todas
explicadas, cada descarte con su causa. [Mostrar las líneas OK.]"

## Preguntas que te pueden hacer

- **¿Por qué Plata se recalcula completa y Oro no?** Sesiones y duplicados dependen de las filas vecinas, y
  son ~15 mil filas (segundos). Oro usa MERGE para mostrar la carga incremental y no reescribir lo que no cambió.
- **¿Qué es Z-ORDER?** Ordena los datos dentro de los archivos por una columna (`date_key`); Delta guarda el
  mínimo y máximo de cada archivo y se salta los que no caen en el filtro.
- **¿Por qué solo el 52% de los artistas tiene género?** MusicBrainz encontró a 1,099 de 1,301, pero la
  comunidad solo etiquetó géneros a 678. Los demás quedan con género `null`: no lo inventamos.
- **¿Cómo sabes que no perdiste filas?** Reconciliación: Bronce = canciones + episodios + descartadas con causa,
  y Plata = Oro. Corre en cada ejecución y falla si no cuadra.
- **¿Qué pasa con las 10 filas sin hora local?** Están en `fact_plays` con `date_key` vacío; los KPIs diarios
  solo cuentan filas con fecha (15,618) y la reconciliación lo verifica.
