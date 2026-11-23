# Ricardo Zerón del Valle: Ingeniero de Datos (Ingesta)

## Tu parte en 30 segundos

Eres la puerta de entrada del lakehouse. Tomas los JSON del historial de Spotify, los guardas intactos en
**landing** y los conviertes en la tabla Delta `bronze/spotify_streams` con esquema explícito. Cada registro se
revisa contra el contrato: el que no cumple va a **cuarentena** con su causa, no se tira ni rompe la corrida.
Además consigues los géneros de cada artista con la API de MusicBrainz y generas datos sintéticos para que
cualquiera pueda correr el pipeline sin nuestros datos.

## Lo que tienes que saber (máx. 5 conceptos)

- **Landing vs Bronce:** landing es el archivo crudo tal cual (si algo sale mal, se reprocesa desde ahí). Bronce
  ya es tabla Delta con tipos (`ts` como timestamp, `ms_played` como número) y metadatos de quién, cuándo y de qué
  archivo vino cada fila (`_source_file`, `_file_sha256`, `_batch_id`, `_ingested_at`).
- **Idempotencia:** correr la ingesta dos veces da el mismo resultado. Cada archivo se identifica por su huella
  **SHA-256**; si ya está en la bitácora `_ingestion_log`, se salta. Y aunque se reprocese, se escribe con
  `replaceWhere` sobre ese hash: reemplaza las filas de ese archivo, nunca las duplica.
- **Cuarentena:** un registro con `ms_played = "abc"` o sin `ts` va a `bronze/_quarantine` con la causa exacta
  (`bad_type:ms_played`, `missing_required:ts`). Así se cumple la reconciliación: leídas = cargadas + cuarentena.
- **Seudonimización:** la IP es dato personal. Al entrar a Bronce se reemplaza por un HMAC-SHA256 con una sal
  secreta del `.env`: sirve para saber "misma conexión" sin saber cuál IP era. En Plata se elimina.
- **API con respeto:** MusicBrainz pide 1 petición por segundo y un User-Agent. El cliente espera entre
  peticiones, reintenta si la API está ocupada (503) y guarda cada respuesta en landing: la segunda corrida ya
  no llama a la API.

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| `src/lakehouse/ingestion/landing.py` | Sube los JSON a `landing/spotify/<fecha>/` | El SHA-256 en los metadatos; no sube dos veces el mismo contenido |
| `src/lakehouse/ingestion/records.py` | Valida cada registro contra el contrato | `validate_record`, las causas de cuarentena y `hash_ip` |
| `src/lakehouse/ingestion/bronze.py` | Escribe Bronce, cuarentena y bitácora | `replace_file_slice` (idempotencia) y el orden: datos, cuarentena, bitácora |
| `src/lakehouse/ingestion/musicbrainz.py` | Géneros por artista | `_throttle` (1 req/s), reintentos, `choose_candidate` (score ≥ 90 y mismo nombre) |
| `src/lakehouse/ingestion/synthetic.py` | Datos falsos con el mismo esquema | IPs de documentación, artistas "Artista Sintetico 0001" |
| `src/lakehouse/governance/contracts.py` | Esquema de cada tabla (de Victor) | `SPOTIFY_STREAMS_SOURCE`: tipos y qué campos son obligatorios |
| `tests/ingestion/` | Pruebas | Idempotencia, cuarentena y MusicBrainz con respuestas grabadas (sin red) |

## Cómo verlo funcionando

```powershell
docker compose exec lakehouse python -m lakehouse.ingestion all
```

Debe decir `leidas=15558, cargadas=15558, cuarentena=0` (y 185 del segundo archivo) y al final
`spotify_streams=15743`. Si lo corres otra vez: `ya ingeridos=2` y los mismos conteos. En la consola de MinIO
(<http://localhost:9001>) se ven los JSON en `landing/spotify/` y las tablas en `bronze/`.

## Tu minuto en el video (guion)

"Yo hice la ingesta. Los JSON de Spotify llegan primero a landing tal cual, con su huella SHA-256. [Mostrar
landing en MinIO.] Luego cada registro se valida contra el contrato de esquema: si un campo trae un tipo
incorrecto o falta la fecha, el registro va a cuarentena con su causa, y la corrida sigue. [Mostrar el log:
15,743 cargadas, 0 en cuarentena.] La ingesta es idempotente: si la corro otra vez, detecta por el hash que los
archivos ya entraron y no duplica nada. [Correrla de nuevo.] La IP, que es dato personal, se seudonimiza con una
sal secreta antes de llegar a Bronce. Para los géneros usamos la API de MusicBrainz respetando su límite de una
petición por segundo; las respuestas se guardan en landing para no volver a pedirlas. Y como el repo es público,
hice un generador de datos sintéticos con el mismo esquema para que cualquiera pueda correr el pipeline."

## Preguntas que te pueden hacer

- **¿Por qué no leer el JSON directo con Spark?** El lector de Spark marca corrupto el archivo entero; nosotros
  queríamos la causa por registro. Validamos en Python (15 mil filas caben de sobra) y Spark escribe Delta.
- **¿Qué pasa si se cae a la mitad?** La bitácora se escribe al final. La siguiente corrida reprocesa ese archivo
  y `replaceWhere` deja las filas igual, sin duplicar.
- **¿Por qué MusicBrainz y no la API de Spotify?** En 2026 Spotify exige Premium para el modo desarrollo y quitó
  datos de artistas; MusicBrainz es gratis, abierta y sin llave.
- **¿Y si MusicBrainz no encuentra al artista?** Queda sin género (`no_match`). Solo aceptamos un candidato con
  score alto y el mismo nombre: mejor sin género que con uno falso.
- **¿Se puede revertir el hash de la IP?** No sin la sal, que vive en el `.env` y nunca se versiona.
