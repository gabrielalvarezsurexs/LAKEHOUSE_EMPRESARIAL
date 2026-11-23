# Catálogo de datos

> Generado desde `src/lakehouse/governance/contracts.py` con `uv run python -m lakehouse.governance docs`. No se edita a mano:
> se cambia el contrato y se regenera. Las mismas reglas corren como checks de calidad
> ([CALIDAD_Y_LINAJE.md](CALIDAD_Y_LINAJE.md)) y el linaje está en [LINAJE.md](LINAJE.md).

## Clasificación

| Nivel | Qué significa |
|---|---|
| restringido | Datos personales en claro (IP). Solo el servicio de ingesta. |
| confidencial | Seudonimos o registros crudos. Solo servicios del pipeline y gobernanza. |
| interno | Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro). |

Privacidad por columna: **personal** identifica a una persona (la IP); **seudónimo** es
un HMAC con sal que no se puede revertir sin la sal (D8). Ninguna tabla de Plata u Oro tiene
columnas con privacidad.

## Índice

| Tabla | Clasificación | Columnas | Dueño | Descripción |
|---|---|---|---|---|
| [`landing.spotify_streams_source`](#landingspotify_streams_source) | restringido | 23 | Ricardo Zeron del Valle (Ingesta) | Historial extendido de Spotify tal como lo entrega la plataforma (un arreglo JSON). |
| [`bronze.spotify_streams`](#bronzespotify_streams) | confidencial | 28 | Ricardo Zeron del Valle (Ingesta) | Una fila por reproduccion valida; ip_addr llega seudonimizada. |
| [`bronze._quarantine`](#bronze_quarantine) | confidencial | 7 | Ricardo Zeron del Valle (Ingesta) | Registros que no cumplieron el contrato, con su causa (D16). La IP va seudonimizada. |
| [`bronze._ingestion_log`](#bronze_ingestion_log) | interno | 8 | Ricardo Zeron del Valle (Ingesta) | Bitacora de ingesta: un renglon por archivo procesado (D15). |
| [`bronze.musicbrainz_artists`](#bronzemusicbrainz_artists) | interno | 10 | Ricardo Zeron del Valle (Ingesta) | Un renglon por artista consultado en MusicBrainz, con sus generos (D9). |
| [`silver.plays`](#silverplays) | interno | 28 | Juan Pablo Ramirez Carlon (Transformacion) | Una fila por reproduccion de musica, sin duplicados ni IP. |
| [`silver.episodes`](#silverepisodes) | interno | 25 | Juan Pablo Ramirez Carlon (Transformacion) | Una fila por reproduccion de podcast, sin duplicados ni IP. |
| [`silver._discarded`](#silver_discarded) | interno | 5 | Juan Pablo Ramirez Carlon (Transformacion) | Filas de Bronce que no pasan a Plata, con causa (reconciliacion de conteos). |
| [`silver.artists`](#silverartists) | interno | 5 | Juan Pablo Ramirez Carlon (Transformacion) | Un renglon por artista escuchado, con sus generos de MusicBrainz si hubo match confiable. |
| [`gold.dim_date`](#golddim_date) | interno | 11 | Juan Pablo Ramirez Carlon (Transformacion) | Calendario continuo entre la primera y la ultima fecha local con reproducciones. |
| [`gold.dim_artist`](#golddim_artist) | interno | 6 | Juan Pablo Ramirez Carlon (Transformacion) | Artistas con genero principal y lista de generos. |
| [`gold.dim_track`](#golddim_track) | interno | 4 | Juan Pablo Ramirez Carlon (Transformacion) | Canciones; si el nombre cambio en el tiempo se queda el de la reproduccion mas reciente. |
| [`gold.dim_platform`](#golddim_platform) | interno | 2 | Juan Pablo Ramirez Carlon (Transformacion) | Familias de plataforma y su tipo de dispositivo (catalogo fijo). |
| [`gold.fact_plays`](#goldfact_plays) | interno | 21 | Juan Pablo Ramirez Carlon (Transformacion) | Una fila por reproduccion de musica. MERGE incremental por play_id; Z-ORDER por date_key. |
| [`gold.kpi_listening_daily`](#goldkpi_listening_daily) | interno | 10 | Juan Pablo Ramirez Carlon (Transformacion) | Escucha por dia local: volumen, variedad, sesiones y tasa de skip. |
| [`gold.kpi_top_artists_monthly`](#goldkpi_top_artists_monthly) | interno | 8 | Juan Pablo Ramirez Carlon (Transformacion) | Artistas por mes con su ranking por minutos escuchados. |
| [`gold.kpi_genres_monthly`](#goldkpi_genres_monthly) | interno | 7 | Juan Pablo Ramirez Carlon (Transformacion) | Generos (principal del artista) por mes; genre null = artista sin genero conocido. |
| [`gold.kpi_artist_retention`](#goldkpi_artist_retention) | interno | 6 | Juan Pablo Ramirez Carlon (Transformacion) | Cohortes por mes de descubrimiento: que parte de los artistas se escucha N meses despues. |

## landing (archivos crudos)

### `landing.spotify_streams_source`

Historial extendido de Spotify tal como lo entrega la plataforma (un arreglo JSON).

- **Dueño:** Ricardo Zeron del Valle (Ingesta)
- **Clasificación:** restringido: Datos personales en claro (IP). Solo el servicio de ingesta.
- **Se alimenta de:** `ext.spotify`
- **Acceso en MinIO:** leen svc-ingestion; escriben svc-ingestion

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `ts` | `TIMESTAMP` | no |  |  | Momento (UTC) en que termino la reproduccion |
| `platform` | `STRING` | sí |  |  | Dispositivo y version de la app |
| `ms_played` | `BIGINT` | no |  | >= 0 | Milisegundos reproducidos |
| `conn_country` | `STRING` | sí |  |  | Pais de la conexion (ISO 3166-1 alfa-2) |
| `ip_addr` | `STRING` | sí | personal |  | IP de la conexion (dato personal) |
| `master_metadata_track_name` | `STRING` | sí |  |  | Nombre de la cancion |
| `master_metadata_album_artist_name` | `STRING` | sí |  |  | Artista del album |
| `master_metadata_album_album_name` | `STRING` | sí |  |  | Nombre del album |
| `spotify_track_uri` | `STRING` | sí |  |  | URI de la cancion (spotify:track:<id>) |
| `episode_name` | `STRING` | sí |  |  | Nombre del episodio de podcast |
| `episode_show_name` | `STRING` | sí |  |  | Nombre del podcast |
| `spotify_episode_uri` | `STRING` | sí |  |  | URI del episodio |
| `audiobook_title` | `STRING` | sí |  |  | Titulo del audiolibro |
| `audiobook_uri` | `STRING` | sí |  |  | URI del audiolibro |
| `audiobook_chapter_uri` | `STRING` | sí |  |  | URI del capitulo |
| `audiobook_chapter_title` | `STRING` | sí |  |  | Titulo del capitulo |
| `reason_start` | `STRING` | sí |  |  | Por que empezo la reproduccion |
| `reason_end` | `STRING` | sí |  |  | Por que termino la reproduccion |
| `shuffle` | `BOOLEAN` | sí |  |  | Modo aleatorio activo |
| `skipped` | `BOOLEAN` | sí |  |  | Spotify la marco como saltada (se llena poco) |
| `offline` | `BOOLEAN` | sí |  |  | Se reprodujo sin conexion |
| `offline_timestamp` | `BIGINT` | sí |  |  | Epoch de la reproduccion offline (segundos o milisegundos) |
| `incognito_mode` | `BOOLEAN` | sí |  |  | Sesion privada |


## bronze (Bronce)

### `bronze.spotify_streams`

Una fila por reproduccion valida; ip_addr llega seudonimizada.

- **Dueño:** Ricardo Zeron del Valle (Ingesta)
- **Clasificación:** confidencial: Seudonimos o registros crudos. Solo servicios del pipeline y gobernanza.
- **Se alimenta de:** `landing.spotify_streams_source`
- **Acceso en MinIO:** leen svc-ingestion, svc-transformation, svc-governance; escriben svc-ingestion
- **Llaves únicas:** (_file_sha256, _record_index)
- **Reglas:** ts no anterior a Spotify; ts no en el futuro; ip_addr_hash es HMAC-SHA256 (no una IP); _file_sha256 es un SHA-256

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `ts` | `TIMESTAMP` | no |  |  | Momento (UTC) en que termino la reproduccion |
| `platform` | `STRING` | sí |  |  | Dispositivo y version de la app |
| `ms_played` | `BIGINT` | no |  | >= 0 | Milisegundos reproducidos |
| `conn_country` | `STRING` | sí |  |  | Pais de la conexion (ISO 3166-1 alfa-2) |
| `ip_addr_hash` | `STRING` | sí | seudónimo |  | HMAC-SHA256 de ip_addr con la sal IP_HASH_SALT (seudonimo, D8) |
| `master_metadata_track_name` | `STRING` | sí |  |  | Nombre de la cancion |
| `master_metadata_album_artist_name` | `STRING` | sí |  |  | Artista del album |
| `master_metadata_album_album_name` | `STRING` | sí |  |  | Nombre del album |
| `spotify_track_uri` | `STRING` | sí |  |  | URI de la cancion (spotify:track:<id>) |
| `episode_name` | `STRING` | sí |  |  | Nombre del episodio de podcast |
| `episode_show_name` | `STRING` | sí |  |  | Nombre del podcast |
| `spotify_episode_uri` | `STRING` | sí |  |  | URI del episodio |
| `audiobook_title` | `STRING` | sí |  |  | Titulo del audiolibro |
| `audiobook_uri` | `STRING` | sí |  |  | URI del audiolibro |
| `audiobook_chapter_uri` | `STRING` | sí |  |  | URI del capitulo |
| `audiobook_chapter_title` | `STRING` | sí |  |  | Titulo del capitulo |
| `reason_start` | `STRING` | sí |  |  | Por que empezo la reproduccion |
| `reason_end` | `STRING` | sí |  |  | Por que termino la reproduccion |
| `shuffle` | `BOOLEAN` | sí |  |  | Modo aleatorio activo |
| `skipped` | `BOOLEAN` | sí |  |  | Spotify la marco como saltada (se llena poco) |
| `offline` | `BOOLEAN` | sí |  |  | Se reprodujo sin conexion |
| `offline_timestamp` | `BIGINT` | sí |  |  | Epoch de la reproduccion offline (segundos o milisegundos) |
| `incognito_mode` | `BOOLEAN` | sí |  |  | Sesion privada |
| `_source_file` | `STRING` | no |  |  | Llave del archivo en landing |
| `_file_sha256` | `STRING` | no |  |  | SHA-256 del archivo (idempotencia) |
| `_record_index` | `BIGINT` | no |  | >= 0 | Posicion del registro dentro del archivo |
| `_batch_id` | `STRING` | no |  |  | Corrida de ingesta |
| `_ingested_at` | `TIMESTAMP` | no |  |  | Momento (UTC) de la ingesta |

### `bronze._quarantine`

Registros que no cumplieron el contrato, con su causa (D16). La IP va seudonimizada.

- **Dueño:** Ricardo Zeron del Valle (Ingesta)
- **Clasificación:** confidencial: Seudonimos o registros crudos. Solo servicios del pipeline y gobernanza.
- **Se alimenta de:** `landing.spotify_streams_source`
- **Acceso en MinIO:** leen svc-ingestion, svc-transformation, svc-governance; escriben svc-ingestion
- **Reglas:** raw_record sin ip_addr en claro; _file_sha256 es un SHA-256

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `_source_file` | `STRING` | no |  |  | Llave del archivo en landing |
| `_file_sha256` | `STRING` | no |  |  | SHA-256 del archivo |
| `_record_index` | `BIGINT` | sí |  | >= 0 | Posicion en el archivo; null si fallo el archivo completo |
| `cause` | `STRING` | no |  |  | Motivo del rechazo, p. ej. bad_type:ms_played |
| `raw_record` | `STRING` | sí |  |  | Registro original en JSON (ip_addr reemplazada por su hash) |
| `_batch_id` | `STRING` | no |  |  | Corrida de ingesta |
| `_ingested_at` | `TIMESTAMP` | no |  |  | Momento (UTC) de la ingesta |

### `bronze._ingestion_log`

Bitacora de ingesta: un renglon por archivo procesado (D15).

- **Dueño:** Ricardo Zeron del Valle (Ingesta)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `landing.spotify_streams_source`
- **Acceso en MinIO:** leen svc-ingestion, svc-transformation, svc-governance; escriben svc-ingestion
- **Reglas:** leidas = cargadas + cuarentena

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `file_sha256` | `STRING` | no |  |  | SHA-256 del archivo |
| `source_file` | `STRING` | no |  |  | Llave del archivo en landing |
| `batch_id` | `STRING` | no |  |  | Corrida de ingesta |
| `rows_read` | `BIGINT` | no |  | >= 0 | Registros leidos del archivo |
| `rows_loaded` | `BIGINT` | no |  | >= 0 | Registros escritos en spotify_streams |
| `rows_quarantined` | `BIGINT` | no |  | >= 0 | Registros enviados a cuarentena |
| `unknown_fields` | `STRING` | sí |  |  | Campos que no estan en el contrato (deriva de esquema) |
| `ingested_at` | `TIMESTAMP` | no |  |  | Momento (UTC) de la ingesta |

### `bronze.musicbrainz_artists`

Un renglon por artista consultado en MusicBrainz, con sus generos (D9).

- **Dueño:** Ricardo Zeron del Valle (Ingesta)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `landing.musicbrainz_cache`
- **Acceso en MinIO:** leen svc-ingestion, svc-transformation, svc-governance; escriben svc-ingestion
- **Llaves únicas:** (artist_name)
- **Reglas:** matched trae mbid

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `artist_name` | `STRING` | no |  |  | Nombre del artista tal como viene en Spotify |
| `status` | `STRING` | no |  | en {matched, no_match} | matched o no_match |
| `mbid` | `STRING` | sí |  |  | Id del artista en MusicBrainz; null si no hubo match confiable |
| `mb_name` | `STRING` | sí |  |  | Nombre en MusicBrainz |
| `match_score` | `BIGINT` | sí |  | [0, 100] | Score de la busqueda (0 a 100) |
| `genres` | `ARRAY<STRUCT<name: STRING, count: BIGINT>>` | sí |  |  | Generos con sus votos; vacio si no hay |
| `raw_json` | `STRING` | sí |  |  | Documento cacheado en landing, tal cual |
| `_source_file` | `STRING` | no |  |  | Llave del documento en landing |
| `fetched_at` | `TIMESTAMP` | no |  |  | Momento (UTC) en que se consulto la API |
| `_ingested_at` | `TIMESTAMP` | no |  |  | Momento (UTC) de la ingesta |


## silver (Plata)

### `silver.plays`

Una fila por reproduccion de musica, sin duplicados ni IP.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `bronze.spotify_streams`
- **Acceso en MinIO:** leen svc-transformation, svc-governance; escriben svc-transformation
- **Llaves únicas:** (play_id); (_file_sha256, _record_index)
- **Reglas:** ts_start <= ts_end; pais conocido y en ISO alfa-2; hora local solo si hay zona; track_uri es de cancion; is_skip solo con motivo de fin

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `play_id` | `STRING` | no |  |  | SHA-256 de (ts_end, uri, ms_played): llave de la reproduccion |
| `ts_start` | `TIMESTAMP` | no |  |  | Inicio (UTC) = ts_end - ms_played |
| `ts_end` | `TIMESTAMP` | no |  |  | Fin (UTC); es el ts de Spotify |
| `ms_played` | `BIGINT` | no |  | >= 0 | Milisegundos reproducidos |
| `track_uri` | `STRING` | no |  |  | URI de la cancion |
| `track_name` | `STRING` | sí |  |  | Nombre de la cancion |
| `artist_name` | `STRING` | sí |  |  | Artista del album |
| `album_name` | `STRING` | sí |  |  | Nombre del album |
| `platform` | `STRING` | sí |  |  | Dispositivo y version tal como viene |
| `platform_family` | `STRING` | sí |  | en {web, smart_device, android, ios, windows, macos, linux, other} | Familia: android, ios, windows, macos, linux, web, smart_device u other |
| `conn_country` | `STRING` | sí |  |  | Pais ISO alfa-2; null si Spotify no lo supo (ZZ) |
| `tz_name` | `STRING` | sí |  |  | Zona horaria IANA usada para la hora local; null si no se pudo saber |
| `tz_source` | `STRING` | sí |  | en {country, session} | country (del pais de la fila) o session (unico pais de su sesion) |
| `local_date` | `DATE` | sí |  |  | Fecha local del inicio |
| `local_hour` | `INT` | sí |  | [0, 23] | Hora local del inicio (0 a 23) |
| `local_weekday` | `INT` | sí |  | [1, 7] | Dia de la semana local (1 lunes a 7 domingo) |
| `reason_start` | `STRING` | sí |  |  | Por que empezo |
| `reason_end` | `STRING` | sí |  |  | Por que termino |
| `shuffle` | `BOOLEAN` | sí |  |  | Modo aleatorio activo |
| `offline` | `BOOLEAN` | sí |  |  | Se reprodujo sin conexion |
| `offline_at` | `TIMESTAMP` | sí |  |  | offline_timestamp normalizado a UTC (H-003) |
| `incognito_mode` | `BOOLEAN` | sí |  |  | Sesion privada |
| `session_id` | `STRING` | no |  |  | Sesion: se corta con mas de 30 min sin escuchar |
| `session_seq` | `INT` | no |  | >= 1 | Posicion dentro de la sesion (desde 1) |
| `_file_sha256` | `STRING` | no |  |  | Archivo de origen en Bronce (linaje) |
| `_record_index` | `BIGINT` | no |  | >= 0 | Registro de origen en Bronce (linaje) |
| `spotify_skipped` | `BOOLEAN` | sí |  |  | Campo skipped de Spotify (se llena poco, H-001) |
| `is_skip` | `BOOLEAN` | sí |  |  | Etiqueta de skip (DATOS_FUENTE 1.3); null si no termino por decision del usuario |

### `silver.episodes`

Una fila por reproduccion de podcast, sin duplicados ni IP.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `bronze.spotify_streams`
- **Acceso en MinIO:** leen svc-transformation, svc-governance; escriben svc-transformation
- **Llaves únicas:** (play_id); (_file_sha256, _record_index)
- **Reglas:** ts_start <= ts_end; pais conocido y en ISO alfa-2; hora local solo si hay zona

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `play_id` | `STRING` | no |  |  | SHA-256 de (ts_end, uri, ms_played): llave de la reproduccion |
| `ts_start` | `TIMESTAMP` | no |  |  | Inicio (UTC) = ts_end - ms_played |
| `ts_end` | `TIMESTAMP` | no |  |  | Fin (UTC); es el ts de Spotify |
| `ms_played` | `BIGINT` | no |  | >= 0 | Milisegundos reproducidos |
| `episode_uri` | `STRING` | no |  |  | URI del episodio |
| `episode_name` | `STRING` | sí |  |  | Nombre del episodio |
| `show_name` | `STRING` | sí |  |  | Nombre del podcast |
| `platform` | `STRING` | sí |  |  | Dispositivo y version tal como viene |
| `platform_family` | `STRING` | sí |  | en {web, smart_device, android, ios, windows, macos, linux, other} | Familia: android, ios, windows, macos, linux, web, smart_device u other |
| `conn_country` | `STRING` | sí |  |  | Pais ISO alfa-2; null si Spotify no lo supo (ZZ) |
| `tz_name` | `STRING` | sí |  |  | Zona horaria IANA usada para la hora local; null si no se pudo saber |
| `tz_source` | `STRING` | sí |  | en {country, session} | country (del pais de la fila) o session (unico pais de su sesion) |
| `local_date` | `DATE` | sí |  |  | Fecha local del inicio |
| `local_hour` | `INT` | sí |  | [0, 23] | Hora local del inicio (0 a 23) |
| `local_weekday` | `INT` | sí |  | [1, 7] | Dia de la semana local (1 lunes a 7 domingo) |
| `reason_start` | `STRING` | sí |  |  | Por que empezo |
| `reason_end` | `STRING` | sí |  |  | Por que termino |
| `shuffle` | `BOOLEAN` | sí |  |  | Modo aleatorio activo |
| `offline` | `BOOLEAN` | sí |  |  | Se reprodujo sin conexion |
| `offline_at` | `TIMESTAMP` | sí |  |  | offline_timestamp normalizado a UTC (H-003) |
| `incognito_mode` | `BOOLEAN` | sí |  |  | Sesion privada |
| `session_id` | `STRING` | no |  |  | Sesion: se corta con mas de 30 min sin escuchar |
| `session_seq` | `INT` | no |  | >= 1 | Posicion dentro de la sesion (desde 1) |
| `_file_sha256` | `STRING` | no |  |  | Archivo de origen en Bronce (linaje) |
| `_record_index` | `BIGINT` | no |  | >= 0 | Registro de origen en Bronce (linaje) |

### `silver._discarded`

Filas de Bronce que no pasan a Plata, con causa (reconciliacion de conteos).

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `bronze.spotify_streams`
- **Acceso en MinIO:** leen svc-transformation, svc-governance; escriben svc-transformation
- **Llaves únicas:** (_file_sha256, _record_index)
- **Reglas:** duplicate_of solo en duplicados

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `_file_sha256` | `STRING` | no |  |  | Archivo de origen en Bronce |
| `_record_index` | `BIGINT` | no |  | >= 0 | Registro de origen en Bronce |
| `cause` | `STRING` | no |  | en {duplicate, no_content, audiobook_not_supported} | duplicate, no_content o audiobook_not_supported |
| `ts_end` | `TIMESTAMP` | no |  |  | ts de la fila descartada |
| `duplicate_of` | `STRING` | sí |  |  | play_id de la fila que se quedo (solo duplicados) |

### `silver.artists`

Un renglon por artista escuchado, con sus generos de MusicBrainz si hubo match confiable.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `silver.plays`, `bronze.musicbrainz_artists`
- **Acceso en MinIO:** leen svc-transformation, svc-governance; escriben svc-transformation
- **Llaves únicas:** (artist_name)
- **Reglas:** generos solo con match; genero principal = primero de la lista

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `artist_name` | `STRING` | no |  |  | Nombre del artista en Spotify |
| `mb_status` | `STRING` | no |  | en {matched, no_match, not_queried} | matched, no_match o not_queried (no se corrio genres) |
| `mbid` | `STRING` | sí |  |  | Id en MusicBrainz |
| `genres` | `ARRAY<STRING>` | sí |  |  | Generos de mas a menos votos; null si no hubo match |
| `primary_genre` | `STRING` | sí |  |  | Genero con mas votos; null si no hay |


## gold (Oro)

### `gold.dim_date`

Calendario continuo entre la primera y la ultima fecha local con reproducciones.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `silver.plays`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (date_key)
- **Reglas:** date_key = AAAAMMDD de date; fin de semana = sabado o domingo

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `date_key` | `INT` | no |  |  | AAAAMMDD |
| `date` | `DATE` | no |  |  | Fecha |
| `year` | `INT` | no |  |  | Anio |
| `quarter` | `INT` | no |  | [1, 4] | Trimestre |
| `month` | `INT` | no |  | [1, 12] | Mes (1 a 12) |
| `month_name` | `STRING` | no |  |  | Nombre del mes |
| `year_month` | `STRING` | no |  |  | AAAA-MM |
| `day` | `INT` | no |  | [1, 31] | Dia del mes |
| `weekday` | `INT` | no |  | [1, 7] | Dia de la semana (1 lunes a 7 domingo) |
| `weekday_name` | `STRING` | no |  |  | Nombre del dia |
| `is_weekend` | `BOOLEAN` | no |  |  | Sabado o domingo |

### `gold.dim_artist`

Artistas con genero principal y lista de generos.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `silver.artists`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (artist_key); (artist_name)
- **Reglas:** artist_key son 16 hex

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `artist_key` | `STRING` | no |  |  | Primeros 16 hex del SHA-256 del nombre |
| `artist_name` | `STRING` | no |  |  | Nombre del artista |
| `mbid` | `STRING` | sí |  |  | Id en MusicBrainz |
| `primary_genre` | `STRING` | sí |  |  | Genero con mas votos; null si no hay |
| `genres` | `ARRAY<STRING>` | sí |  |  | Generos de mas a menos votos |
| `mb_status` | `STRING` | no |  | en {matched, no_match, not_queried} | matched, no_match o not_queried |

### `gold.dim_track`

Canciones; si el nombre cambio en el tiempo se queda el de la reproduccion mas reciente.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `silver.plays`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (track_key)
- **Referencias:** artist_key > `gold.dim_artist.artist_key`

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `track_key` | `STRING` | no |  |  | URI de la cancion |
| `track_name` | `STRING` | sí |  |  | Nombre de la cancion |
| `album_name` | `STRING` | sí |  |  | Nombre del album |
| `artist_key` | `STRING` | sí |  |  | Llave del artista (dim_artist) |

### `gold.dim_platform`

Familias de plataforma y su tipo de dispositivo (catalogo fijo).

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `ext.platform_catalog`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (platform_family)

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `platform_family` | `STRING` | no |  | en {web, smart_device, android, ios, windows, macos, linux, other} | Familia de plataforma |
| `device_type` | `STRING` | no |  | en {mobile, desktop, web, smart_device, other} | mobile, desktop, web, smart_device u other |

### `gold.fact_plays`

Una fila por reproduccion de musica. MERGE incremental por play_id; Z-ORDER por date_key.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `silver.plays`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Partición:** year
- **Llaves únicas:** (play_id)
- **Referencias:** date_key > `gold.dim_date.date_key`; track_key > `gold.dim_track.track_key`; artist_key > `gold.dim_artist.artist_key`; platform_family > `gold.dim_platform.platform_family`
- **Reglas:** ts_start <= ts_end; year = anio de ts_start

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `play_id` | `STRING` | no |  |  | SHA-256 de (ts_end, uri, ms_played): llave de la reproduccion |
| `date_key` | `INT` | sí |  |  | Fecha local como AAAAMMDD (dim_date) |
| `ts_start` | `TIMESTAMP` | no |  |  | Inicio (UTC) |
| `ts_end` | `TIMESTAMP` | no |  |  | Fin (UTC) |
| `local_hour` | `INT` | sí |  | [0, 23] | Hora local del inicio |
| `local_weekday` | `INT` | sí |  | [1, 7] | Dia de la semana local (1 lunes a 7 domingo) |
| `track_key` | `STRING` | no |  |  | Cancion (dim_track) |
| `artist_key` | `STRING` | sí |  |  | Llave del artista (dim_artist) |
| `platform_family` | `STRING` | sí |  | en {web, smart_device, android, ios, windows, macos, linux, other} | Plataforma (dim_platform) |
| `conn_country` | `STRING` | sí |  |  | Pais ISO alfa-2 |
| `session_id` | `STRING` | no |  |  | Sesion de escucha |
| `session_seq` | `INT` | no |  | >= 1 | Posicion dentro de la sesion |
| `ms_played` | `BIGINT` | no |  | >= 0 | Milisegundos reproducidos |
| `minutes_played` | `DOUBLE` | no |  | >= 0 | Minutos reproducidos |
| `is_skip` | `BOOLEAN` | sí |  |  | Etiqueta de skip; null si no aplica |
| `reason_start` | `STRING` | sí |  |  | Por que empezo |
| `reason_end` | `STRING` | sí |  |  | Por que termino |
| `shuffle` | `BOOLEAN` | sí |  |  | Modo aleatorio |
| `offline` | `BOOLEAN` | sí |  |  | Sin conexion |
| `incognito_mode` | `BOOLEAN` | sí |  |  | Sesion privada |
| `year` | `INT` | no |  |  | Anio UTC de ts_start (particion) |

### `gold.kpi_listening_daily`

Escucha por dia local: volumen, variedad, sesiones y tasa de skip.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `gold.fact_plays`, `gold.dim_date`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (date_key)
- **Referencias:** date_key > `gold.dim_date.date_key`
- **Reglas:** skips <= etiquetadas <= plays; skip_rate null solo sin etiquetadas

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `date_key` | `INT` | no |  |  | AAAAMMDD |
| `date` | `DATE` | no |  |  | Fecha local |
| `plays` | `BIGINT` | no |  | >= 1 | Reproducciones |
| `minutes_played` | `DOUBLE` | no |  | >= 0 | Minutos escuchados |
| `distinct_tracks` | `BIGINT` | no |  | >= 1 | Canciones distintas |
| `distinct_artists` | `BIGINT` | no |  | >= 0 | Artistas distintos |
| `sessions` | `BIGINT` | no |  | >= 1 | Sesiones distintas |
| `labeled_plays` | `BIGINT` | no |  | >= 0 | Reproducciones con etiqueta de skip |
| `skips` | `BIGINT` | no |  | >= 0 | Reproducciones con is_skip = true |
| `skip_rate` | `DOUBLE` | sí |  | [0, 1] | skips / labeled_plays; null si no hay etiquetadas |

### `gold.kpi_top_artists_monthly`

Artistas por mes con su ranking por minutos escuchados.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `gold.fact_plays`, `gold.dim_artist`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (year_month, artist_key); (year_month, rank_in_month)
- **Referencias:** artist_key > `gold.dim_artist.artist_key`
- **Reglas:** year_month = mes de month_start

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `year_month` | `STRING` | no |  |  | Mes local AAAA-MM |
| `month_start` | `DATE` | no |  |  | Primer dia del mes |
| `artist_key` | `STRING` | no |  |  | Artista |
| `artist_name` | `STRING` | sí |  |  | Nombre del artista |
| `primary_genre` | `STRING` | sí |  |  | Genero principal |
| `plays` | `BIGINT` | no |  | >= 1 | Reproducciones |
| `minutes_played` | `DOUBLE` | no |  | >= 0 | Minutos |
| `rank_in_month` | `INT` | no |  | >= 1 | 1 = el mas escuchado del mes |

### `gold.kpi_genres_monthly`

Generos (principal del artista) por mes; genre null = artista sin genero conocido.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `gold.fact_plays`, `gold.dim_artist`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (year_month, genre)
- **Reglas:** year_month = mes de month_start

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `year_month` | `STRING` | no |  |  | Mes local AAAA-MM |
| `month_start` | `DATE` | no |  |  | Primer dia del mes |
| `genre` | `STRING` | sí |  |  | Genero principal; null si no se conoce |
| `plays` | `BIGINT` | no |  | >= 1 | Reproducciones |
| `minutes_played` | `DOUBLE` | no |  | >= 0 | Minutos |
| `artists` | `BIGINT` | no |  | >= 0 | Artistas distintos |
| `minutes_share` | `DOUBLE` | sí |  | [0, 1] | Fraccion de los minutos del mes; null si el mes suma 0 minutos |

### `gold.kpi_artist_retention`

Cohortes por mes de descubrimiento: que parte de los artistas se escucha N meses despues.

- **Dueño:** Juan Pablo Ramirez Carlon (Transformacion)
- **Clasificación:** interno: Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).
- **Se alimenta de:** `gold.fact_plays`
- **Acceso en MinIO:** leen svc-transformation, svc-governance, svc-ml, svc-bi; escriben svc-transformation
- **Llaves únicas:** (cohort_month, months_since)
- **Reglas:** activos <= tamano de cohorte; mes 0 retiene al 100%

| Columna | Tipo | Nulos | Privacidad | Reglas | Descripción |
|---|---|---|---|---|---|
| `cohort_month` | `STRING` | no |  |  | Mes (AAAA-MM) en que se escucho al artista por primera vez |
| `cohort_start` | `DATE` | no |  |  | Primer dia del mes de la cohorte |
| `months_since` | `INT` | no |  | >= 0 | Meses desde el descubrimiento (0 = mismo mes) |
| `cohort_size` | `BIGINT` | no |  | >= 1 | Artistas descubiertos ese mes |
| `artists_active` | `BIGINT` | no |  | >= 0 | De esos, cuantos se escucharon en el mes N |
| `retention_rate` | `DOUBLE` | no |  | [0, 1] | artists_active / cohort_size |
