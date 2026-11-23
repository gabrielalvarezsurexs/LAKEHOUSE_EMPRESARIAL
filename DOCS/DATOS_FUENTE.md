# Datos fuente

## 1. Historial extendido de Spotify (fuente principal)

- **Dónde:** `DATOS_SPOTIFY/` en la raíz (gitignored, nunca al repo). Es el *Extended Streaming History* de un
  integrante, pedido desde la página de privacidad de Spotify.
- **Aviso:** es ~99% seguro que estos son los datos finales. Si llegaran a cambiar, el pipeline no se toca: se
  reemplazan los JSON en `DATOS_SPOTIFY/` y se re-corre (la ingesta es idempotente por hash de archivo). Revisar que
  el perfil de la §1.2 siga cuadrando y actualizar esta sección.

### 1.1 Archivos (perfil del 2026-11-22, solo conteos)

| Archivo | Filas | Rango de `ts` |
|---|---|---|
| `Streaming_History_Audio_2016-2024_0.json` | 15,558 | 2016-01 a 2024-09 |
| `Streaming_History_Audio_2024-2026_1.json` | 185 | 2024-09 a 2026-03 |
| **Total** | **15,743** | 2016-01 a 2026-03 |

Cada archivo es un arreglo JSON de objetos planos (un objeto = una reproducción). Un solo usuario.

### 1.2 Esquema (23 campos, todos presentes en todas las filas)

| Campo | Tipo | Nulos (archivo 0 / 1) | Notas |
|---|---|---|---|
| `ts` | string ISO-8601 UTC (`...Z`) | 0 / 0 | Momento en que **terminó** la reproducción |
| `platform` | string | 0 / 0 | 66 valores distintos (traen modelo y versión del dispositivo); se normaliza a familia en Plata |
| `ms_played` | int | 0 / 0 | Milisegundos reproducidos |
| `conn_country` | string (ISO 3166-1 alfa-2) | 0 / 0 | 3 países distintos |
| `ip_addr` | string | 0 / 0 | **Dato personal.** Hash con sal en Bronce, se elimina en Plata |
| `master_metadata_track_name` | string | 88 / 2 | Null en podcasts y en reproducciones sin metadatos |
| `master_metadata_album_artist_name` | string | 88 / 2 | |
| `master_metadata_album_album_name` | string | 88 / 2 | |
| `spotify_track_uri` | string `spotify:track:<id>` | 88 / 2 | 3,539 distintos (archivo 0) |
| `episode_name`, `episode_show_name`, `spotify_episode_uri` | string | 15,508 / 183 | 50 + 2 episodios de podcast |
| `audiobook_title`, `audiobook_uri`, `audiobook_chapter_uri`, `audiobook_chapter_title` | string | todos nulos | Se conservan en Bronce, se descartan en Plata |
| `reason_start` | string | 0 / 0 | `trackdone`, `clickrow`, `fwdbtn`, `playbtn`, `appload`, `unknown`, `cosmos`, `trackerror`, ... |
| `reason_end` | string | 0 / 0 | `trackdone`, `endplay`, `fwdbtn`, `logout`, `playbtn`, `unknown`, `unexpected-exit-while-paused`, `cosmos`, ... |
| `shuffle` | bool | 0 / 0 | |
| `skipped` | bool | 0 / 0 | Solo 461 + 36 en `true`: Spotify lo llena poco; no sirve solo como etiqueta |
| `offline` | bool | 0 / 0 | |
| `offline_timestamp` | int (epoch) | 13,778 / 0 | Null cuando no aplica. Unidades mezcladas: 1,940 en segundos y 25 en milisegundos (H-003) |
| `incognito_mode` | bool | 0 / 0 | |

Distintos (archivo 0): 3,539 canciones, 1,283 artistas, 3 países, 66 plataformas. En los dos archivos: 1,301
artistas; 90 filas sin canción, de las cuales 52 son episodios y 38 no traen ni canción ni episodio; IPs: 15,526
IPv4 y 217 IPv6.

Validación de F2 (2026-11-22): las 15,743 filas cumplen el contrato (`lakehouse.governance.contracts`), 0 a
cuarentena y 0 campos fuera del contrato. `ts` siempre viene como `AAAA-MM-DDTHH:MM:SSZ`.

### 1.3 Reglas de Plata (medidas en F3, 2026-11-23; código en `transformation/catalogs.py`)

- **Música vs podcast:** fila de música = `spotify_track_uri` no nulo (15,653); episodios a su tabla (52); las 38
  sin canción ni episodio van a `silver._discarded` con causa `no_content` (audiolibros: `audiobook_not_supported`).
- **Llave de deduplicación:** `(ts, uri de canción o episodio, ms_played)`. Medido: 27 filas extra (22 son filas
  idénticas; 5 difieren en otros campos). Con `(ts, uri)` serían 35: las 8 de más tienen otro `ms_played` y se
  conservan (pueden ser eventos distintos). Gana la primera por archivo y posición; la otra va a `_discarded`.
- **Etiqueta de skip (para ML):** conteos de `reason_end` en música: `trackdone` 10,380, `endplay` 1,851, `fwdbtn`
  1,534, `logout` 587, `playbtn` 400, `unknown` 349, `unexpected-exit-while-paused` 298, `cosmos` 122,
  `unexpected-exit` 35, `remote` 34, `backbtn` 32, vacío 19, `trackerror` 12. Regla final:
  - `fwdbtn`, `backbtn`: skip (incluye 316 `fwdbtn` de 30 s o más: el usuario saltó igual).
  - `trackdone`: no skip (509 duran menos de 30 s: canciones cortas que terminaron solas).
  - `endplay`, `playbtn`: skip si `ms_played < 30000`.
  - Cualquier otro (logout, unknown, unexpected-exit*, remote, trackerror, cosmos, vacío): `null`.
  - Resultado en Plata: 14,174 etiquetadas (90.7%), 2,557 skips (18.0%).
- **Vacíos:** strings vacíos o de puros espacios pasan a `null` (19 `reason_start` y 19 `reason_end`).
- **Hora local:** `ts` es UTC y marca el **fin**; el inicio es `ts - ms_played` y la hora local sale del inicio con
  la zona del país (catálogo en `catalogs.py`; países con varias zonas usan la de la capital o la más poblada).
  `conn_country = ZZ` (desconocido, 268 filas de contenido) se guarda como `null`; si su sesión tiene un solo país
  conocido se usa ese (`tz_source = session`, 255 filas). Quedan 10 reproducciones sin hora local.
- **Plataforma:** 66 valores a familias (`web`, `smart_device`, `android`, `ios`, `windows`, `macos`, `linux`,
  `other`); en los reales todas caen en una familia (0 `other`).
- **Sesiones:** se corta con un hueco de más de 30 min entre el inicio y el fin más tardío anterior: 1,619.
- **`offline_timestamp`** (H-003): 10 dígitos = segundos, 13 = milisegundos, otro = `null`; queda en `offline_at`.
- **Géneros:** 678 de 1,301 artistas tienen género (52.1%), 47.9% de las reproducciones (H-004).

## 2. Géneros (enriquecimiento)

El historial no trae géneros. Se agregan por artista con **MusicBrainz** (gratis, sin llave, `inc=genres`):

- Búsqueda por nombre de artista, se toma el candidato solo si el score es alto y el nombre coincide normalizado;
  si hay duda, sin género (no se inventa). La cobertura se reporta (artistas y reproducciones con género).
- Límite: ~1 petición por segundo y User-Agent obligatorio (genérico, configurable en `.env`, sin datos
  personales). Son 2 peticiones por artista con match (búsqueda + géneros) y 1 sin match: para 1,301 artistas,
  **~55 min una sola vez** (medido el 2026-11-23: la API responde 503 seguido y el cliente reintenta). Resultado:
  1,099 de 1,301 artistas con match confiable (84%). Las respuestas crudas se guardan en
  `landing/musicbrainz/api/` y las corridas siguientes solo piden los artistas nuevos (si se corta, se re-corre y sigue donde iba).
- **Spotify Web API descartada como fuente principal:** desde febrero de 2026 el modo desarrollo exige Premium,
  limita usuarios y quitó `popularity` y `followers` de artistas; `genres` no está quitado oficialmente pero se
  reporta vacío o en desuso. Queda como plan B documentado, no se implementa salvo que MusicBrainz cubra muy poco.

## 3. Datos sintéticos (para réplica y CI)

Generador con **el mismo esquema exacto** de la §1.2 y distribuciones parecidas, sin datos reales. Sirve para que
el profe y CI corran el pipeline completo sin nuestros datos. Canciones y artistas inventados; los géneros
sintéticos se generan junto con ellos (CI no llama a MusicBrainz).

- Comando: `python -m lakehouse.ingestion synthetic --out <carpeta> --rows 5000 --seed 42` (corre también local
  con `uv run`). Determinista por semilla; IPs de los rangos de documentación (RFC 5737/3849).
- Los archivos llevan `SINTETICO` en el nombre y la carga a landing **se niega a mezclarlos con reales**: para
  cambiar de datos se borra el volumen (`docker compose down -v`).
