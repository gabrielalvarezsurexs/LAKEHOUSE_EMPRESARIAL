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
| `offline_timestamp` | int (epoch) | 13,778 / 0 | Null cuando no aplica |
| `incognito_mode` | bool | 0 / 0 | |

Distintos (archivo 0): 3,539 canciones, 1,283 artistas, 3 países, 66 plataformas.

### 1.3 Reglas que salen de este perfil (para Plata)

- **Música vs podcast:** fila de música = `spotify_track_uri` no nulo. Los episodios van a su propia tabla; las
  filas sin track ni episodio se cuentan y se descartan con causa.
- **Llave de deduplicación:** `(ts, spotify_track_uri o spotify_episode_uri, ms_played)`. Se mide cuántos duplicados
  hay antes de fijarla.
- **Etiqueta de skip (para ML):** propuesta a confirmar con conteos en F3: `is_skip = reason_end in ('fwdbtn',
  'backbtn') or (ms_played < 30000 and reason_end not in ('logout', 'unexpected-exit-while-paused', 'unknown'))`.
  Las filas cuya terminación no fue decisión del usuario quedan con etiqueta `null` (mejor vacío que falso).
- **Hora local:** `ts` es UTC; la hora local se deriva por `conn_country` con un catálogo explícito de zonas. País
  sin zona en el catálogo: hora local `null` y se cuenta.
- **Sesiones:** una sesión se corta con un hueco de más de 30 min entre reproducciones.

## 2. Géneros (enriquecimiento)

El historial no trae géneros. Se agregan por artista con **MusicBrainz** (gratis, sin llave, `inc=genres`):

- Búsqueda por nombre de artista, se toma el candidato solo si el score es alto y el nombre coincide normalizado;
  si hay duda, sin género (no se inventa). La cobertura se reporta (artistas y reproducciones con género).
- Límite: ~1 petición por segundo y User-Agent obligatorio (genérico, configurable en `.env`, sin datos
  personales). Para ~1,300 artistas son ~25 min **una sola vez**: las respuestas crudas se guardan en landing y
  las corridas siguientes solo piden los artistas nuevos.
- **Spotify Web API descartada como fuente principal:** desde febrero de 2026 el modo desarrollo exige Premium,
  limita usuarios y quitó `popularity` y `followers` de artistas; `genres` no está quitado oficialmente pero se
  reporta vacío o en desuso. Queda como plan B documentado, no se implementa salvo que MusicBrainz cubra muy poco.

## 3. Datos sintéticos (para réplica y CI)

Generador con **el mismo esquema exacto** de la §1.2 y distribuciones parecidas, sin datos reales. Sirve para que
el profe y CI corran el pipeline completo sin nuestros datos. Canciones y artistas inventados; los géneros
sintéticos se generan junto con ellos (CI no llama a MusicBrainz).
