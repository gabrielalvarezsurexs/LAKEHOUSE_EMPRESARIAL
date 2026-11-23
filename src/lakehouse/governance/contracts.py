"""Contratos de esquema: la unica definicion de columnas de cada tabla (tipo, nulos, descripcion, PII).

Los demas modulos importan de aqui en vez de repetir listas de columnas. Cubre Bronce (F2), Plata y Oro (F3);
F4 genera el catalogo y los checks de calidad a partir de estos contratos.
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.common.errors import SchemaError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

INGESTION_OWNER = "Ricardo Zeron del Valle (Ingesta)"
TRANSFORMATION_OWNER = "Juan Pablo Ramirez Carlon (Transformacion)"


@dataclass(frozen=True)
class Column:
    """Una columna: dtype es un tipo de Spark en DDL (STRING, BIGINT, BOOLEAN, TIMESTAMP, ...)."""

    name: str
    dtype: str
    description: str
    nullable: bool = True
    pii: bool = False


@dataclass(frozen=True)
class TableContract:
    """Esquema y metadatos de una tabla; layer es la capa (bucket) donde vive."""

    name: str
    layer: str
    owner: str
    description: str
    columns: tuple[Column, ...]
    partition_by: tuple[str, ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"La tabla {self.name} no tiene la columna {name}")

    def ddl(self) -> str:
        """Esquema en DDL de Spark, p. ej. '`ts` TIMESTAMP NOT NULL, `platform` STRING'."""
        return ", ".join(f"`{c.name}` {c.dtype}{'' if c.nullable else ' NOT NULL'}" for c in self.columns)

    def spark_schema(self) -> StructType:
        """StructType de Spark (necesita una SparkSession activa)."""
        from pyspark.sql.types import _parse_datatype_string

        return _parse_datatype_string(self.ddl())

    def conform(self, df: DataFrame) -> DataFrame:
        """Columnas del contrato, en su orden y con su tipo; falla si al DataFrame le falta alguna."""
        from pyspark.sql import functions as F

        missing = [n for n in self.column_names if n not in df.columns]
        if missing:
            raise SchemaError(f"A {self.layer}.{self.name} le faltan columnas: {', '.join(missing)}")
        return df.select([F.col(c.name).cast(c.dtype).alias(c.name) for c in self.columns])


def _c(name: str, dtype: str, description: str, nullable: bool = True, pii: bool = False) -> Column:
    return Column(name, dtype, description, nullable, pii)


# --- Landing: un objeto JSON por reproduccion (historial extendido de Spotify, DATOS_FUENTE 1.2) ---
# Solo ts y ms_played son obligatorios: sin ellos la reproduccion no significa nada. Lo demas puede faltar
# y queda null (mejor vacio que falso).
SPOTIFY_STREAMS_SOURCE = TableContract(
    name="spotify_streams_source",
    layer="landing",
    owner=INGESTION_OWNER,
    description="Historial extendido de Spotify tal como lo entrega la plataforma (un arreglo JSON).",
    columns=(
        _c("ts", "TIMESTAMP", "Momento (UTC) en que termino la reproduccion", nullable=False),
        _c("platform", "STRING", "Dispositivo y version de la app"),
        _c("ms_played", "BIGINT", "Milisegundos reproducidos", nullable=False),
        _c("conn_country", "STRING", "Pais de la conexion (ISO 3166-1 alfa-2)"),
        _c("ip_addr", "STRING", "IP de la conexion (dato personal)", pii=True),
        _c("master_metadata_track_name", "STRING", "Nombre de la cancion"),
        _c("master_metadata_album_artist_name", "STRING", "Artista del album"),
        _c("master_metadata_album_album_name", "STRING", "Nombre del album"),
        _c("spotify_track_uri", "STRING", "URI de la cancion (spotify:track:<id>)"),
        _c("episode_name", "STRING", "Nombre del episodio de podcast"),
        _c("episode_show_name", "STRING", "Nombre del podcast"),
        _c("spotify_episode_uri", "STRING", "URI del episodio"),
        _c("audiobook_title", "STRING", "Titulo del audiolibro"),
        _c("audiobook_uri", "STRING", "URI del audiolibro"),
        _c("audiobook_chapter_uri", "STRING", "URI del capitulo"),
        _c("audiobook_chapter_title", "STRING", "Titulo del capitulo"),
        _c("reason_start", "STRING", "Por que empezo la reproduccion"),
        _c("reason_end", "STRING", "Por que termino la reproduccion"),
        _c("shuffle", "BOOLEAN", "Modo aleatorio activo"),
        _c("skipped", "BOOLEAN", "Spotify la marco como saltada (se llena poco)"),
        _c("offline", "BOOLEAN", "Se reprodujo sin conexion"),
        _c("offline_timestamp", "BIGINT", "Epoch de la reproduccion offline (segundos o milisegundos)"),
        _c("incognito_mode", "BOOLEAN", "Sesion privada"),
    ),
)

# Metadatos que la ingesta agrega a cada renglon de Bronce (D15).
INGESTION_METADATA = (
    _c("_source_file", "STRING", "Llave del archivo en landing", nullable=False),
    _c("_file_sha256", "STRING", "SHA-256 del archivo (idempotencia)", nullable=False),
    _c("_record_index", "BIGINT", "Posicion del registro dentro del archivo", nullable=False),
    _c("_batch_id", "STRING", "Corrida de ingesta", nullable=False),
    _c("_ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
)

_IP_HASH = _c("ip_addr_hash", "STRING", "HMAC-SHA256 de ip_addr con la sal IP_HASH_SALT (seudonimo, D8)")

BRONZE_SPOTIFY_STREAMS = TableContract(
    name="spotify_streams",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Una fila por reproduccion valida; ip_addr llega seudonimizada.",
    columns=tuple(_IP_HASH if c.name == "ip_addr" else c for c in SPOTIFY_STREAMS_SOURCE.columns)
    + INGESTION_METADATA,
)

BRONZE_QUARANTINE = TableContract(
    name="_quarantine",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Registros que no cumplieron el contrato, con su causa (D16). La IP va seudonimizada.",
    columns=(
        _c("_source_file", "STRING", "Llave del archivo en landing", nullable=False),
        _c("_file_sha256", "STRING", "SHA-256 del archivo", nullable=False),
        _c("_record_index", "BIGINT", "Posicion en el archivo; null si fallo el archivo completo"),
        _c("cause", "STRING", "Motivo del rechazo, p. ej. bad_type:ms_played", nullable=False),
        _c("raw_record", "STRING", "Registro original en JSON (ip_addr reemplazada por su hash)"),
        _c("_batch_id", "STRING", "Corrida de ingesta", nullable=False),
        _c("_ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
    ),
)

BRONZE_INGESTION_LOG = TableContract(
    name="_ingestion_log",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Bitacora de ingesta: un renglon por archivo procesado (D15).",
    columns=(
        _c("file_sha256", "STRING", "SHA-256 del archivo", nullable=False),
        _c("source_file", "STRING", "Llave del archivo en landing", nullable=False),
        _c("batch_id", "STRING", "Corrida de ingesta", nullable=False),
        _c("rows_read", "BIGINT", "Registros leidos del archivo", nullable=False),
        _c("rows_loaded", "BIGINT", "Registros escritos en spotify_streams", nullable=False),
        _c("rows_quarantined", "BIGINT", "Registros enviados a cuarentena", nullable=False),
        _c("unknown_fields", "STRING", "Campos que no estan en el contrato (deriva de esquema)"),
        _c("ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
    ),
)

BRONZE_MUSICBRAINZ_ARTISTS = TableContract(
    name="musicbrainz_artists",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Un renglon por artista consultado en MusicBrainz, con sus generos (D9).",
    columns=(
        _c("artist_name", "STRING", "Nombre del artista tal como viene en Spotify", nullable=False),
        _c("status", "STRING", "matched o no_match", nullable=False),
        _c("mbid", "STRING", "Id del artista en MusicBrainz; null si no hubo match confiable"),
        _c("mb_name", "STRING", "Nombre en MusicBrainz"),
        _c("match_score", "BIGINT", "Score de la busqueda (0 a 100)"),
        _c("genres", "ARRAY<STRUCT<name: STRING, count: BIGINT>>", "Generos con sus votos; vacio si no hay"),
        _c("raw_json", "STRING", "Documento cacheado en landing, tal cual"),
        _c("_source_file", "STRING", "Llave del documento en landing", nullable=False),
        _c("fetched_at", "TIMESTAMP", "Momento (UTC) en que se consulto la API", nullable=False),
        _c("_ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
    ),
)

# --- Plata: limpio, sin duplicados, sin IP (D8), con hora local, sesiones y etiqueta de skip (F3) ---
# Contexto comun a canciones y episodios: tiempo, dispositivo, pais, hora local, motivos, sesion y linaje.
_PLAY_ID = _c(
    "play_id", "STRING", "SHA-256 de (ts_end, uri, ms_played): llave de la reproduccion", nullable=False
)
_PLAY_TIME = (
    _c("ts_start", "TIMESTAMP", "Inicio (UTC) = ts_end - ms_played", nullable=False),
    _c("ts_end", "TIMESTAMP", "Fin (UTC); es el ts de Spotify", nullable=False),
    _c("ms_played", "BIGINT", "Milisegundos reproducidos", nullable=False),
)
_PLAY_CONTEXT = (
    _c("platform", "STRING", "Dispositivo y version tal como viene"),
    _c(
        "platform_family", "STRING", "Familia: android, ios, windows, macos, linux, web, smart_device u other"
    ),
    _c("conn_country", "STRING", "Pais ISO alfa-2; null si Spotify no lo supo (ZZ)"),
    _c("tz_name", "STRING", "Zona horaria IANA usada para la hora local; null si no se pudo saber"),
    _c("tz_source", "STRING", "country (del pais de la fila) o session (unico pais de su sesion)"),
    _c("local_date", "DATE", "Fecha local del inicio"),
    _c("local_hour", "INT", "Hora local del inicio (0 a 23)"),
    _c("local_weekday", "INT", "Dia de la semana local (1 lunes a 7 domingo)"),
    _c("reason_start", "STRING", "Por que empezo"),
    _c("reason_end", "STRING", "Por que termino"),
    _c("shuffle", "BOOLEAN", "Modo aleatorio activo"),
    _c("offline", "BOOLEAN", "Se reprodujo sin conexion"),
    _c("offline_at", "TIMESTAMP", "offline_timestamp normalizado a UTC (H-003)"),
    _c("incognito_mode", "BOOLEAN", "Sesion privada"),
    _c("session_id", "STRING", "Sesion: se corta con mas de 30 min sin escuchar", nullable=False),
    _c("session_seq", "INT", "Posicion dentro de la sesion (desde 1)", nullable=False),
    _c("_file_sha256", "STRING", "Archivo de origen en Bronce (linaje)", nullable=False),
    _c("_record_index", "BIGINT", "Registro de origen en Bronce (linaje)", nullable=False),
)

SILVER_PLAYS = TableContract(
    name="plays",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Una fila por reproduccion de musica, sin duplicados ni IP.",
    columns=(
        _PLAY_ID,
        *_PLAY_TIME,
        _c("track_uri", "STRING", "URI de la cancion", nullable=False),
        _c("track_name", "STRING", "Nombre de la cancion"),
        _c("artist_name", "STRING", "Artista del album"),
        _c("album_name", "STRING", "Nombre del album"),
        *_PLAY_CONTEXT,
        _c("spotify_skipped", "BOOLEAN", "Campo skipped de Spotify (se llena poco, H-001)"),
        _c(
            "is_skip",
            "BOOLEAN",
            "Etiqueta de skip (DATOS_FUENTE 1.3); null si no termino por decision del usuario",
        ),
    ),
)

SILVER_EPISODES = TableContract(
    name="episodes",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Una fila por reproduccion de podcast, sin duplicados ni IP.",
    columns=(
        _PLAY_ID,
        *_PLAY_TIME,
        _c("episode_uri", "STRING", "URI del episodio", nullable=False),
        _c("episode_name", "STRING", "Nombre del episodio"),
        _c("show_name", "STRING", "Nombre del podcast"),
        *_PLAY_CONTEXT,
    ),
)

SILVER_DISCARDED = TableContract(
    name="_discarded",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Filas de Bronce que no pasan a Plata, con causa (reconciliacion de conteos).",
    columns=(
        _c("_file_sha256", "STRING", "Archivo de origen en Bronce", nullable=False),
        _c("_record_index", "BIGINT", "Registro de origen en Bronce", nullable=False),
        _c("cause", "STRING", "duplicate, no_content o audiobook_not_supported", nullable=False),
        _c("ts_end", "TIMESTAMP", "ts de la fila descartada", nullable=False),
        _c("duplicate_of", "STRING", "play_id de la fila que se quedo (solo duplicados)"),
    ),
)

SILVER_ARTISTS = TableContract(
    name="artists",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Un renglon por artista escuchado, con sus generos de MusicBrainz si hubo match confiable.",
    columns=(
        _c("artist_name", "STRING", "Nombre del artista en Spotify", nullable=False),
        _c("mb_status", "STRING", "matched, no_match o not_queried (no se corrio genres)", nullable=False),
        _c("mbid", "STRING", "Id en MusicBrainz"),
        _c("genres", "ARRAY<STRING>", "Generos de mas a menos votos; null si no hubo match"),
        _c("primary_genre", "STRING", "Genero con mas votos; null si no hay"),
    ),
)

# --- Oro: modelo estrella (D10) y marts de KPIs que consume BI; ML lee fact_plays ---
_DATE_KEY = _c("date_key", "INT", "Fecha local como AAAAMMDD (dim_date)")
_ARTIST_KEY = _c("artist_key", "STRING", "Llave del artista (dim_artist)")
_MONTH = (
    _c("year_month", "STRING", "Mes local AAAA-MM", nullable=False),
    _c("month_start", "DATE", "Primer dia del mes", nullable=False),
)

GOLD_DIM_DATE = TableContract(
    name="dim_date",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Calendario continuo entre la primera y la ultima fecha local con reproducciones.",
    columns=(
        _c("date_key", "INT", "AAAAMMDD", nullable=False),
        _c("date", "DATE", "Fecha", nullable=False),
        _c("year", "INT", "Anio", nullable=False),
        _c("quarter", "INT", "Trimestre", nullable=False),
        _c("month", "INT", "Mes (1 a 12)", nullable=False),
        _c("month_name", "STRING", "Nombre del mes", nullable=False),
        _c("year_month", "STRING", "AAAA-MM", nullable=False),
        _c("day", "INT", "Dia del mes", nullable=False),
        _c("weekday", "INT", "Dia de la semana (1 lunes a 7 domingo)", nullable=False),
        _c("weekday_name", "STRING", "Nombre del dia", nullable=False),
        _c("is_weekend", "BOOLEAN", "Sabado o domingo", nullable=False),
    ),
)

GOLD_DIM_ARTIST = TableContract(
    name="dim_artist",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Artistas con genero principal y lista de generos.",
    columns=(
        _c("artist_key", "STRING", "Primeros 16 hex del SHA-256 del nombre", nullable=False),
        _c("artist_name", "STRING", "Nombre del artista", nullable=False),
        _c("mbid", "STRING", "Id en MusicBrainz"),
        _c("primary_genre", "STRING", "Genero con mas votos; null si no hay"),
        _c("genres", "ARRAY<STRING>", "Generos de mas a menos votos"),
        _c("mb_status", "STRING", "matched, no_match o not_queried", nullable=False),
    ),
)

GOLD_DIM_TRACK = TableContract(
    name="dim_track",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Canciones; si el nombre cambio en el tiempo se queda el de la reproduccion mas reciente.",
    columns=(
        _c("track_key", "STRING", "URI de la cancion", nullable=False),
        _c("track_name", "STRING", "Nombre de la cancion"),
        _c("album_name", "STRING", "Nombre del album"),
        _ARTIST_KEY,
    ),
)

GOLD_DIM_PLATFORM = TableContract(
    name="dim_platform",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Familias de plataforma y su tipo de dispositivo (catalogo fijo).",
    columns=(
        _c("platform_family", "STRING", "Familia de plataforma", nullable=False),
        _c("device_type", "STRING", "mobile, desktop, web, smart_device u other", nullable=False),
    ),
)

GOLD_FACT_PLAYS = TableContract(
    name="fact_plays",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Una fila por reproduccion de musica. MERGE incremental por play_id; Z-ORDER por date_key.",
    columns=(
        _PLAY_ID,
        _DATE_KEY,
        _c("ts_start", "TIMESTAMP", "Inicio (UTC)", nullable=False),
        _c("ts_end", "TIMESTAMP", "Fin (UTC)", nullable=False),
        _c("local_hour", "INT", "Hora local del inicio"),
        _c("local_weekday", "INT", "Dia de la semana local (1 lunes a 7 domingo)"),
        _c("track_key", "STRING", "Cancion (dim_track)", nullable=False),
        _ARTIST_KEY,
        _c("platform_family", "STRING", "Plataforma (dim_platform)"),
        _c("conn_country", "STRING", "Pais ISO alfa-2"),
        _c("session_id", "STRING", "Sesion de escucha", nullable=False),
        _c("session_seq", "INT", "Posicion dentro de la sesion", nullable=False),
        _c("ms_played", "BIGINT", "Milisegundos reproducidos", nullable=False),
        _c("minutes_played", "DOUBLE", "Minutos reproducidos", nullable=False),
        _c("is_skip", "BOOLEAN", "Etiqueta de skip; null si no aplica"),
        _c("reason_start", "STRING", "Por que empezo"),
        _c("reason_end", "STRING", "Por que termino"),
        _c("shuffle", "BOOLEAN", "Modo aleatorio"),
        _c("offline", "BOOLEAN", "Sin conexion"),
        _c("incognito_mode", "BOOLEAN", "Sesion privada"),
        _c("year", "INT", "Anio UTC de ts_start (particion)", nullable=False),
    ),
    partition_by=("year",),
)

GOLD_KPI_LISTENING_DAILY = TableContract(
    name="kpi_listening_daily",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Escucha por dia local: volumen, variedad, sesiones y tasa de skip.",
    columns=(
        _c("date_key", "INT", "AAAAMMDD", nullable=False),
        _c("date", "DATE", "Fecha local", nullable=False),
        _c("plays", "BIGINT", "Reproducciones", nullable=False),
        _c("minutes_played", "DOUBLE", "Minutos escuchados", nullable=False),
        _c("distinct_tracks", "BIGINT", "Canciones distintas", nullable=False),
        _c("distinct_artists", "BIGINT", "Artistas distintos", nullable=False),
        _c("sessions", "BIGINT", "Sesiones distintas", nullable=False),
        _c("labeled_plays", "BIGINT", "Reproducciones con etiqueta de skip", nullable=False),
        _c("skips", "BIGINT", "Reproducciones con is_skip = true", nullable=False),
        _c("skip_rate", "DOUBLE", "skips / labeled_plays; null si no hay etiquetadas"),
    ),
)

GOLD_KPI_TOP_ARTISTS_MONTHLY = TableContract(
    name="kpi_top_artists_monthly",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Artistas por mes con su ranking por minutos escuchados.",
    columns=(
        *_MONTH,
        _c("artist_key", "STRING", "Artista", nullable=False),
        _c("artist_name", "STRING", "Nombre del artista"),
        _c("primary_genre", "STRING", "Genero principal"),
        _c("plays", "BIGINT", "Reproducciones", nullable=False),
        _c("minutes_played", "DOUBLE", "Minutos", nullable=False),
        _c("rank_in_month", "INT", "1 = el mas escuchado del mes", nullable=False),
    ),
)

GOLD_KPI_GENRES_MONTHLY = TableContract(
    name="kpi_genres_monthly",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Generos (principal del artista) por mes; genre null = artista sin genero conocido.",
    columns=(
        *_MONTH,
        _c("genre", "STRING", "Genero principal; null si no se conoce"),
        _c("plays", "BIGINT", "Reproducciones", nullable=False),
        _c("minutes_played", "DOUBLE", "Minutos", nullable=False),
        _c("artists", "BIGINT", "Artistas distintos", nullable=False),
        _c("minutes_share", "DOUBLE", "Fraccion de los minutos del mes; null si el mes suma 0 minutos"),
    ),
)

GOLD_KPI_ARTIST_RETENTION = TableContract(
    name="kpi_artist_retention",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Cohortes por mes de descubrimiento: que parte de los artistas se escucha N meses despues.",
    columns=(
        _c(
            "cohort_month",
            "STRING",
            "Mes (AAAA-MM) en que se escucho al artista por primera vez",
            nullable=False,
        ),
        _c("cohort_start", "DATE", "Primer dia del mes de la cohorte", nullable=False),
        _c("months_since", "INT", "Meses desde el descubrimiento (0 = mismo mes)", nullable=False),
        _c("cohort_size", "BIGINT", "Artistas descubiertos ese mes", nullable=False),
        _c("artists_active", "BIGINT", "De esos, cuantos se escucharon en el mes N", nullable=False),
        _c("retention_rate", "DOUBLE", "artists_active / cohort_size", nullable=False),
    ),
)

SILVER_TABLES = (SILVER_PLAYS, SILVER_EPISODES, SILVER_DISCARDED, SILVER_ARTISTS)
GOLD_TABLES = (
    GOLD_DIM_DATE,
    GOLD_DIM_ARTIST,
    GOLD_DIM_TRACK,
    GOLD_DIM_PLATFORM,
    GOLD_FACT_PLAYS,
    GOLD_KPI_LISTENING_DAILY,
    GOLD_KPI_TOP_ARTISTS_MONTHLY,
    GOLD_KPI_GENRES_MONTHLY,
    GOLD_KPI_ARTIST_RETENTION,
)
