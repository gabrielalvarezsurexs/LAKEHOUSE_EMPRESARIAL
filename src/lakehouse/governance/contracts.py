"""Contratos de esquema: la unica definicion de cada tabla (columnas, tipos, nulos, privacidad y reglas).

Los demas modulos importan de aqui en vez de repetir listas de columnas. De estos contratos salen las tablas
Delta (D22), los checks de calidad, el catalogo, el linaje (sources) y las propiedades de cada tabla (F4).
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

# Privacidad de una columna: none, personal (identifica a alguien: la IP) o pseudonym (seudonimo con sal).
NO_PII, PERSONAL, PSEUDONYM = "none", "personal", "pseudonym"
PRIVACY_LEVELS = (NO_PII, PERSONAL, PSEUDONYM)

# Clasificacion de tablas, de mas a menos sensible; define quien puede leerlas (politicas de MinIO, D17).
CLASSIFICATIONS: dict[str, str] = {
    "restringido": "Datos personales en claro (IP). Solo el servicio de ingesta.",
    "confidencial": "Seudonimos o registros crudos. Solo servicios del pipeline y gobernanza.",
    "interno": "Historial limpio sin identificadores. Pipeline, gobernanza, ML y BI (Oro).",
}

# Valores aceptados compartidos (las pruebas los comparan con transformation/catalogs.py).
PLATFORM_FAMILY_VALUES = ("web", "smart_device", "android", "ios", "windows", "macos", "linux", "other")
DEVICE_TYPE_VALUES = ("mobile", "desktop", "web", "smart_device", "other")
DISCARD_CAUSES = ("duplicate", "no_content", "audiobook_not_supported")
SPOTIFY_FIRST_DAY = "2008-10-07"  # lanzamiento de Spotify: nada puede ser anterior


@dataclass(frozen=True)
class Column:
    """Una columna: dtype es un tipo de Spark en DDL (STRING, BIGINT, ...); values y rangos son reglas."""

    name: str
    dtype: str
    description: str
    nullable: bool = True
    privacy: str = NO_PII
    values: tuple[str, ...] = ()
    min_value: float | None = None
    max_value: float | None = None

    @property
    def pii(self) -> bool:
        return self.privacy != NO_PII


@dataclass(frozen=True)
class ForeignKey:
    """column apunta a ref_column de la tabla ref ("capa.tabla"); null no se revisa."""

    column: str
    ref: str
    ref_column: str


@dataclass(frozen=True)
class Rule:
    """Condicion SQL que toda fila debe cumplir; si da null cuenta como cumplida."""

    name: str
    sql: str


@dataclass(frozen=True)
class TableContract:
    """Esquema, dueno, clasificacion, origen (sources, para el linaje) y reglas de calidad de una tabla."""

    name: str
    layer: str
    owner: str
    description: str
    columns: tuple[Column, ...]
    partition_by: tuple[str, ...] = ()
    classification: str = "interno"
    sources: tuple[str, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    references: tuple[ForeignKey, ...] = ()
    rules: tuple[Rule, ...] = ()
    allow_empty: bool = False

    @property
    def qualified_name(self) -> str:
        return f"{self.layer}.{self.name}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> Column:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"La tabla {self.name} no tiene la columna {name}")

    def table_properties(self) -> dict[str, str]:
        """Propiedades de la tabla Delta (TBLPROPERTIES): el metadato viaja con la tabla."""
        return {
            "lakehouse.owner": self.owner,
            "lakehouse.layer": self.layer,
            "lakehouse.classification": self.classification,
        }

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


def _c(
    name: str,
    dtype: str,
    description: str,
    nullable: bool = True,
    privacy: str = NO_PII,
    values: tuple[str, ...] = (),
    min_value: float | None = None,
    max_value: float | None = None,
) -> Column:
    return Column(name, dtype, description, nullable, privacy, values, min_value, max_value)


def _fk(column: str, ref: str, ref_column: str | None = None) -> ForeignKey:
    return ForeignKey(column, ref, ref_column or column)


_SHA256_RULE = Rule("_file_sha256 es un SHA-256", "_file_sha256 RLIKE '^[0-9a-f]{64}$'")


# --- Landing: un objeto JSON por reproduccion (historial extendido de Spotify, DATOS_FUENTE 1.2) ---
# Solo ts y ms_played son obligatorios: sin ellos la reproduccion no significa nada. Lo demas puede faltar
# y queda null (mejor vacio que falso).
SPOTIFY_STREAMS_SOURCE = TableContract(
    name="spotify_streams_source",
    layer="landing",
    owner=INGESTION_OWNER,
    description="Historial extendido de Spotify tal como lo entrega la plataforma (un arreglo JSON).",
    classification="restringido",
    sources=("ext.spotify",),
    columns=(
        _c("ts", "TIMESTAMP", "Momento (UTC) en que termino la reproduccion", nullable=False),
        _c("platform", "STRING", "Dispositivo y version de la app"),
        _c("ms_played", "BIGINT", "Milisegundos reproducidos", nullable=False, min_value=0),
        _c("conn_country", "STRING", "Pais de la conexion (ISO 3166-1 alfa-2)"),
        _c("ip_addr", "STRING", "IP de la conexion (dato personal)", privacy=PERSONAL),
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
    _c("_record_index", "BIGINT", "Posicion del registro dentro del archivo", nullable=False, min_value=0),
    _c("_batch_id", "STRING", "Corrida de ingesta", nullable=False),
    _c("_ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
)

_IP_HASH = _c(
    "ip_addr_hash",
    "STRING",
    "HMAC-SHA256 de ip_addr con la sal IP_HASH_SALT (seudonimo, D8)",
    privacy=PSEUDONYM,
)

BRONZE_SPOTIFY_STREAMS = TableContract(
    name="spotify_streams",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Una fila por reproduccion valida; ip_addr llega seudonimizada.",
    classification="confidencial",
    sources=("landing.spotify_streams_source",),
    columns=tuple(_IP_HASH if c.name == "ip_addr" else c for c in SPOTIFY_STREAMS_SOURCE.columns)
    + INGESTION_METADATA,
    unique_keys=(("_file_sha256", "_record_index"),),
    rules=(
        Rule("ts no anterior a Spotify", f"ts >= TIMESTAMP '{SPOTIFY_FIRST_DAY}'"),
        Rule("ts no en el futuro", "ts <= current_timestamp()"),
        Rule("ip_addr_hash es HMAC-SHA256 (no una IP)", "ip_addr_hash RLIKE '^[0-9a-f]{64}$'"),
        _SHA256_RULE,
    ),
)

BRONZE_QUARANTINE = TableContract(
    name="_quarantine",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Registros que no cumplieron el contrato, con su causa (D16). La IP va seudonimizada.",
    classification="confidencial",
    sources=("landing.spotify_streams_source",),
    allow_empty=True,
    columns=(
        _c("_source_file", "STRING", "Llave del archivo en landing", nullable=False),
        _c("_file_sha256", "STRING", "SHA-256 del archivo", nullable=False),
        _c(
            "_record_index",
            "BIGINT",
            "Posicion en el archivo; null si fallo el archivo completo",
            min_value=0,
        ),
        _c("cause", "STRING", "Motivo del rechazo, p. ej. bad_type:ms_played", nullable=False),
        _c("raw_record", "STRING", "Registro original en JSON (ip_addr reemplazada por su hash)"),
        _c("_batch_id", "STRING", "Corrida de ingesta", nullable=False),
        _c("_ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
    ),
    rules=(Rule("raw_record sin ip_addr en claro", """raw_record NOT LIKE '%"ip_addr"%'"""), _SHA256_RULE),
)

BRONZE_INGESTION_LOG = TableContract(
    name="_ingestion_log",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Bitacora de ingesta: un renglon por archivo procesado (D15).",
    classification="interno",
    sources=("landing.spotify_streams_source",),
    columns=(
        _c("file_sha256", "STRING", "SHA-256 del archivo", nullable=False),
        _c("source_file", "STRING", "Llave del archivo en landing", nullable=False),
        _c("batch_id", "STRING", "Corrida de ingesta", nullable=False),
        _c("rows_read", "BIGINT", "Registros leidos del archivo", nullable=False, min_value=0),
        _c("rows_loaded", "BIGINT", "Registros escritos en spotify_streams", nullable=False, min_value=0),
        _c("rows_quarantined", "BIGINT", "Registros enviados a cuarentena", nullable=False, min_value=0),
        _c("unknown_fields", "STRING", "Campos que no estan en el contrato (deriva de esquema)"),
        _c("ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
    ),
    rules=(Rule("leidas = cargadas + cuarentena", "rows_read = rows_loaded + rows_quarantined"),),
)

BRONZE_MUSICBRAINZ_ARTISTS = TableContract(
    name="musicbrainz_artists",
    layer="bronze",
    owner=INGESTION_OWNER,
    description="Un renglon por artista consultado en MusicBrainz, con sus generos (D9).",
    classification="interno",
    sources=("landing.musicbrainz_cache",),
    columns=(
        _c("artist_name", "STRING", "Nombre del artista tal como viene en Spotify", nullable=False),
        _c("status", "STRING", "matched o no_match", nullable=False, values=("matched", "no_match")),
        _c("mbid", "STRING", "Id del artista en MusicBrainz; null si no hubo match confiable"),
        _c("mb_name", "STRING", "Nombre en MusicBrainz"),
        _c("match_score", "BIGINT", "Score de la busqueda (0 a 100)", min_value=0, max_value=100),
        _c("genres", "ARRAY<STRUCT<name: STRING, count: BIGINT>>", "Generos con sus votos; vacio si no hay"),
        _c("raw_json", "STRING", "Documento cacheado en landing, tal cual"),
        _c("_source_file", "STRING", "Llave del documento en landing", nullable=False),
        _c("fetched_at", "TIMESTAMP", "Momento (UTC) en que se consulto la API", nullable=False),
        _c("_ingested_at", "TIMESTAMP", "Momento (UTC) de la ingesta", nullable=False),
    ),
    unique_keys=(("artist_name",),),
    rules=(Rule("matched trae mbid", "status <> 'matched' OR mbid IS NOT NULL"),),
)

# --- Plata: limpio, sin duplicados, sin IP (D8), con hora local, sesiones y etiqueta de skip (F3) ---
# Contexto comun a canciones y episodios: tiempo, dispositivo, pais, hora local, motivos, sesion y linaje.
_PLAY_ID = _c(
    "play_id", "STRING", "SHA-256 de (ts_end, uri, ms_played): llave de la reproduccion", nullable=False
)
_PLAY_TIME = (
    _c("ts_start", "TIMESTAMP", "Inicio (UTC) = ts_end - ms_played", nullable=False),
    _c("ts_end", "TIMESTAMP", "Fin (UTC); es el ts de Spotify", nullable=False),
    _c("ms_played", "BIGINT", "Milisegundos reproducidos", nullable=False, min_value=0),
)
_PLAY_CONTEXT = (
    _c("platform", "STRING", "Dispositivo y version tal como viene"),
    _c(
        "platform_family",
        "STRING",
        "Familia: android, ios, windows, macos, linux, web, smart_device u other",
        values=PLATFORM_FAMILY_VALUES,
    ),
    _c("conn_country", "STRING", "Pais ISO alfa-2; null si Spotify no lo supo (ZZ)"),
    _c("tz_name", "STRING", "Zona horaria IANA usada para la hora local; null si no se pudo saber"),
    _c(
        "tz_source",
        "STRING",
        "country (del pais de la fila) o session (unico pais de su sesion)",
        values=("country", "session"),
    ),
    _c("local_date", "DATE", "Fecha local del inicio"),
    _c("local_hour", "INT", "Hora local del inicio (0 a 23)", min_value=0, max_value=23),
    _c("local_weekday", "INT", "Dia de la semana local (1 lunes a 7 domingo)", min_value=1, max_value=7),
    _c("reason_start", "STRING", "Por que empezo"),
    _c("reason_end", "STRING", "Por que termino"),
    _c("shuffle", "BOOLEAN", "Modo aleatorio activo"),
    _c("offline", "BOOLEAN", "Se reprodujo sin conexion"),
    _c("offline_at", "TIMESTAMP", "offline_timestamp normalizado a UTC (H-003)"),
    _c("incognito_mode", "BOOLEAN", "Sesion privada"),
    _c("session_id", "STRING", "Sesion: se corta con mas de 30 min sin escuchar", nullable=False),
    _c("session_seq", "INT", "Posicion dentro de la sesion (desde 1)", nullable=False, min_value=1),
    _c("_file_sha256", "STRING", "Archivo de origen en Bronce (linaje)", nullable=False),
    _c("_record_index", "BIGINT", "Registro de origen en Bronce (linaje)", nullable=False, min_value=0),
)
_PLAY_RULES = (
    Rule("ts_start <= ts_end", "ts_start <= ts_end"),
    Rule("pais conocido y en ISO alfa-2", "conn_country RLIKE '^[A-Z]{2}$' AND conn_country <> 'ZZ'"),
    Rule("hora local solo si hay zona", "(tz_name IS NULL) = (local_date IS NULL)"),
)
_PLAY_KEYS = (("play_id",), ("_file_sha256", "_record_index"))

SILVER_PLAYS = TableContract(
    name="plays",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Una fila por reproduccion de musica, sin duplicados ni IP.",
    sources=("bronze.spotify_streams",),
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
    unique_keys=_PLAY_KEYS,
    rules=(
        *_PLAY_RULES,
        Rule("track_uri es de cancion", "track_uri LIKE 'spotify:track:%'"),
        Rule("is_skip solo con motivo de fin", "is_skip IS NULL OR reason_end IS NOT NULL"),
    ),
)

SILVER_EPISODES = TableContract(
    name="episodes",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Una fila por reproduccion de podcast, sin duplicados ni IP.",
    sources=("bronze.spotify_streams",),
    allow_empty=True,
    columns=(
        _PLAY_ID,
        *_PLAY_TIME,
        _c("episode_uri", "STRING", "URI del episodio", nullable=False),
        _c("episode_name", "STRING", "Nombre del episodio"),
        _c("show_name", "STRING", "Nombre del podcast"),
        *_PLAY_CONTEXT,
    ),
    unique_keys=_PLAY_KEYS,
    rules=_PLAY_RULES,
)

SILVER_DISCARDED = TableContract(
    name="_discarded",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Filas de Bronce que no pasan a Plata, con causa (reconciliacion de conteos).",
    sources=("bronze.spotify_streams",),
    allow_empty=True,
    columns=(
        _c("_file_sha256", "STRING", "Archivo de origen en Bronce", nullable=False),
        _c("_record_index", "BIGINT", "Registro de origen en Bronce", nullable=False, min_value=0),
        _c(
            "cause",
            "STRING",
            "duplicate, no_content o audiobook_not_supported",
            nullable=False,
            values=DISCARD_CAUSES,
        ),
        _c("ts_end", "TIMESTAMP", "ts de la fila descartada", nullable=False),
        _c("duplicate_of", "STRING", "play_id de la fila que se quedo (solo duplicados)"),
    ),
    unique_keys=(("_file_sha256", "_record_index"),),
    rules=(Rule("duplicate_of solo en duplicados", "(cause = 'duplicate') = (duplicate_of IS NOT NULL)"),),
)

SILVER_ARTISTS = TableContract(
    name="artists",
    layer="silver",
    owner=TRANSFORMATION_OWNER,
    description="Un renglon por artista escuchado, con sus generos de MusicBrainz si hubo match confiable.",
    sources=("silver.plays", "bronze.musicbrainz_artists"),
    columns=(
        _c("artist_name", "STRING", "Nombre del artista en Spotify", nullable=False),
        _c(
            "mb_status",
            "STRING",
            "matched, no_match o not_queried (no se corrio genres)",
            nullable=False,
            values=("matched", "no_match", "not_queried"),
        ),
        _c("mbid", "STRING", "Id en MusicBrainz"),
        _c("genres", "ARRAY<STRING>", "Generos de mas a menos votos; null si no hubo match"),
        _c("primary_genre", "STRING", "Genero con mas votos; null si no hay"),
    ),
    unique_keys=(("artist_name",),),
    rules=(
        Rule("generos solo con match", "mb_status = 'matched' OR genres IS NULL"),
        Rule("genero principal = primero de la lista", "primary_genre IS NULL OR primary_genre = genres[0]"),
    ),
)

# --- Oro: modelo estrella (D10) y marts de KPIs que consume BI; ML lee fact_plays ---
_DATE_KEY = _c("date_key", "INT", "Fecha local como AAAAMMDD (dim_date)")
_ARTIST_KEY = _c("artist_key", "STRING", "Llave del artista (dim_artist)")
_MONTH = (
    _c("year_month", "STRING", "Mes local AAAA-MM", nullable=False),
    _c("month_start", "DATE", "Primer dia del mes", nullable=False),
)
_MONTH_RULE = Rule("year_month = mes de month_start", "year_month = date_format(month_start, 'yyyy-MM')")
_SHARE = {"min_value": 0, "max_value": 1}

GOLD_DIM_DATE = TableContract(
    name="dim_date",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Calendario continuo entre la primera y la ultima fecha local con reproducciones.",
    sources=("silver.plays",),
    columns=(
        _c("date_key", "INT", "AAAAMMDD", nullable=False),
        _c("date", "DATE", "Fecha", nullable=False),
        _c("year", "INT", "Anio", nullable=False),
        _c("quarter", "INT", "Trimestre", nullable=False, min_value=1, max_value=4),
        _c("month", "INT", "Mes (1 a 12)", nullable=False, min_value=1, max_value=12),
        _c("month_name", "STRING", "Nombre del mes", nullable=False),
        _c("year_month", "STRING", "AAAA-MM", nullable=False),
        _c("day", "INT", "Dia del mes", nullable=False, min_value=1, max_value=31),
        _c(
            "weekday",
            "INT",
            "Dia de la semana (1 lunes a 7 domingo)",
            nullable=False,
            min_value=1,
            max_value=7,
        ),
        _c("weekday_name", "STRING", "Nombre del dia", nullable=False),
        _c("is_weekend", "BOOLEAN", "Sabado o domingo", nullable=False),
    ),
    unique_keys=(("date_key",),),
    rules=(
        Rule("date_key = AAAAMMDD de date", "date_key = CAST(date_format(date, 'yyyyMMdd') AS INT)"),
        Rule("fin de semana = sabado o domingo", "is_weekend = (weekday >= 6)"),
    ),
)

GOLD_DIM_ARTIST = TableContract(
    name="dim_artist",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Artistas con genero principal y lista de generos.",
    sources=("silver.artists",),
    columns=(
        _c("artist_key", "STRING", "Primeros 16 hex del SHA-256 del nombre", nullable=False),
        _c("artist_name", "STRING", "Nombre del artista", nullable=False),
        _c("mbid", "STRING", "Id en MusicBrainz"),
        _c("primary_genre", "STRING", "Genero con mas votos; null si no hay"),
        _c("genres", "ARRAY<STRING>", "Generos de mas a menos votos"),
        _c(
            "mb_status",
            "STRING",
            "matched, no_match o not_queried",
            nullable=False,
            values=("matched", "no_match", "not_queried"),
        ),
    ),
    unique_keys=(("artist_key",), ("artist_name",)),
    rules=(Rule("artist_key son 16 hex", "artist_key RLIKE '^[0-9a-f]{16}$'"),),
)

GOLD_DIM_TRACK = TableContract(
    name="dim_track",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Canciones; si el nombre cambio en el tiempo se queda el de la reproduccion mas reciente.",
    sources=("silver.plays",),
    columns=(
        _c("track_key", "STRING", "URI de la cancion", nullable=False),
        _c("track_name", "STRING", "Nombre de la cancion"),
        _c("album_name", "STRING", "Nombre del album"),
        _ARTIST_KEY,
    ),
    unique_keys=(("track_key",),),
    references=(_fk("artist_key", "gold.dim_artist"),),
)

GOLD_DIM_PLATFORM = TableContract(
    name="dim_platform",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Familias de plataforma y su tipo de dispositivo (catalogo fijo).",
    sources=("ext.platform_catalog",),
    columns=(
        _c(
            "platform_family",
            "STRING",
            "Familia de plataforma",
            nullable=False,
            values=PLATFORM_FAMILY_VALUES,
        ),
        _c(
            "device_type",
            "STRING",
            "mobile, desktop, web, smart_device u other",
            nullable=False,
            values=DEVICE_TYPE_VALUES,
        ),
    ),
    unique_keys=(("platform_family",),),
)

GOLD_FACT_PLAYS = TableContract(
    name="fact_plays",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Una fila por reproduccion de musica. MERGE incremental por play_id; Z-ORDER por date_key.",
    sources=("silver.plays",),
    columns=(
        _PLAY_ID,
        _DATE_KEY,
        _c("ts_start", "TIMESTAMP", "Inicio (UTC)", nullable=False),
        _c("ts_end", "TIMESTAMP", "Fin (UTC)", nullable=False),
        _c("local_hour", "INT", "Hora local del inicio", min_value=0, max_value=23),
        _c("local_weekday", "INT", "Dia de la semana local (1 lunes a 7 domingo)", min_value=1, max_value=7),
        _c("track_key", "STRING", "Cancion (dim_track)", nullable=False),
        _ARTIST_KEY,
        _c("platform_family", "STRING", "Plataforma (dim_platform)", values=PLATFORM_FAMILY_VALUES),
        _c("conn_country", "STRING", "Pais ISO alfa-2"),
        _c("session_id", "STRING", "Sesion de escucha", nullable=False),
        _c("session_seq", "INT", "Posicion dentro de la sesion", nullable=False, min_value=1),
        _c("ms_played", "BIGINT", "Milisegundos reproducidos", nullable=False, min_value=0),
        _c("minutes_played", "DOUBLE", "Minutos reproducidos", nullable=False, min_value=0),
        _c("is_skip", "BOOLEAN", "Etiqueta de skip; null si no aplica"),
        _c("reason_start", "STRING", "Por que empezo"),
        _c("reason_end", "STRING", "Por que termino"),
        _c("shuffle", "BOOLEAN", "Modo aleatorio"),
        _c("offline", "BOOLEAN", "Sin conexion"),
        _c("incognito_mode", "BOOLEAN", "Sesion privada"),
        _c("year", "INT", "Anio UTC de ts_start (particion)", nullable=False),
    ),
    partition_by=("year",),
    unique_keys=(("play_id",),),
    references=(
        _fk("date_key", "gold.dim_date"),
        _fk("track_key", "gold.dim_track"),
        _fk("artist_key", "gold.dim_artist"),
        _fk("platform_family", "gold.dim_platform"),
    ),
    rules=(
        Rule("ts_start <= ts_end", "ts_start <= ts_end"),
        Rule("year = anio de ts_start", "year = year(ts_start)"),
    ),
)

GOLD_KPI_LISTENING_DAILY = TableContract(
    name="kpi_listening_daily",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Escucha por dia local: volumen, variedad, sesiones y tasa de skip.",
    sources=("gold.fact_plays", "gold.dim_date"),
    columns=(
        _c("date_key", "INT", "AAAAMMDD", nullable=False),
        _c("date", "DATE", "Fecha local", nullable=False),
        _c("plays", "BIGINT", "Reproducciones", nullable=False, min_value=1),
        _c("minutes_played", "DOUBLE", "Minutos escuchados", nullable=False, min_value=0),
        _c("distinct_tracks", "BIGINT", "Canciones distintas", nullable=False, min_value=1),
        _c("distinct_artists", "BIGINT", "Artistas distintos", nullable=False, min_value=0),
        _c("sessions", "BIGINT", "Sesiones distintas", nullable=False, min_value=1),
        _c("labeled_plays", "BIGINT", "Reproducciones con etiqueta de skip", nullable=False, min_value=0),
        _c("skips", "BIGINT", "Reproducciones con is_skip = true", nullable=False, min_value=0),
        _c("skip_rate", "DOUBLE", "skips / labeled_plays; null si no hay etiquetadas", **_SHARE),
    ),
    unique_keys=(("date_key",),),
    references=(_fk("date_key", "gold.dim_date"),),
    rules=(
        Rule("skips <= etiquetadas <= plays", "skips <= labeled_plays AND labeled_plays <= plays"),
        Rule("skip_rate null solo sin etiquetadas", "(skip_rate IS NULL) = (labeled_plays = 0)"),
    ),
)

GOLD_KPI_TOP_ARTISTS_MONTHLY = TableContract(
    name="kpi_top_artists_monthly",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Artistas por mes con su ranking por minutos escuchados.",
    sources=("gold.fact_plays", "gold.dim_artist"),
    columns=(
        *_MONTH,
        _c("artist_key", "STRING", "Artista", nullable=False),
        _c("artist_name", "STRING", "Nombre del artista"),
        _c("primary_genre", "STRING", "Genero principal"),
        _c("plays", "BIGINT", "Reproducciones", nullable=False, min_value=1),
        _c("minutes_played", "DOUBLE", "Minutos", nullable=False, min_value=0),
        _c("rank_in_month", "INT", "1 = el mas escuchado del mes", nullable=False, min_value=1),
    ),
    unique_keys=(("year_month", "artist_key"), ("year_month", "rank_in_month")),
    references=(_fk("artist_key", "gold.dim_artist"),),
    rules=(_MONTH_RULE,),
)

GOLD_KPI_GENRES_MONTHLY = TableContract(
    name="kpi_genres_monthly",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Generos (principal del artista) por mes; genre null = artista sin genero conocido.",
    sources=("gold.fact_plays", "gold.dim_artist"),
    columns=(
        *_MONTH,
        _c("genre", "STRING", "Genero principal; null si no se conoce"),
        _c("plays", "BIGINT", "Reproducciones", nullable=False, min_value=1),
        _c("minutes_played", "DOUBLE", "Minutos", nullable=False, min_value=0),
        _c("artists", "BIGINT", "Artistas distintos", nullable=False, min_value=0),
        _c(
            "minutes_share",
            "DOUBLE",
            "Fraccion de los minutos del mes; null si el mes suma 0 minutos",
            **_SHARE,
        ),
    ),
    unique_keys=(("year_month", "genre"),),
    rules=(_MONTH_RULE,),
)

GOLD_KPI_ARTIST_RETENTION = TableContract(
    name="kpi_artist_retention",
    layer="gold",
    owner=TRANSFORMATION_OWNER,
    description="Cohortes por mes de descubrimiento: que parte de los artistas se escucha N meses despues.",
    sources=("gold.fact_plays",),
    columns=(
        _c(
            "cohort_month",
            "STRING",
            "Mes (AAAA-MM) en que se escucho al artista por primera vez",
            nullable=False,
        ),
        _c("cohort_start", "DATE", "Primer dia del mes de la cohorte", nullable=False),
        _c(
            "months_since",
            "INT",
            "Meses desde el descubrimiento (0 = mismo mes)",
            nullable=False,
            min_value=0,
        ),
        _c("cohort_size", "BIGINT", "Artistas descubiertos ese mes", nullable=False, min_value=1),
        _c(
            "artists_active",
            "BIGINT",
            "De esos, cuantos se escucharon en el mes N",
            nullable=False,
            min_value=0,
        ),
        _c("retention_rate", "DOUBLE", "artists_active / cohort_size", nullable=False, **_SHARE),
    ),
    unique_keys=(("cohort_month", "months_since"),),
    rules=(
        Rule("activos <= tamano de cohorte", "artists_active <= cohort_size"),
        Rule("mes 0 retiene al 100%", "months_since > 0 OR artists_active = cohort_size"),
    ),
)

# --- ML (F6): features sin fuga y predicciones por lote; viven en Oro (gold/ml_*, D17) ---
# Sin fuga: cada feature usa solo lo que se sabe al empezar la reproduccion (su contexto y las que
# empezaron antes). ms_played y reason_end de la propia fila nunca entran: son el resultado.
ML_OWNER = "Fernando Pena Meade (Machine Learning)"
ML_MODEL_NODE = "ml.skip_model"  # modelo registrado en MLflow: nodo del linaje, no tabla Delta
SPLITS = ("train", "validation", "test", "unlabeled")

GOLD_ML_SKIP_FEATURES = TableContract(
    name="ml_skip_features",
    layer="gold",
    owner=ML_OWNER,
    description="Una fila por reproduccion de fact_plays: features (solo historia previa) y etiqueta.",
    sources=("gold.fact_plays", "gold.dim_artist"),
    columns=(
        _PLAY_ID,
        _c("ts_start", "TIMESTAMP", "Inicio (UTC): ordena la historia y el split temporal", nullable=False),
        _DATE_KEY,
        _c("is_skip", "BOOLEAN", "Etiqueta (fact_plays.is_skip); null = sin etiqueta, no se entrena"),
        _c("local_hour", "INT", "Hora local del inicio", min_value=0, max_value=23),
        _c("local_weekday", "INT", "Dia de la semana local (1 lunes a 7 domingo)", min_value=1, max_value=7),
        _c("is_weekend", "BOOLEAN", "Sabado o domingo local; null sin hora local"),
        _c("platform_family", "STRING", "Familia de plataforma", values=PLATFORM_FAMILY_VALUES),
        _c("reason_start", "STRING", "Como empezo (se sabe al empezar)"),
        _c("shuffle", "BOOLEAN", "Modo aleatorio"),
        _c("offline", "BOOLEAN", "Sin conexion"),
        _c("incognito_mode", "BOOLEAN", "Sesion privada"),
        _c("primary_genre", "STRING", "Genero principal del artista; null si no se conoce"),
        _c("session_seq", "INT", "Posicion dentro de la sesion", nullable=False, min_value=1),
        _c(
            "session_minutes",
            "DOUBLE",
            "Minutos desde el inicio de la sesion hasta esta reproduccion",
            nullable=False,
            min_value=0,
        ),
        _c("prev_is_skip", "BOOLEAN", "Etiqueta de la reproduccion anterior de la sesion; null si no hay"),
        _c("prev_minutes_played", "DOUBLE", "Minutos de la anterior en la sesion; null si no", min_value=0),
        _c(
            "track_prior_plays",
            "BIGINT",
            "Veces que ya se habia reproducido la cancion",
            nullable=False,
            min_value=0,
        ),
        _c(
            "track_prior_skip_rate",
            "DOUBLE",
            "Tasa de skip previa de la cancion; null si no tiene reproducciones etiquetadas antes",
            **_SHARE,
        ),
        _c(
            "days_since_track_first",
            "DOUBLE",
            "Dias desde la primera vez que se escucho la cancion (0 = primera vez)",
            nullable=False,
            min_value=0,
        ),
        _c("artist_prior_plays", "BIGINT", "Reproducciones previas del artista; null si no hay", min_value=0),
        _c("artist_prior_skip_rate", "DOUBLE", "Tasa de skip previa del artista; null si no hay", **_SHARE),
        _c(
            "user_recent_skip_rate",
            "DOUBLE",
            "Tasa de skip de las 100 reproducciones anteriores (cuentan las etiquetadas); null si ninguna",
            **_SHARE,
        ),
    ),
    unique_keys=(("play_id",),),
    references=(_fk("play_id", "gold.fact_plays"),),
    rules=(
        Rule("primera vez = 0 dias", "track_prior_plays > 0 OR days_since_track_first = 0"),
        Rule("sin historia no hay tasa", "track_prior_plays > 0 OR track_prior_skip_rate IS NULL"),
        Rule("primera de la sesion sin anterior", "session_seq > 1 OR prev_minutes_played IS NULL"),
    ),
)

GOLD_ML_SKIP_PREDICTIONS = TableContract(
    name="ml_skip_predictions",
    layer="gold",
    owner=ML_OWNER,
    description="Probabilidad de skip por reproduccion (inferencia por lote con MLflow) y el baseline.",
    sources=("gold.ml_skip_features", ML_MODEL_NODE),
    columns=(
        _PLAY_ID,
        _c("ts_start", "TIMESTAMP", "Inicio (UTC)", nullable=False),
        _DATE_KEY,
        _c(
            "split",
            "STRING",
            "train, validation o test (corte temporal del entrenamiento) o unlabeled",
            nullable=False,
            values=SPLITS,
        ),
        _c("is_skip", "BOOLEAN", "Etiqueta real; null si no tiene"),
        _c("skip_probability", "DOUBLE", "Probabilidad de skip del modelo", nullable=False, **_SHARE),
        _c("threshold", "DOUBLE", "Umbral elegido en validacion (max F1)", nullable=False, **_SHARE),
        _c("predicted_skip", "BOOLEAN", "skip_probability >= threshold", nullable=False),
        _c(
            "baseline_probability",
            "DOUBLE",
            "Baseline: tasa de skip previa de la cancion (o la reciente del usuario, o la de entrenamiento)",
            nullable=False,
            **_SHARE,
        ),
        _c("model_name", "STRING", "Modelo registrado en MLflow", nullable=False),
        _c("model_version", "STRING", "Version del modelo registrado", nullable=False),
        _c("mlflow_run_id", "STRING", "Corrida de MLflow que entreno el modelo", nullable=False),
        _c("predicted_at", "TIMESTAMP", "Momento (UTC) de la inferencia", nullable=False),
    ),
    unique_keys=(("play_id",),),
    references=(_fk("play_id", "gold.ml_skip_features"),),
    rules=(
        Rule("prediccion = probabilidad >= umbral", "predicted_skip = (skip_probability >= threshold)"),
        Rule("unlabeled si y solo si no hay etiqueta", "(split = 'unlabeled') = (is_skip IS NULL)"),
    ),
)

LANDING_TABLES = (SPOTIFY_STREAMS_SOURCE,)
BRONZE_TABLES = (
    BRONZE_SPOTIFY_STREAMS,
    BRONZE_QUARANTINE,
    BRONZE_INGESTION_LOG,
    BRONZE_MUSICBRAINZ_ARTISTS,
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
# Tablas de ML: en Oro, pero las escribe el paso de ML (svc-ml), no transformacion.
ML_TABLES = (GOLD_ML_SKIP_FEATURES, GOLD_ML_SKIP_PREDICTIONS)
# Tablas Delta (las de landing son archivos crudos) y el registro completo, en orden del flujo.
DELTA_TABLES = BRONZE_TABLES + SILVER_TABLES + GOLD_TABLES + ML_TABLES
ALL_TABLES = LANDING_TABLES + DELTA_TABLES


def by_name(qualified_name: str) -> TableContract:
    """Contrato por nombre 'capa.tabla'."""
    for contract in ALL_TABLES:
        if contract.qualified_name == qualified_name:
            return contract
    raise KeyError(f"No hay contrato para {qualified_name}")
