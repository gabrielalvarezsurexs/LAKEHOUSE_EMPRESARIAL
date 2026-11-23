"""Contratos de esquema: la unica definicion de columnas de cada tabla (tipo, nulos, descripcion, PII).

Los demas modulos importan de aqui en vez de repetir listas de columnas. F4 agrega Plata y Oro y genera
el catalogo a partir de estos contratos.
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

INGESTION_OWNER = "Ricardo Zeron del Valle (Ingesta)"


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

_IP_HASH = _c("ip_addr_hash", "STRING", "SHA-256 con sal de ip_addr (seudonimo, D8)")

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
