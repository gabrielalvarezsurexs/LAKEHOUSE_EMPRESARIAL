"""Bronce a Plata: limpieza, dedup, musica vs podcast, hora local, sesiones, etiqueta de skip y artistas.

Plata se recalcula completa en cada corrida (sesiones y duplicados dependen de las filas vecinas; son ~15k
filas). Cada fila de Bronce termina en plays, episodes o _discarded con su causa: asi cuadran los conteos.
Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lakehouse.common.delta import TableLocator, overwrite, read_optional, read_table
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import (
    BRONZE_MUSICBRAINZ_ARTISTS,
    BRONZE_SPOTIFY_STREAMS,
    SILVER_ARTISTS,
    SILVER_DISCARDED,
    SILVER_EPISODES,
    SILVER_PLAYS,
)
from lakehouse.transformation.catalogs import (
    COMPLETE_REASONS,
    COUNTRY_TIMEZONES,
    OTHER_PLATFORM,
    PLATFORM_FAMILIES,
    SESSION_GAP_MINUTES,
    SHORT_RULE_REASONS,
    SKIP_REASONS,
    SKIP_THRESHOLD_MS,
    UNKNOWN_COUNTRIES,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger("lakehouse.transformation.silver")

DEDUP_KEY = ("ts_end", "content_uri", "ms_played")  # medido en F3: DATOS_FUENTE 1.3
CAUSE_DUPLICATE = "duplicate"
CAUSE_NO_CONTENT = "no_content"
CAUSE_AUDIOBOOK = "audiobook_not_supported"


@dataclass
class SilverResult:
    bronze_rows: int
    plays: int
    episodes: int
    discarded: dict[str, int] = field(default_factory=dict)
    sessions: int = 0
    local_time_unknown: int = 0
    labeled: int = 0
    skips: int = 0
    artists: int = 0
    artists_with_genre: int = 0
    plays_with_genre: int = 0


def platform_family_expr(col: str) -> Column:
    """Misma regla que catalogs.platform_family: primer patron que coincide; sin patron = other."""
    from pyspark.sql import functions as F

    text = F.lower(F.col(col))
    expr = F.lit(OTHER_PLATFORM)
    for family, pattern in reversed(PLATFORM_FAMILIES):
        expr = F.when(text.rlike(pattern), family).otherwise(expr)
    return F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), F.lit(None)).otherwise(expr)


def skip_label_expr() -> Column:
    """Misma regla que catalogs.skip_label."""
    from pyspark.sql import functions as F

    reason, ms = F.col("reason_end"), F.col("ms_played")
    return (
        F.when(reason.isin(*SKIP_REASONS), F.lit(True))
        .when(reason.isin(*COMPLETE_REASONS), F.lit(False))
        .when(reason.isin(*SHORT_RULE_REASONS), ms < SKIP_THRESHOLD_MS)
        .otherwise(F.lit(None).cast("boolean"))
    )


def offline_at_expr() -> Column:
    """offline_timestamp a TIMESTAMP (H-003): 10 digitos = segundos, 13 = milisegundos, otro = null."""
    from pyspark.sql import functions as F

    v = F.col("offline_timestamp")
    return (
        F.when((v >= 10**9) & (v < 10**10), F.timestamp_seconds(v))
        .when((v >= 10**12) & (v < 10**13), F.timestamp_seconds(v / 1000))
        .otherwise(F.lit(None).cast("timestamp"))
    )


def prepare(bronze: DataFrame) -> DataFrame:
    """Reglas por fila (sin vecinas): vacios a null, pais, inicio, plataforma, offline y tipo de fila."""
    from pyspark.sql import functions as F

    df = bronze
    text_columns = [
        c.name for c in BRONZE_SPOTIFY_STREAMS.columns if c.dtype == "STRING" and c.name[0] != "_"
    ]
    for name in text_columns:
        df = df.withColumn(name, F.when(F.trim(F.col(name)) == "", F.lit(None)).otherwise(F.col(name)))
    country = F.upper(F.col("conn_country"))
    audiobook = F.col("audiobook_uri").isNotNull() | F.col("audiobook_chapter_uri").isNotNull()
    return (
        df.withColumn(
            "conn_country", F.when(country.isin(*UNKNOWN_COUNTRIES), F.lit(None)).otherwise(country)
        )
        .withColumn("ts_end", F.col("ts"))
        .withColumn("ts_start", F.expr("timestamp_micros(unix_micros(ts) - ms_played * 1000)"))
        .withColumn("platform_family", platform_family_expr("platform"))
        .withColumn("offline_at", offline_at_expr())
        .withColumn("content_uri", F.coalesce("spotify_track_uri", "spotify_episode_uri"))
        .withColumn(
            "kind",
            F.when(F.col("spotify_track_uri").isNotNull(), "track")
            .when(F.col("spotify_episode_uri").isNotNull(), "episode")
            .when(audiobook, "audiobook")
            .otherwise("none"),
        )
    )


def dedup(content: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(unicas, duplicadas). Gana la primera por archivo y posicion; play_id sale de la llave de dedup."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    play_id = F.sha2(
        F.concat_ws(
            "|",
            F.expr("CAST(unix_micros(ts_end) AS STRING)"),
            F.col("content_uri"),
            F.col("ms_played").cast("string"),
        ),
        256,
    )
    first = Window.partitionBy(*DEDUP_KEY).orderBy("_source_file", "_record_index")
    ranked = content.withColumn("play_id", play_id).withColumn("_rank", F.row_number().over(first))
    unique = ranked.where("_rank = 1").drop("_rank")
    duplicates = ranked.where("_rank > 1").select(
        "_file_sha256",
        "_record_index",
        F.lit(CAUSE_DUPLICATE).alias("cause"),
        "ts_end",
        F.col("play_id").alias("duplicate_of"),
    )
    return unique, duplicates


def add_sessions(df: DataFrame) -> DataFrame:
    """Sesion nueva cuando el inicio queda a mas de 30 min del fin mas tardio de lo anterior."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Un solo usuario: la ventana va sin particion a proposito (log4j2 calla el aviso de WindowExec).
    order = Window.orderBy("ts_start", "play_id")
    gap_us = SESSION_GAP_MINUTES * 60 * 1_000_000
    df = df.withColumn("_prev_end", F.max("ts_end").over(order.rowsBetween(Window.unboundedPreceding, -1)))
    new_session = F.col("_prev_end").isNull() | (
        F.expr("unix_micros(ts_start) - unix_micros(_prev_end)") > gap_us
    )
    df = df.withColumn("_new", new_session.cast("int")).withColumn(
        "_session", F.sum("_new").over(order.rowsBetween(Window.unboundedPreceding, 0))
    )
    session = Window.partitionBy("_session")
    return (
        df.withColumn("session_id", F.date_format(F.min("ts_start").over(session), "yyyyMMdd'T'HHmmss"))
        .withColumn("session_seq", F.row_number().over(session.orderBy("ts_start", "play_id")))
        .drop("_prev_end", "_new")
    )


def add_local_time(df: DataFrame) -> DataFrame:
    """Hora local por pais; si la fila no trae pais pero su sesion tiene uno solo, se usa ese."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    countries = F.collect_set("conn_country").over(Window.partitionBy("_session"))  # ignora nulls
    tz_country = F.coalesce(F.col("conn_country"), F.when(F.size(countries) == 1, countries[0]))
    zones = F.create_map(*[F.lit(x) for pair in sorted(COUNTRY_TIMEZONES.items()) for x in pair])
    df = df.withColumn("tz_name", F.try_element_at(zones, tz_country))
    df = df.withColumn(
        "tz_source",
        F.when(F.col("tz_name").isNull(), F.lit(None))
        .when(F.col("conn_country").isNotNull(), "country")
        .otherwise("session"),
    )
    local = F.from_utc_timestamp(F.col("ts_start"), F.col("tz_name"))
    return (
        df.withColumn("_local", local)
        .withColumn("local_date", F.to_date("_local"))
        .withColumn("local_hour", F.hour("_local"))
        .withColumn("local_weekday", F.expr("weekday(_local) + 1"))
        .drop("_local")
    )


def to_plays(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        df.where("kind = 'track'")
        .withColumn("track_uri", F.col("spotify_track_uri"))
        .withColumn("track_name", F.col("master_metadata_track_name"))
        .withColumn("artist_name", F.col("master_metadata_album_artist_name"))
        .withColumn("album_name", F.col("master_metadata_album_album_name"))
        .withColumn("spotify_skipped", F.col("skipped"))
        .withColumn("is_skip", skip_label_expr())
    )


def to_episodes(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        df.where("kind = 'episode'")
        .withColumn("episode_uri", F.col("spotify_episode_uri"))
        .withColumn("show_name", F.col("episode_show_name"))
    )


def build_artists(plays: DataFrame, musicbrainz: DataFrame | None) -> DataFrame:
    """Artistas escuchados + generos de MusicBrainz (de mas a menos votos). Sin consulta: not_queried."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    names = plays.where("artist_name IS NOT NULL").select("artist_name").distinct()
    if musicbrainz is None:
        return names.select(
            "artist_name",
            F.lit("not_queried").alias("mb_status"),
            F.lit(None).cast("string").alias("mbid"),
            F.lit(None).cast("array<string>").alias("genres"),
            F.lit(None).cast("string").alias("primary_genre"),
        )
    latest = Window.partitionBy("artist_name").orderBy(F.col("fetched_at").desc())
    mb = (
        musicbrainz.withColumn("_rank", F.row_number().over(latest))
        .where("_rank = 1")
        .select("artist_name", "status", "mbid", "genres")
    )
    by_votes = F.expr(
        "transform(array_sort(genres, (a, b) -> CASE WHEN a.`count` > b.`count` THEN -1 "
        "WHEN a.`count` < b.`count` THEN 1 WHEN a.name < b.name THEN -1 WHEN a.name > b.name THEN 1 "
        "ELSE 0 END), g -> g.name)"
    )
    joined = names.join(mb, "artist_name", "left")
    genres = F.when(F.col("status") == "matched", by_votes)
    return (
        joined.withColumn("genres", genres)
        .withColumn("mb_status", F.coalesce(F.col("status"), F.lit("not_queried")))
        .withColumn("primary_genre", F.when(F.size("genres") > 0, F.col("genres")[0]))
    )


def _discards(prepared: DataFrame, duplicates: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    cause = F.when(F.col("kind") == "audiobook", CAUSE_AUDIOBOOK).otherwise(CAUSE_NO_CONTENT)
    no_content = prepared.where("kind IN ('none', 'audiobook')").select(
        "_file_sha256",
        "_record_index",
        cause.alias("cause"),
        "ts_end",
        F.lit(None).cast("string").alias("duplicate_of"),
    )
    return no_content.unionByName(duplicates)


def build_silver(spark: SparkSession, locator: TableLocator) -> SilverResult:
    """Lee Bronce, escribe las 4 tablas de Plata y regresa los conteos para el resumen."""
    from pyspark.sql import functions as F

    bronze = read_table(spark, locator, BRONZE_SPOTIFY_STREAMS)
    prepared = prepare(bronze).cache()
    unique, duplicates = dedup(prepared.where("kind IN ('track', 'episode')"))
    ready = add_local_time(add_sessions(unique)).cache()
    try:
        plays, episodes = to_plays(ready), to_episodes(ready)
        discarded = _discards(prepared, duplicates)
        artists = build_artists(plays, read_optional(spark, locator, BRONZE_MUSICBRAINZ_ARTISTS))

        overwrite(spark, plays, locator, SILVER_PLAYS)
        overwrite(spark, episodes, locator, SILVER_EPISODES)
        overwrite(spark, discarded, locator, SILVER_DISCARDED)
        overwrite(spark, artists, locator, SILVER_ARTISTS)

        silver_plays = read_table(spark, locator, SILVER_PLAYS)
        silver_artists = read_table(spark, locator, SILVER_ARTISTS)
        stats = silver_plays.agg(
            F.count("*").alias("plays"),
            F.count("is_skip").alias("labeled"),
            F.sum(F.col("is_skip").cast("int")).alias("skips"),
            F.sum(F.col("tz_name").isNull().cast("int")).alias("no_tz"),
        ).first()
        with_genre = silver_plays.join(
            silver_artists.where("primary_genre IS NOT NULL").select("artist_name"), "artist_name"
        ).count()
        causes = read_table(spark, locator, SILVER_DISCARDED).groupBy("cause").count().collect()
        result = SilverResult(
            bronze_rows=bronze.count(),
            plays=stats["plays"],
            episodes=read_table(spark, locator, SILVER_EPISODES).count(),
            discarded={r["cause"]: r["count"] for r in causes},
            sessions=ready.select("session_id").distinct().count(),
            local_time_unknown=stats["no_tz"] or 0,
            labeled=stats["labeled"],
            skips=stats["skips"] or 0,
            artists=silver_artists.count(),
            artists_with_genre=silver_artists.where("primary_genre IS NOT NULL").count(),
            plays_with_genre=with_genre,
        )
    finally:
        ready.unpersist()
        prepared.unpersist()
    _log_result(result)
    return result


def _pct(part: int, total: int) -> str:
    return f"{100 * part / total:.1f}%" if total else "n/a"


def _log_result(r: SilverResult) -> None:
    log.info(
        "Plata: bronce=%d, plays=%d, episodes=%d, descartadas=%s, sesiones=%d",
        r.bronze_rows,
        r.plays,
        r.episodes,
        dict(sorted(r.discarded.items())) or "{}",
        r.sessions,
    )
    log.info(
        "Plata: sin hora local=%d, con etiqueta de skip=%d (%s), skips=%d (%s de las etiquetadas)",
        r.local_time_unknown,
        r.labeled,
        _pct(r.labeled, r.plays),
        r.skips,
        _pct(r.skips, r.labeled),
    )
    log.info(
        "Plata: artistas=%d, con genero=%d (%s), reproducciones con genero=%d (%s)",
        r.artists,
        r.artists_with_genre,
        _pct(r.artists_with_genre, r.artists),
        r.plays_with_genre,
        _pct(r.plays_with_genre, r.plays),
    )
