"""Plata a Oro: modelo estrella (D10) y marts de KPIs para BI. ML y BI leen solo de aqui.

fact_plays se actualiza con MERGE por play_id (incremental: re-correr no escribe nada) y se compacta con
OPTIMIZE + Z-ORDER por date_key solo cuando cambio. Dimensiones y marts son chicos: se reescriben completos.
Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

from lakehouse.common.delta import TableLocator, ensure_table, overwrite, read_table
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import (
    GOLD_DIM_ARTIST,
    GOLD_DIM_DATE,
    GOLD_DIM_PLATFORM,
    GOLD_DIM_TRACK,
    GOLD_FACT_PLAYS,
    GOLD_KPI_ARTIST_RETENTION,
    GOLD_KPI_GENRES_MONTHLY,
    GOLD_KPI_LISTENING_DAILY,
    GOLD_KPI_TOP_ARTISTS_MONTHLY,
    GOLD_TABLES,
    SILVER_ARTISTS,
    SILVER_PLAYS,
)
from lakehouse.transformation.catalogs import DEVICE_TYPES

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger("lakehouse.transformation.gold")

MONTH_NAMES = (
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
)  # fmt: skip
WEEKDAY_NAMES = ("lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo")
ZORDER_COLUMN = "date_key"


@dataclass
class MergeStats:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0

    @property
    def changed(self) -> bool:
        return bool(self.inserted or self.updated or self.deleted)


@dataclass
class GoldResult:
    merge: MergeStats
    optimized: bool
    counts: dict[str, int] = field(default_factory=dict)


def artist_key(col: str) -> Column:
    """Llave estable del artista: primeros 16 hex del SHA-256 del nombre (null si no hay nombre)."""
    from pyspark.sql import functions as F

    return F.substring(F.sha2(F.col(col), 256), 1, 16)


def dim_artist(artists: DataFrame) -> DataFrame:
    return artists.withColumn("artist_key", artist_key("artist_name"))


def dim_track(plays: DataFrame) -> DataFrame:
    """Una fila por URI; nombre, album y artista de la reproduccion mas reciente."""
    from pyspark.sql import functions as F

    return (
        plays.groupBy(F.col("track_uri").alias("track_key"))
        .agg(
            F.max_by("track_name", "ts_end").alias("track_name"),
            F.max_by("album_name", "ts_end").alias("album_name"),
            F.max_by("artist_name", "ts_end").alias("artist_name"),
        )
        .withColumn("artist_key", artist_key("artist_name"))
    )


def dim_platform(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(sorted(DEVICE_TYPES.items()), GOLD_DIM_PLATFORM.spark_schema())


def dim_date(spark: SparkSession, plays: DataFrame) -> DataFrame:
    """Calendario continuo entre la primera y la ultima fecha local."""
    from pyspark.sql import functions as F

    first, last = plays.agg(F.min("local_date"), F.max("local_date")).first()
    if first is None:
        return spark.createDataFrame([], GOLD_DIM_DATE.spark_schema())
    return date_rows(spark, first, last)


def date_rows(spark: SparkSession, first: date, last: date) -> DataFrame:
    from pyspark.sql import functions as F

    months = F.array(*[F.lit(m) for m in MONTH_NAMES])
    weekdays = F.array(*[F.lit(d) for d in WEEKDAY_NAMES])
    weekday = F.expr("weekday(date) + 1")
    return (
        spark.range(1)
        .select(F.explode(F.sequence(F.lit(first), F.lit(last))).alias("date"))
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.element_at(months, F.month("date")).alias("month_name"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.dayofmonth("date").alias("day"),
            weekday.alias("weekday"),
            F.element_at(weekdays, weekday).alias("weekday_name"),
            (weekday >= 6).alias("is_weekend"),
        )
    )


def fact_plays(plays: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return plays.select(
        "play_id",
        F.date_format("local_date", "yyyyMMdd").cast("int").alias("date_key"),
        "ts_start",
        "ts_end",
        "local_hour",
        "local_weekday",
        F.col("track_uri").alias("track_key"),
        artist_key("artist_name").alias("artist_key"),
        "platform_family",
        "conn_country",
        "session_id",
        "session_seq",
        "ms_played",
        (F.col("ms_played") / 60000).alias("minutes_played"),
        "is_skip",
        "reason_start",
        "reason_end",
        "shuffle",
        "offline",
        "incognito_mode",
        F.year("ts_start").alias("year"),
    )


def merge_fact(spark: SparkSession, locator: TableLocator, fact: DataFrame) -> MergeStats:
    """MERGE por play_id: inserta nuevas, actualiza si algo cambio y borra las que ya no estan en Plata."""
    from delta.tables import DeltaTable

    uri = locator.uri(GOLD_FACT_PLAYS)
    ensure_table(spark, uri, GOLD_FACT_PLAYS)
    others = [c for c in GOLD_FACT_PLAYS.column_names if c != "play_id"]
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in others)
    table = DeltaTable.forPath(spark, uri)
    (
        table.alias("t")
        .merge(GOLD_FACT_PLAYS.conform(fact).alias("s"), "t.play_id = s.play_id")
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    metrics = table.history(1).select("operationMetrics").first()[0] or {}
    return MergeStats(
        inserted=int(metrics.get("numTargetRowsInserted", 0)),
        updated=int(metrics.get("numTargetRowsUpdated", 0)),
        deleted=int(metrics.get("numTargetRowsDeleted", 0)),
    )


def optimize_fact(spark: SparkSession, locator: TableLocator) -> None:
    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, locator.uri(GOLD_FACT_PLAYS)).optimize().executeZOrderBy(ZORDER_COLUMN)


def _dated(fact: DataFrame) -> DataFrame:
    """Solo filas con fecha local (las demas se cuentan en Plata), con su mes."""
    from pyspark.sql import functions as F

    day = F.to_date(F.col("date_key").cast("string"), "yyyyMMdd")
    return (
        fact.where("date_key IS NOT NULL")
        .withColumn("month_start", F.trunc(day, "MM"))
        .withColumn("year_month", F.date_format("month_start", "yyyy-MM"))
    )


def kpi_listening_daily(fact: DataFrame, dates: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    daily = (
        fact.where("date_key IS NOT NULL")
        .groupBy("date_key")
        .agg(
            F.count("*").alias("plays"),
            F.sum("minutes_played").alias("minutes_played"),
            F.countDistinct("track_key").alias("distinct_tracks"),
            F.countDistinct("artist_key").alias("distinct_artists"),
            F.countDistinct("session_id").alias("sessions"),
            F.count("is_skip").alias("labeled_plays"),
            F.count(F.when(F.col("is_skip"), 1)).alias("skips"),
        )
    )
    rate = F.when(F.col("labeled_plays") > 0, F.col("skips") / F.col("labeled_plays"))
    return daily.join(dates.select("date_key", "date"), "date_key").withColumn("skip_rate", rate)


def kpi_top_artists_monthly(fact: DataFrame, artists: DataFrame) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    monthly = (
        _dated(fact)
        .where("artist_key IS NOT NULL")
        .groupBy("year_month", "month_start", "artist_key")
        .agg(F.count("*").alias("plays"), F.sum("minutes_played").alias("minutes_played"))
    )
    rank = Window.partitionBy("year_month").orderBy(
        F.col("minutes_played").desc(), F.col("plays").desc(), "artist_key"
    )
    return monthly.join(
        artists.select("artist_key", "artist_name", "primary_genre"), "artist_key", "left"
    ).withColumn("rank_in_month", F.row_number().over(rank))


def kpi_genres_monthly(fact: DataFrame, artists: DataFrame) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    genre = artists.select("artist_key", F.col("primary_genre").alias("genre"))
    monthly = (
        _dated(fact)
        .join(genre, "artist_key", "left")
        .groupBy("year_month", "month_start", "genre")
        .agg(
            F.count("*").alias("plays"),
            F.sum("minutes_played").alias("minutes_played"),
            F.countDistinct("artist_key").alias("artists"),
        )
    )
    month_total = F.sum("minutes_played").over(Window.partitionBy("year_month"))
    share = F.when(month_total > 0, F.col("minutes_played") / month_total)
    return monthly.withColumn("minutes_share", share)


def kpi_artist_retention(fact: DataFrame) -> DataFrame:
    """Cohortes por mes de descubrimiento; la rejilla incluye meses sin escucha (0 contado, no inventado)."""
    from pyspark.sql import functions as F

    active = (
        _dated(fact)
        .where("artist_key IS NOT NULL")
        .select("artist_key", (F.year("month_start") * 12 + F.month("month_start") - 1).alias("month_idx"))
        .distinct()
    )
    first = active.groupBy("artist_key").agg(F.min("month_idx").alias("cohort_idx"))
    counts = (
        active.join(first, "artist_key")
        .withColumn("months_since", F.col("month_idx") - F.col("cohort_idx"))
        .groupBy("cohort_idx", "months_since")
        .agg(F.count("*").alias("artists_active"))
    )
    last_idx = active.agg(F.max("month_idx").alias("last_idx"))
    grid = (
        first.groupBy("cohort_idx")
        .agg(F.count("*").alias("cohort_size"))
        .crossJoin(last_idx)
        .withColumn("months_since", F.explode(F.sequence(F.lit(0), F.col("last_idx") - F.col("cohort_idx"))))
    )
    cohort_start = F.expr("make_date(CAST(cohort_idx DIV 12 AS INT), CAST(cohort_idx % 12 + 1 AS INT), 1)")
    return (
        grid.join(counts, ["cohort_idx", "months_since"], "left")
        .fillna(0, subset=["artists_active"])
        .withColumn("cohort_start", cohort_start)
        .withColumn("cohort_month", F.date_format("cohort_start", "yyyy-MM"))
        .withColumn("retention_rate", F.col("artists_active") / F.col("cohort_size"))
    )


def build_gold(spark: SparkSession, locator: TableLocator) -> GoldResult:
    """Lee Plata, escribe dimensiones, MERGE de fact_plays y marts; regresa conteos para el resumen."""
    plays = read_table(spark, locator, SILVER_PLAYS).cache()
    try:
        artists = dim_artist(read_table(spark, locator, SILVER_ARTISTS))
        overwrite(spark, artists, locator, GOLD_DIM_ARTIST)
        overwrite(spark, dim_track(plays), locator, GOLD_DIM_TRACK)
        overwrite(spark, dim_platform(spark), locator, GOLD_DIM_PLATFORM)
        overwrite(spark, dim_date(spark, plays), locator, GOLD_DIM_DATE)

        stats = merge_fact(spark, locator, fact_plays(plays))
        if stats.changed:
            optimize_fact(spark, locator)
    finally:
        plays.unpersist()

    # Los marts salen de las tablas de Oro ya escritas, no de Plata.
    fact = read_table(spark, locator, GOLD_FACT_PLAYS)
    dim_artists = read_table(spark, locator, GOLD_DIM_ARTIST)
    dates = read_table(spark, locator, GOLD_DIM_DATE)
    overwrite(spark, kpi_listening_daily(fact, dates), locator, GOLD_KPI_LISTENING_DAILY)
    overwrite(spark, kpi_top_artists_monthly(fact, dim_artists), locator, GOLD_KPI_TOP_ARTISTS_MONTHLY)
    overwrite(spark, kpi_genres_monthly(fact, dim_artists), locator, GOLD_KPI_GENRES_MONTHLY)
    overwrite(spark, kpi_artist_retention(fact), locator, GOLD_KPI_ARTIST_RETENTION)

    result = GoldResult(
        stats, stats.changed, {t.name: read_table(spark, locator, t).count() for t in GOLD_TABLES}
    )
    log.info(
        "Oro: MERGE fact_plays insertadas=%d, actualizadas=%d, borradas=%d, OPTIMIZE+ZORDER=%s",
        stats.inserted,
        stats.updated,
        stats.deleted,
        "si" if result.optimized else "no (sin cambios)",
    )
    log.info("Oro: %s", ", ".join(f"{k}={v}" for k, v in result.counts.items()))
    return result
