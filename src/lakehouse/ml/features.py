"""Features de la prediccion de skip: una fila por reproduccion de gold.fact_plays, sin fuga (F6).

Sin fuga: cada feature usa solo el contexto de la reproduccion y las que empezaron antes (las ventanas
terminan en la fila anterior). ms_played y reason_end de la propia fila nunca entran: son el resultado.
Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.common.delta import TableLocator, overwrite, read_table
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import GOLD_DIM_ARTIST, GOLD_FACT_PLAYS, GOLD_ML_SKIP_FEATURES

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger("lakehouse.ml.features")

RECENT_PLAYS = 100  # ventana de la tasa de skip reciente del usuario
SECONDS_PER_DAY = 86_400


def _rate(skips: Column, labeled: Column) -> Column:
    """skips / etiquetadas; null si no hay etiquetadas (mejor vacio que 0)."""
    from pyspark.sql import functions as F

    return F.when(labeled > 0, skips / labeled)


def skip_features(fact: DataFrame, artists: DataFrame) -> DataFrame:
    """Features por reproduccion; el orden de la historia es (ts_start, play_id), estable entre corridas."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = ("ts_start", "play_id")
    before = (Window.unboundedPreceding, -1)
    by_track = Window.partitionBy("track_key").orderBy(*order).rowsBetween(*before)
    by_artist = Window.partitionBy("artist_key").orderBy(*order).rowsBetween(*before)
    # Una sola persona: la ventana global sobre ~15k filas cabe en una particion.
    recent = Window.orderBy(*order).rowsBetween(-RECENT_PLAYS, -1)
    by_session = Window.partitionBy("session_id").orderBy("session_seq")
    skip = F.col("is_skip").cast("double")  # count y sum ignoran los null (sin etiqueta)
    has_artist = F.col("artist_key").isNotNull()

    # El primer inicio de la cancion o de la sesion siempre es <= al de la fila: es pasado, no fuga.
    track_first = F.min("ts_start").over(Window.partitionBy("track_key"))
    session_first = F.min("ts_start").over(Window.partitionBy("session_id"))
    seconds = F.col("ts_start").cast("long")

    genres = artists.select("artist_key", "primary_genre")
    return fact.join(genres, "artist_key", "left").select(
        "play_id",
        "ts_start",
        "date_key",
        "is_skip",
        "local_hour",
        "local_weekday",
        F.when(F.col("local_weekday").isNotNull(), F.col("local_weekday") >= 6).alias("is_weekend"),
        "platform_family",
        "reason_start",
        "shuffle",
        "offline",
        "incognito_mode",
        "primary_genre",
        "session_seq",
        ((seconds - session_first.cast("long")) / 60.0).alias("session_minutes"),
        F.lag("is_skip").over(by_session).alias("prev_is_skip"),
        F.lag("minutes_played").over(by_session).alias("prev_minutes_played"),
        F.count(F.lit(1)).over(by_track).alias("track_prior_plays"),
        _rate(F.sum(skip).over(by_track), F.count(skip).over(by_track)).alias("track_prior_skip_rate"),
        ((seconds - track_first.cast("long")) / SECONDS_PER_DAY).alias("days_since_track_first"),
        F.when(has_artist, F.count(F.lit(1)).over(by_artist)).alias("artist_prior_plays"),
        F.when(has_artist, _rate(F.sum(skip).over(by_artist), F.count(skip).over(by_artist))).alias(
            "artist_prior_skip_rate"
        ),
        _rate(F.sum(skip).over(recent), F.count(skip).over(recent)).alias("user_recent_skip_rate"),
    )


def build_features(spark: SparkSession, locator: TableLocator) -> int:
    """Lee Oro, escribe gold.ml_skip_features completa y verifica una fila por reproduccion."""
    fact = read_table(spark, locator, GOLD_FACT_PLAYS)
    artists = read_table(spark, locator, GOLD_DIM_ARTIST)
    overwrite(spark, skip_features(fact, artists), locator, GOLD_ML_SKIP_FEATURES)
    written = read_table(spark, locator, GOLD_ML_SKIP_FEATURES)
    rows, expected, labeled = written.count(), fact.count(), written.where("is_skip IS NOT NULL").count()
    if rows != expected:
        raise LakehouseError(f"ml_skip_features tiene {rows} filas y fact_plays {expected}: deben cuadrar")
    log.info("Features: %d filas (%d con etiqueta, %d sin etiqueta)", rows, labeled, rows - labeled)
    return rows
