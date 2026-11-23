"""Bronce a Plata con Spark sobre filas hechas a mano: cada regla de Plata con su caso.

Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from datetime import date

import pytest

from lakehouse.common.delta import read_table
from lakehouse.governance.contracts import SILVER_ARTISTS, SILVER_DISCARDED, SILVER_EPISODES, SILVER_PLAYS
from lakehouse.transformation.catalogs import COUNTRY_TIMEZONES, platform_family, skip_label
from lakehouse.transformation.silver import build_silver, platform_family_expr, skip_label_expr

pytestmark = pytest.mark.spark

UNO, DOS, TRES = "Artista Uno", "Artista Dos", "Artista Tres"


def scenario(row):
    """12 filas de Bronce: 8 canciones, 1 episodio, 1 duplicado, 1 sin contenido y 1 audiolibro."""
    win = "Windows 10 (10.0.19045; x64)"
    return [
        # Sesion 1 (10 ene, MX): inicio 17:56:40 UTC = 11:56:40 en Ciudad de Mexico.
        row(0, "2024-01-10T18:00:00", 200_000, ("A", UNO), platform=win),
        row(1, "2024-01-10T18:00:00", 200_000, ("A", UNO), platform="otro"),  # duplicado de la 0
        row(2, "2024-01-10T18:01:00", 10_000, ("B", UNO), conn_country="ZZ", reason_end="fwdbtn"),
        row(
            3, "2024-01-10T18:02:00", 20_000, ("C", DOS), reason_end="endplay", platform="web_player windows"
        ),
        row(4, "2024-01-10T18:05:00", 40_000, ("C", DOS), reason_end="endplay"),
        row(5, "2024-01-10T18:06:00", 1_000, ("D", TRES), reason_end="logout"),
        row(6, "2024-01-10T18:07:00", 5_000, ("D", TRES), reason_end="", reason_start="  "),
        row(7, "2024-01-10T18:08:00", 0),  # ni cancion ni episodio
        # Sesion 2: el episodio empieza 19:50, mas de 30 min despues del fin anterior.
        row(
            8,
            "2024-01-10T20:00:00",
            600_000,
            spotify_episode_uri="spotify:episode:E",
            episode_name="Episodio",
            episode_show_name="Podcast",
        ),
        # Sesion 3 (US, verano): 16:00 UTC = 12:00 en Nueva York; offline_timestamp en milisegundos.
        row(
            9,
            "2024-07-01T16:03:20",
            200_000,
            ("E", TRES),
            conn_country="US",
            offline=True,
            offline_timestamp=1_719_849_600_000,
        ),
        # Sesion 4: solo ZZ, sin pais en toda la sesion; offline_timestamp en segundos.
        row(
            10,
            "2024-08-01T10:00:00",
            100_000,
            ("A", UNO),
            conn_country="ZZ",
            platform=None,
            offline_timestamp=1_722_506_400,
        ),
        row(11, "2024-08-02T10:00:00", 1_000, audiobook_uri="spotify:audiobook:X"),
    ]


def by_track(locator, spark):
    """Plays por (letra de la cancion, HH:mm de fin). Las horas las formatea Spark (sesion en UTC)."""
    from pyspark.sql import functions as F

    rows = (
        read_table(spark, locator, SILVER_PLAYS)
        .withColumn("_end", F.date_format("ts_end", "HH:mm"))
        .withColumn("_offline", F.date_format("offline_at", "yyyy-MM-dd HH:mm"))
        .collect()
    )
    return {(r["track_uri"][-1], r["_end"]): r for r in rows}


def test_silver_rules(spark, write_bronze, make_row, make_artist):
    artists = [
        make_artist(UNO, "matched", [("rock", 2), ("pop", 5), ("indie", 5)]),
        make_artist(DOS, "no_match"),
    ]
    locator = write_bronze(scenario(make_row), artists)
    result = build_silver(spark, locator)

    assert (result.bronze_rows, result.plays, result.episodes) == (12, 8, 1)
    assert result.discarded == {"duplicate": 1, "no_content": 1, "audiobook_not_supported": 1}
    assert result.sessions == 4
    assert (result.labeled, result.skips, result.local_time_unknown) == (6, 2, 1)

    plays = by_track(locator, spark)
    first = plays[("A", "18:00")]
    assert first["platform_family"] == "windows" and first["tz_source"] == "country"
    assert (first["local_date"], first["local_hour"], first["local_weekday"]) == (date(2024, 1, 10), 11, 3)
    assert first["session_id"] == "20240110T175640" and first["session_seq"] == 1

    zz = plays[("B", "18:01")]
    assert zz["conn_country"] is None and zz["tz_source"] == "session"
    assert zz["tz_name"] == "America/Mexico_City" and zz["is_skip"] is True and zz["session_seq"] == 2

    assert plays[("C", "18:02")]["platform_family"] == "web" and plays[("C", "18:02")]["is_skip"] is True
    assert plays[("C", "18:05")]["is_skip"] is False
    assert plays[("D", "18:06")]["is_skip"] is None
    blank = plays[("D", "18:07")]
    assert blank["reason_end"] is None and blank["reason_start"] is None and blank["is_skip"] is None
    assert {p["session_id"] for k, p in plays.items() if k[1].startswith("18")} == {"20240110T175640"}

    us = plays[("E", "16:03")]
    assert (us["tz_name"], us["local_hour"], us["local_weekday"]) == ("America/New_York", 12, 1)
    assert us["_offline"] == "2024-07-01 16:00"

    alone = plays[("A", "10:00")]
    assert alone["tz_name"] is None and alone["local_date"] is None and alone["tz_source"] is None
    assert alone["platform_family"] is None and alone["_offline"] == "2024-08-01 10:00"

    episodes = read_table(spark, locator, SILVER_EPISODES).collect()
    assert [e["session_id"] for e in episodes] == ["20240110T195000"]
    assert "ip_addr_hash" not in read_table(spark, locator, SILVER_PLAYS).columns

    dup = read_table(spark, locator, SILVER_DISCARDED).where("cause = 'duplicate'").first()
    assert dup["_record_index"] == 1 and dup["duplicate_of"] == first["play_id"]

    genres = {r["artist_name"]: r for r in read_table(spark, locator, SILVER_ARTISTS).collect()}
    assert genres[UNO]["genres"] == ["indie", "pop", "rock"] and genres[UNO]["primary_genre"] == "indie"
    assert genres[DOS]["mb_status"] == "no_match" and genres[DOS]["genres"] is None
    assert genres[TRES]["mb_status"] == "not_queried" and genres[TRES]["primary_genre"] is None


def test_silver_is_idempotent_and_works_without_genres(spark, write_bronze, make_row):
    locator = write_bronze(scenario(make_row))
    first = build_silver(spark, locator)
    second = build_silver(spark, locator)
    assert first == second
    statuses = {r["mb_status"] for r in read_table(spark, locator, SILVER_ARTISTS).collect()}
    assert statuses == {"not_queried"}


def test_spark_rules_match_python_rules(spark):
    platforms = [
        "web_player windows",
        "Partner sonos_speaker",
        "iOS 17",
        "Windows 10",
        "OS X",
        "nuevo",
        "",
        None,
    ]
    reasons = ["fwdbtn", "backbtn", "trackdone", "endplay", "playbtn", "logout", "cosmos", None]
    rows = [(p, r, ms) for p in platforms for r in reasons for ms in (0, 29_999, 30_000)]
    df = spark.createDataFrame(rows, "platform STRING, reason_end STRING, ms_played BIGINT")
    out = df.select(
        "*", platform_family_expr("platform").alias("fam"), skip_label_expr().alias("skip")
    ).collect()
    for r in out:
        assert r["fam"] == platform_family(r["platform"])
        assert r["skip"] == skip_label(r["reason_end"], r["ms_played"])


def test_timezone_catalog_is_valid_for_spark(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(z,) for z in COUNTRY_TIMEZONES.values()], "zone STRING")
    # Una zona invalida hace fallar a Spark; una valida nunca da null.
    shifted = df.select(F.from_utc_timestamp(F.lit("2024-01-01 00:00:00").cast("timestamp"), F.col("zone")))
    values = [r[0] for r in shifted.collect()]
    assert len(values) == len(COUNTRY_TIMEZONES) and None not in values
