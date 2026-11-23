"""Fixtures de transformacion: escribe Bronce a mano (filas chicas y controladas) en un directorio temporal.

Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from datetime import UTC, datetime

import pytest

from lakehouse.common.delta import TableLocator
from lakehouse.governance.contracts import BRONZE_MUSICBRAINZ_ARTISTS, BRONZE_SPOTIFY_STREAMS

NOW = datetime(2026, 11, 23, tzinfo=UTC)


def utc(text):
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def bronze_row(index, end, ms, track=None, **fields):
    """Una fila de Bronce con valores normales; fields sobreescribe cualquier columna."""
    row = dict.fromkeys(BRONZE_SPOTIFY_STREAMS.column_names)
    row.update(
        ts=utc(end),
        ms_played=ms,
        platform="iOS 17.4 (iPhone15,2)",
        conn_country="MX",
        ip_addr_hash="hash",
        reason_start="clickrow",
        reason_end="trackdone",
        shuffle=False,
        skipped=False,
        offline=False,
        incognito_mode=False,
        _source_file="spotify/2026-11-23/a.json",
        _file_sha256="a" * 64,
        _record_index=index,
        _batch_id="lote",
        _ingested_at=NOW,
    )
    if track:
        uri, artist = track
        row.update(
            spotify_track_uri=f"spotify:track:{uri}",
            master_metadata_track_name=f"Cancion {uri}",
            master_metadata_album_artist_name=artist,
            master_metadata_album_album_name=f"Album {uri}",
        )
    row.update(fields)
    return row


@pytest.fixture
def write_bronze(spark, tmp_path):
    """write_bronze(rows, artists=None) -> TableLocator con Bronce escrito en tmp_path."""
    locator = TableLocator.at(str(tmp_path))

    def write(rows, artists=None):
        for contract, data in ((BRONZE_SPOTIFY_STREAMS, rows), (BRONZE_MUSICBRAINZ_ARTISTS, artists)):
            if data is None:
                continue
            names = contract.column_names
            df = spark.createDataFrame(
                [tuple(r.get(n) for n in names) for r in data], contract.spark_schema()
            )
            df.write.format("delta").mode("overwrite").save(locator.uri(contract))
        return locator

    return write


def mb_artist(name, status, genres=()):
    return {
        "artist_name": name,
        "status": status,
        "mbid": "00000000-0000-0000-0000-000000000000" if status == "matched" else None,
        "genres": list(genres),
        "_source_file": f"musicbrainz/synthetic/{name}.json",
        "fetched_at": NOW,
        "_ingested_at": NOW,
    }


# Con --import-mode=importlib las pruebas no pueden importar este conftest: los helpers van como fixtures.
@pytest.fixture
def make_row():
    return bronze_row


@pytest.fixture
def make_artist():
    return mb_artist
