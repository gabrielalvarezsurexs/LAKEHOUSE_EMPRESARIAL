"""Pruebas del cliente y del match de MusicBrainz con respuestas grabadas (sin red).

Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

import io
import json
import urllib.error
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse.ingestion.musicbrainz import (
    ApiSource,
    MusicBrainzClient,
    MusicBrainzError,
    NotFoundError,
    cache_key,
    choose_candidate,
    doc_to_row,
    fetch_missing,
    lucene_escape,
    normalize_name,
)

FIXTURES = Path(__file__).parent / "fixtures"
SEARCH = json.loads((FIXTURES / "mb_search.json").read_text(encoding="utf-8"))
ARTIST = json.loads((FIXTURES / "mb_artist.json").read_text(encoding="utf-8"))
UA = "lakehouse-pruebas/0.1"


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeOpener:
    """Devuelve las respuestas en orden; un int es un error HTTP con ese codigo."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append(request)
        item = self.responses.pop(0)
        if isinstance(item, int):
            raise urllib.error.HTTPError(request.full_url, item, "error", {}, None)
        return io.BytesIO(json.dumps(item).encode("utf-8"))


def make_client(*responses, max_attempts=3):
    fake_time, opener = FakeTime(), FakeOpener(*responses)
    client = MusicBrainzClient(
        UA, max_attempts=max_attempts, opener=opener, sleep=fake_time.sleep, clock=fake_time.clock
    )
    return client, opener, fake_time


def test_sends_generic_user_agent_and_json_format():
    client, opener, _ = make_client(SEARCH)
    client.search_artist('Los "Ejemplos"')
    request = opener.requests[0]
    assert request.get_header("User-agent") == UA
    assert "fmt=json" in request.full_url
    assert "limit=5" in request.full_url


def test_waits_one_second_between_requests():
    client, _, fake_time = make_client(SEARCH, ARTIST)
    client.search_artist("Los Ejemplos")
    client.lookup_artist(ARTIST["id"])
    assert fake_time.sleeps == [1.0]


def test_retries_503_then_succeeds():
    client, opener, fake_time = make_client(503, 503, SEARCH)
    assert client.search_artist("Los Ejemplos") == SEARCH
    assert len(opener.requests) == 3
    assert client.retries == 2
    assert 2.0 in fake_time.sleeps and 4.0 in fake_time.sleeps


def test_gives_up_after_max_attempts():
    client, _, _ = make_client(503, 503, 503)
    with pytest.raises(MusicBrainzError, match="3 intentos"):
        client.search_artist("Los Ejemplos")


def test_404_and_400_are_not_retried():
    client, opener, _ = make_client(404)
    with pytest.raises(NotFoundError):
        client.lookup_artist("x")
    client, opener, _ = make_client(400)
    with pytest.raises(MusicBrainzError, match="400"):
        client.search_artist("x")
    assert len(opener.requests) == 1


def test_lucene_escape_quotes_and_backslash():
    assert lucene_escape('a"b\\c') == 'a\\"b\\\\c'


def test_normalize_ignores_case_accents_and_signs():
    assert normalize_name("Los Ejémplos!") == normalize_name("los ejemplos") == "los ejemplos"


@pytest.mark.parametrize(
    ("name", "expected_id"),
    [
        ("Los Ejemplos", "11111111-2222-3333-4444-555555555555"),  # sin acento
        ("The Examples", "11111111-2222-3333-4444-555555555555"),  # por alias
        ("Los Ejemplos Tributo", None),  # nombre igual pero score bajo
        ("Ejemplos", None),  # score alto pero el nombre no coincide
    ],
)
def test_choose_candidate(name, expected_id):
    match = choose_candidate(name, SEARCH)
    assert (match["id"] if match else None) == expected_id


def test_api_source_builds_doc_and_row():
    client, _, _ = make_client(SEARCH, ARTIST)
    doc = ApiSource(client).fetch_doc("Los Ejemplos")
    assert doc["status"] == "matched"
    assert doc["match"]["score"] == 100
    row = doc_to_row(doc, cache_key("api", "Los Ejemplos"), datetime.now(UTC))
    assert row["mbid"] == ARTIST["id"]
    # Ordenados por votos y luego por nombre.
    assert row["genres"] == [("pop rock", 5), ("latin rock", 3), ("rock en español", 3)]


def test_no_match_skips_lookup():
    client, opener, _ = make_client(SEARCH)
    doc = ApiSource(client).fetch_doc("Artista Desconocido")
    assert doc["status"] == "no_match"
    assert doc["artist"] is None
    assert len(opener.requests) == 1


class CountingSource:
    name = "api"

    def __init__(self, fail_on=()):
        self.calls = []
        self.fail_on = set(fail_on)

    def fetch_doc(self, artist_name):
        self.calls.append(artist_name)
        if artist_name in self.fail_on:
            raise MusicBrainzError("caida")
        return {"status": "matched", "query_name": artist_name}


def test_fetch_missing_uses_cache_and_does_not_cache_errors(store):
    source = CountingSource(fail_on={"C"})
    first = fetch_missing(store, source, ["A", "B", "C"])
    assert (first.fetched, first.errors) == (2, 1)
    second = fetch_missing(store, source, ["A", "B", "C"])
    assert source.calls == ["A", "B", "C", "C"]  # solo reintenta el que fallo
    assert second.already_cached == 2


def test_fetch_missing_respects_limit(store):
    result = fetch_missing(store, CountingSource(), ["A", "B", "C"], limit=1)
    assert (result.fetched, result.pending) == (1, 2)


def test_cache_key_does_not_contain_the_name():
    assert "Ejemplos" not in cache_key("api", "Los Ejemplos")
