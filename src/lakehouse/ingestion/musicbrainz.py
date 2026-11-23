"""Generos por artista con MusicBrainz (D9): cliente respetuoso, cache cruda en landing y tabla de Bronce.

Una sola corrida contra la API: las respuestas se guardan en landing y las corridas siguientes solo piden
artistas nuevos. Los logs solo llevan conteos, nunca nombres de artistas (privacidad).
Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from lakehouse.common.config import Settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.common.storage import ObjectStore
from lakehouse.governance.contracts import BRONZE_MUSICBRAINZ_ARTISTS, BRONZE_SPOTIFY_STREAMS
from lakehouse.ingestion.bronze import BronzeTables
from lakehouse.ingestion.landing import LANDING_BUCKET

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger("lakehouse.ingestion.musicbrainz")

API_ROOT = "https://musicbrainz.org/ws/2"
CACHE_PREFIX = "musicbrainz/"
DOC_VERSION = 1
MIN_SCORE = 90  # debajo de esto la busqueda no es confiable: sin genero antes que uno falso
_RETRY_STATUS = {429, 500, 502, 503, 504}


class MusicBrainzError(LakehouseError):
    """La API no respondio bien despues de los reintentos."""


class NotFoundError(MusicBrainzError):
    """La API respondio 404."""


class MusicBrainzClient:
    """Cliente HTTP minimo: User-Agent generico, 1 peticion por segundo y reintentos con espera."""

    def __init__(
        self,
        user_agent: str,
        min_interval: float = 1.0,
        max_attempts: int = 5,
        timeout: float = 30.0,
        opener: Callable[..., Any] = urllib.request.urlopen,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.user_agent = user_agent
        self.min_interval = min_interval
        self.max_attempts = max_attempts
        self.timeout = timeout
        self._open, self._sleep, self._clock = opener, sleep, clock
        self._last_request: float | None = None
        self.requests_made = 0
        self.retries = 0  # 503 es normal en MusicBrainz: se cuentan en vez de llenar el log

    def _throttle(self) -> None:
        if self._last_request is not None:
            wait = self.min_interval - (self._clock() - self._last_request)
            if wait > 0:
                self._sleep(wait)
        self._last_request = self._clock()

    def get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        url = f"{API_ROOT}/{path}?{urllib.parse.urlencode({**params, 'fmt': 'json'})}"
        request = urllib.request.Request(
            url, headers={"User-Agent": self.user_agent, "Accept": "application/json"}
        )
        last_error = "sin intentos"
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            self.requests_made += 1
            try:
                with self._open(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    raise NotFoundError(f"404 en {path}") from exc
                if exc.code not in _RETRY_STATUS:
                    raise MusicBrainzError(f"HTTP {exc.code} en {path}") from exc
                last_error = f"HTTP {exc.code}"
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                backoff = float(retry_after) if retry_after and retry_after.isdigit() else 2.0**attempt
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = type(exc).__name__
                backoff = 2.0**attempt
            if attempt < self.max_attempts:
                self.retries += 1
                log.debug("MusicBrainz: %s (intento %d/%d)", last_error, attempt, self.max_attempts)
                self._sleep(backoff)
        raise MusicBrainzError(f"MusicBrainz no respondio tras {self.max_attempts} intentos: {last_error}")

    def search_artist(self, name: str, limit: int = 5) -> dict[str, Any]:
        return self.get("artist/", {"query": f'artist:"{lucene_escape(name)}"', "limit": str(limit)})

    def lookup_artist(self, mbid: str) -> dict[str, Any]:
        return self.get(f"artist/{urllib.parse.quote(mbid)}", {"inc": "genres"})


def lucene_escape(text: str) -> str:
    """Escapa lo que rompe una frase entre comillas en la busqueda de MusicBrainz (Lucene)."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


def normalize_name(name: str) -> str:
    """Minusculas, sin acentos ni signos: 'Beyonce!' y 'beyonce' cuentan como el mismo nombre."""
    decomposed = unicodedata.normalize("NFKD", name)
    no_marks = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(re.sub(r"[^\w]+", " ", no_marks.casefold()).split())


def choose_candidate(name: str, search: dict[str, Any], min_score: int = MIN_SCORE) -> dict[str, Any] | None:
    """Primer candidato con score alto y nombre (o alias) igual al buscado; si hay duda, ninguno."""
    target = normalize_name(name) or name.casefold()
    for candidate in search.get("artists", []):
        if int(candidate.get("score", 0)) < min_score:
            continue
        names = [candidate.get("name"), candidate.get("sort-name")]
        names += [alias.get("name") for alias in candidate.get("aliases", [])]
        if any(n and (normalize_name(n) or n.casefold()) == target for n in names):
            return candidate
    return None


def cache_key(source: str, name: str) -> str:
    """Llave del documento en landing; el hash evita meter el nombre (y caracteres raros) en la ruta."""
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:32]
    return f"{CACHE_PREFIX}{source}/{digest}.json"


def make_doc(
    name: str,
    source: str,
    search: dict[str, Any],
    match: dict[str, Any] | None,
    artist: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "doc_version": DOC_VERSION,
        "source": source,
        "query_name": name,
        "fetched_at": datetime.now(UTC).isoformat(),
        "status": "matched" if match else "no_match",
        "match": {"mbid": match["id"], "name": match.get("name"), "score": int(match.get("score", 0))}
        if match
        else None,
        "search": search,
        "artist": artist,
    }


class GenreSource(Protocol):
    name: str

    def fetch_doc(self, artist_name: str) -> dict[str, Any]: ...


class ApiSource:
    """Busca el artista y, si hay match confiable, pide sus generos (2 peticiones por artista)."""

    name = "api"

    def __init__(self, client: MusicBrainzClient):
        self.client = client

    def fetch_doc(self, artist_name: str) -> dict[str, Any]:
        search = self.client.search_artist(artist_name)
        match = choose_candidate(artist_name, search)
        artist = self.client.lookup_artist(match["id"]) if match else None
        return make_doc(artist_name, self.name, search, match, artist)


@dataclass
class EnrichResult:
    artists_total: int = 0
    already_cached: int = 0
    fetched: int = 0
    matched: int = 0
    errors: int = 0
    pending: int = 0  # quedaron fuera por --limit
    error_causes: dict[str, int] = field(default_factory=dict)


def artist_names_from_bronze(spark: SparkSession, tables: BronzeTables) -> list[str]:
    col = "master_metadata_album_artist_name"
    df = spark.read.format("delta").load(tables.uri(BRONZE_SPOTIFY_STREAMS))
    return sorted(r[0] for r in df.select(col).where(f"{col} IS NOT NULL").distinct().collect())


def fetch_missing(
    store: ObjectStore, source: GenreSource, names: list[str], limit: int | None = None
) -> EnrichResult:
    """Pide a la fuente solo los artistas sin documento en landing; los errores no se cachean."""
    cached = set(store.list_keys(LANDING_BUCKET, f"{CACHE_PREFIX}{source.name}/"))
    missing = [n for n in names if cache_key(source.name, n) not in cached]
    todo = missing if limit is None else missing[:limit]
    result = EnrichResult(len(names), len(names) - len(missing), pending=len(missing) - len(todo))
    if todo:
        log.info("MusicBrainz (%s): %d artistas por consultar", source.name, len(todo))
    for i, name in enumerate(todo, start=1):
        try:
            doc = source.fetch_doc(name)
        except MusicBrainzError as exc:
            result.errors += 1
            cause = type(exc).__name__
            result.error_causes[cause] = result.error_causes.get(cause, 0) + 1
            continue
        body = json.dumps(doc, ensure_ascii=False).encode("utf-8")
        store.put_bytes(LANDING_BUCKET, cache_key(source.name, name), body)
        result.fetched += 1
        result.matched += doc["status"] == "matched"
        if i % 100 == 0 or i == len(todo):
            log.info(
                "MusicBrainz: %d/%d (con match=%d, errores=%d)", i, len(todo), result.matched, result.errors
            )
    return result


def _genres(artist: dict[str, Any] | None) -> list[tuple[str, int]]:
    if not artist:
        return []
    genres = [(g["name"], int(g.get("count", 0))) for g in artist.get("genres", []) if g.get("name")]
    return sorted(genres, key=lambda g: (-g[1], g[0]))


def doc_to_row(doc: dict[str, Any], key: str, ingested_at: datetime) -> dict[str, Any]:
    match = doc.get("match") or {}
    return {
        "artist_name": doc["query_name"],
        "status": doc["status"],
        "mbid": match.get("mbid"),
        "mb_name": match.get("name"),
        "match_score": match.get("score"),
        "genres": _genres(doc.get("artist")),
        "raw_json": json.dumps(doc, ensure_ascii=False),
        "_source_file": key,
        "fetched_at": datetime.fromisoformat(doc["fetched_at"]),
        "_ingested_at": ingested_at,
    }


def build_bronze_artists(spark: SparkSession, store: ObjectStore, source: str, tables: BronzeTables) -> int:
    """Reescribe musicbrainz_artists con todos los documentos cacheados (tabla chica: overwrite)."""
    now = datetime.now(UTC)
    keys = store.list_keys(LANDING_BUCKET, f"{CACHE_PREFIX}{source}/")
    rows = [doc_to_row(json.loads(store.get_bytes(LANDING_BUCKET, k)), k, now) for k in keys]
    names = BRONZE_MUSICBRAINZ_ARTISTS.column_names
    df = spark.createDataFrame(
        [tuple(r[n] for n in names) for r in rows], BRONZE_MUSICBRAINZ_ARTISTS.spark_schema()
    )
    df.write.format("delta").mode("overwrite").save(tables.uri(BRONZE_MUSICBRAINZ_ARTISTS))
    return len(rows)


def enrich_genres(
    spark: SparkSession,
    store: ObjectStore,
    settings: Settings,
    source: GenreSource | None = None,
    tables: BronzeTables | None = None,
    limit: int | None = None,
) -> EnrichResult:
    """Artistas de Bronce > documentos en landing (solo los nuevos) > tabla musicbrainz_artists."""
    tables = tables or BronzeTables.from_settings(settings)
    source = source or ApiSource(MusicBrainzClient(settings.musicbrainz_user_agent))
    names = artist_names_from_bronze(spark, tables)
    result = fetch_missing(store, source, names, limit)
    rows = build_bronze_artists(spark, store, source.name, tables)
    retries = getattr(getattr(source, "client", None), "retries", 0)
    log.info(
        "Generos (%s): artistas=%d, ya en cache=%d, consultados=%d, con match=%d, errores=%d, pendientes=%d, "
        "reintentos=%d, filas en Bronce=%d",
        source.name,
        result.artists_total,
        result.already_cached,
        result.fetched,
        result.matched,
        result.errors,
        result.pending,
        retries,
        rows,
    )
    if result.errors:
        log.warning(
            "%d artistas fallaron tras los reintentos; vuelve a correr 'genres' para pedirlos.", result.errors
        )
    return result
