"""Datos sinteticos con el esquema exacto del historial de Spotify (D14), para CI y replicas sin datos reales.

Artistas, canciones e IPs inventados (IPs de los rangos de documentacion, RFC 5737/3849). Los generos tambien
se inventan aqui (SyntheticSource), asi CI nunca llama a MusicBrainz.
Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from __future__ import annotations

import hashlib
import json
import random
import string
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from lakehouse.governance.contracts import SPOTIFY_STREAMS_SOURCE
from lakehouse.ingestion.landing import SYNTHETIC_MARKER
from lakehouse.ingestion.musicbrainz import make_doc

ARTIST_PREFIX = "Artista Sintetico"
GENRES = (
    "pop",
    "rock",
    "hip hop",
    "reggaeton",
    "indie",
    "electronic",
    "latin pop",
    "r&b",
    "jazz",
    "metal",
    "folk",
    "classical",
    "regional mexicano",
    "punk",
    "soul",
)
PLATFORMS = (
    "Android OS 13 API 33 (Sintetico, Phone)",
    "Android OS 14 API 34 (Sintetico, Phone)",
    "iOS 17.4 (iPhone15,2)",
    "iOS 18.1 (iPhone16,1)",
    "windows",
    "osx",
    "web_player windows 10;chrome 120",
    "Partner sonos_speaker",
)
COUNTRIES = (("MX", 0.9), ("US", 0.07), ("ES", 0.03))
_B62 = string.ascii_letters + string.digits


def _uri(rng: random.Random, kind: str) -> str:
    return f"spotify:{kind}:" + "".join(rng.choice(_B62) for _ in range(22))


def _ip(rng: random.Random) -> str:
    if rng.random() < 0.02:
        return f"2001:db8::{rng.randrange(1, 0xFFFF):x}"
    net = rng.choice(("192.0.2", "198.51.100", "203.0.113"))
    return f"{net}.{rng.randrange(1, 255)}"


def _catalog(rng: random.Random, n_artists: int) -> list[dict[str, Any]]:
    tracks = []
    for a in range(1, n_artists + 1):
        artist = f"{ARTIST_PREFIX} {a:04d}"
        for t in range(1, rng.randint(3, 12) + 1):
            tracks.append(
                {
                    "artist": artist,
                    "track": f"Cancion {a:04d}-{t:02d}",
                    "album": f"Album {a:04d}-{(t - 1) // 6 + 1}",
                    "uri": _uri(rng, "track"),
                    "duration": rng.randint(120_000, 300_000),
                    # Popularidad tipo Zipf: pocos artistas concentran muchas reproducciones.
                    "weight": 1.0 / (a**1.1),
                }
            )
    return tracks


def _play(rng: random.Random, item: dict[str, Any] | None, prev_end: str | None) -> dict[str, Any]:
    """Una reproduccion con campos de cancion/episodio; los motivos siguen al motivo de fin anterior."""
    if prev_end in ("trackdone", "fwdbtn", "backbtn"):
        reason_start = prev_end
    else:
        reason_start = rng.choice(("clickrow", "playbtn", "appload", "clickrow"))
    duration = item["duration"] if item else rng.randint(900_000, 3_600_000)
    roll = rng.random()
    if roll < 0.62:
        ms, reason_end = duration, "trackdone"
    elif roll < 0.88:
        ms, reason_end = rng.randint(800, 30_000), rng.choice(("fwdbtn",) * 9 + ("backbtn",))
    else:
        ms = rng.randint(0, duration)
        reason_end = rng.choice(("endplay", "logout", "unexpected-exit-while-paused", "unknown"))
    return {"ms": ms, "reason_start": reason_start, "reason_end": reason_end}


def generate_streams(
    n_rows: int, seed: int = 42, start: datetime | None = None, end: datetime | None = None
) -> list[dict[str, Any]]:
    """Lista de reproducciones ordenadas por ts, con las 23 llaves del contrato en el mismo orden."""
    rng = random.Random(seed)
    start = start or datetime(2019, 1, 1, tzinfo=UTC)
    end = end or datetime(2026, 3, 1, tzinfo=UTC)
    catalog = _catalog(rng, max(20, n_rows // 12))
    weights = [t["weight"] for t in catalog]
    episodes = [
        {"show": f"Podcast Sintetico {i}", "name": f"Episodio {i}-{j}", "uri": _uri(rng, "episode")}
        for i in range(1, 4)
        for j in range(1, 6)
    ]
    ips = [_ip(rng) for _ in range(12)]
    countries, country_w = zip(*COUNTRIES, strict=True)

    rows: list[dict[str, Any]] = []
    span = (end - start).total_seconds()
    while len(rows) < n_rows:
        # Una sesion: arranca a una hora al azar y encadena reproducciones una tras otra.
        clock = start + timedelta(seconds=rng.uniform(0, span))
        platform, country, ip = rng.choice(PLATFORMS), rng.choices(countries, country_w)[0], rng.choice(ips)
        shuffle, offline = rng.random() < 0.4, rng.random() < 0.12
        prev_end = None
        for _ in range(min(rng.randint(3, 25), n_rows - len(rows))):
            kind = rng.random()
            item = rng.choices(catalog, weights)[0] if kind >= 0.007 else None
            episode = rng.choice(episodes) if 0.0025 <= kind < 0.007 else None
            play = _play(rng, item, prev_end)
            prev_end = play["reason_end"]
            clock += timedelta(milliseconds=play["ms"] + rng.randint(0, 4000))
            values = {
                "ts": clock.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "platform": platform,
                "ms_played": play["ms"],
                "conn_country": country,
                "ip_addr": ip,
                "master_metadata_track_name": item["track"] if item else None,
                "master_metadata_album_artist_name": item["artist"] if item else None,
                "master_metadata_album_album_name": item["album"] if item else None,
                "spotify_track_uri": item["uri"] if item else None,
                "episode_name": episode["name"] if episode else None,
                "episode_show_name": episode["show"] if episode else None,
                "spotify_episode_uri": episode["uri"] if episode else None,
                "reason_start": play["reason_start"],
                "reason_end": play["reason_end"],
                "shuffle": shuffle,
                # Spotify marca pocos skips: solo una parte de los fwdbtn llega con skipped=true.
                "skipped": play["reason_end"] == "fwdbtn" and rng.random() < 0.3,
                "offline": offline,
                "offline_timestamp": int(clock.timestamp()) - rng.randint(60, 86_400) if offline else None,
                "incognito_mode": rng.random() < 0.01,
            }
            # Orden y llaves salen del contrato; los campos de audiolibro siempre llegan null.
            rows.append({name: values.get(name) for name in SPOTIFY_STREAMS_SOURCE.column_names})
    rows.sort(key=lambda r: r["ts"])
    return rows


def write_files(rows: list[dict[str, Any]], out_dir: str | Path, n_files: int = 2) -> list[Path]:
    """Parte las filas en n_files JSON con nombre parecido al de Spotify y la marca de sinteticos."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    size = -(-len(rows) // n_files)
    paths = []
    for i in range(n_files):
        chunk = rows[i * size : (i + 1) * size]
        if not chunk:
            break
        years = f"{chunk[0]['ts'][:4]}-{chunk[-1]['ts'][:4]}"
        path = out / f"Streaming_History_Audio_{SYNTHETIC_MARKER}_{years}_{i}.json"
        path.write_text(json.dumps(chunk, ensure_ascii=False, indent=1), encoding="utf-8")
        paths.append(path)
    return paths


def synthetic_genres(artist_name: str) -> list[dict[str, Any]] | None:
    """Generos fijos por artista (deterministas); None para ~8% de artistas y para nombres no sinteticos."""
    if not artist_name.startswith(ARTIST_PREFIX):
        return None
    h = int(hashlib.sha256(artist_name.encode("utf-8")).hexdigest(), 16)
    if h % 100 < 8:
        return None
    picks = sorted({GENRES[(h >> (8 * k)) % len(GENRES)] for k in range(1 + h % 3)})
    return [{"name": g, "count": 1 + (h >> (4 * i)) % 9} for i, g in enumerate(picks)]


class SyntheticSource:
    """Imita las respuestas de MusicBrainz para artistas sinteticos, sin red."""

    name = "synthetic"

    def fetch_doc(self, artist_name: str) -> dict[str, Any]:
        genres = synthetic_genres(artist_name)
        if genres is None:
            return make_doc(artist_name, self.name, {"artists": []}, None, None)
        mbid = str(hashlib.md5(artist_name.encode("utf-8"), usedforsecurity=False).hexdigest())
        mbid = f"{mbid[:8]}-{mbid[8:12]}-{mbid[12:16]}-{mbid[16:20]}-{mbid[20:]}"
        candidate = {"id": mbid, "name": artist_name, "sort-name": artist_name, "score": 100}
        artist = {"id": mbid, "name": artist_name, "genres": genres}
        return make_doc(artist_name, self.name, {"artists": [candidate]}, candidate, artist)
