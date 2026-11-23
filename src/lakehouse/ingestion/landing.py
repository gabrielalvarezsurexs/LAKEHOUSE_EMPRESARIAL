"""Carga a landing: sube los JSON de Spotify intactos a s3://landing/spotify/<fecha>/ con su SHA-256.

Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.common.storage import ObjectStore
from lakehouse.ingestion.records import file_sha256

log = get_logger("lakehouse.ingestion.landing")

LANDING_BUCKET = "landing"
SPOTIFY_PREFIX = "spotify/"
SYNTHETIC_MARKER = "SINTETICO"  # el generador lo pone en el nombre de sus archivos


class IngestionError(LakehouseError):
    """Error de ingesta con mensaje accionable."""


@dataclass
class LandingResult:
    uploaded: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)  # mismo contenido ya estaba en landing


def _is_synthetic(name: str) -> bool:
    return SYNTHETIC_MARKER in name.upper()


def _existing_hashes(store: ObjectStore) -> dict[str, str]:
    """sha256 -> llave de lo que ya hay en landing/spotify."""
    hashes = {}
    for key in store.list_keys(LANDING_BUCKET, SPOTIFY_PREFIX):
        meta = store.get_metadata(LANDING_BUCKET, key) or {}
        if meta.get("sha256"):
            hashes[meta["sha256"]] = key
    return hashes


def _check_not_mixed(existing_keys: list[str], local_names: list[str]) -> None:
    kinds = {_is_synthetic(Path(k).name) for k in existing_keys} | {_is_synthetic(n) for n in local_names}
    if len(kinds) > 1:
        raise IngestionError(
            "No se mezclan datos sinteticos y reales en el mismo MinIO. Para cambiar de tipo de datos, "
            "borra el volumen: docker compose down -v"
        )


def upload_spotify_files(
    store: ObjectStore, source_dir: str | Path, pattern: str = "*.json", today: date | None = None
) -> LandingResult:
    """Sube cada archivo cuyo contenido (SHA-256) aun no este en landing; re-correr no duplica."""
    source = Path(source_dir)
    files = sorted(p for p in source.glob(pattern) if p.is_file())
    if not files:
        raise IngestionError(f"No hay archivos '{pattern}' en {source}. Revisa SPOTIFY_DATA_DIR en tu .env.")
    _check_not_mixed(store.list_keys(LANDING_BUCKET, SPOTIFY_PREFIX), [p.name for p in files])

    day = (today or datetime.now(UTC).date()).isoformat()
    existing = _existing_hashes(store)
    result = LandingResult()
    for path in files:
        content = path.read_bytes()
        sha = file_sha256(content)
        if sha in existing:
            log.info("Ya estaba en landing (mismo SHA-256): %s", path.name)
            result.skipped.append(existing[sha])
            continue
        key = f"{SPOTIFY_PREFIX}{day}/{path.name}"
        if store.get_metadata(LANDING_BUCKET, key) is not None:
            # Mismo nombre el mismo dia pero otro contenido: no se pisa el crudo anterior.
            key = f"{SPOTIFY_PREFIX}{day}/{path.stem}_{sha[:12]}{path.suffix}"
        # Los metadatos de S3 viajan como headers HTTP: solo ASCII.
        original = path.name.encode("ascii", "replace").decode("ascii")
        store.put_bytes(LANDING_BUCKET, key, content, {"sha256": sha, "original-name": original})
        existing[sha] = key
        result.uploaded.append(key)
        log.info("Subido a landing: %s (%d bytes)", key, len(content))
    return result
