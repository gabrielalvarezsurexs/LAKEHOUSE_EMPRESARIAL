"""Pasos del pipeline: que CLI corre cada uno, con que usuario de MinIO y que tablas produce.

Dagster solo envuelve estos comandos (D7). Cada paso es un proceso aparte con el usuario de su rol (D17),
porque una SparkSession no cambia de credenciales; el grafo sale de los sources de los contratos (D23).
Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

from __future__ import annotations

import collections
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass

from lakehouse.common.errors import LakehouseError
from lakehouse.governance.contracts import (
    ALL_TABLES,
    BRONZE_INGESTION_LOG,
    BRONZE_MUSICBRAINZ_ARTISTS,
    BRONZE_QUARANTINE,
    BRONZE_SPOTIFY_STREAMS,
    GOLD_ML_SKIP_FEATURES,
    GOLD_ML_SKIP_PREDICTIONS,
    GOLD_TABLES,
    ML_MODEL_NODE,
    SILVER_TABLES,
    SPOTIFY_STREAMS_SOURCE,
)

# Cache cruda de MinIO: no es tabla Delta, pero la escribe el paso de generos (D9).
MUSICBRAINZ_CACHE = "landing.musicbrainz_cache"
# Nodos sin contrato: la cache de MusicBrainz y el modelo registrado en MLflow (F6).
EXTRA_SOURCES: dict[str, tuple[str, ...]] = {
    MUSICBRAINZ_CACHE: ("ext.musicbrainz", BRONZE_SPOTIFY_STREAMS.qualified_name),
    ML_MODEL_NODE: (GOLD_ML_SKIP_FEATURES.qualified_name,),
}
# Mismo nombre que ml/registry.py (los roles no se importan entre si; una prueba los compara).
ML_MODEL_NAME = "lakehouse_skip"
# Fuentes de fuera del lakehouse: aparecen en el grafo, pero nada las materializa.
EXTERNAL_SOURCES: dict[str, str] = {
    "ext.spotify": "Export del historial extendido de Spotify (JSON) en la carpeta SPOTIFY_DATA_DIR.",
    "ext.musicbrainz": "API publica de MusicBrainz (generos por artista), 1 peticion por segundo.",
    "ext.platform_catalog": "Catalogo fijo de plataformas en transformation/catalogs.py.",
}
GOVERNANCE_MODULE = "lakehouse.governance"
TAIL_LINES = 30


class StepError(LakehouseError):
    """Un comando del pipeline termino mal; trae las ultimas lineas de su salida."""

    def __init__(self, message: str, code: int, tail: list[str]):
        super().__init__(message)
        self.code = code
        self.tail = tail


@dataclass(frozen=True)
class Step:
    """Un paso: python -m <module> <args> con el usuario svc-<role>; produce outputs ('capa.tabla')."""

    name: str
    role: str
    module: str
    args: tuple[str, ...]
    outputs: tuple[str, ...]
    description: str
    retries: int = 0

    def command(self) -> list[str]:
        return python_command(self.module, *self.args)


def python_command(module: str, *args: str) -> list[str]:
    """Mismo interprete que Dagster (el venv del contenedor)."""
    return [sys.executable, "-m", module, *args]


def _names(tables) -> tuple[str, ...]:
    return tuple(t.qualified_name for t in tables)


STEPS: tuple[Step, ...] = (
    Step(
        "landing",
        "ingestion",
        "lakehouse.ingestion",
        ("landing",),
        (SPOTIFY_STREAMS_SOURCE.qualified_name,),
        "Sube a landing los JSON nuevos de la carpeta de origen (idempotente por SHA-256).",
    ),
    Step(
        "bronze",
        "ingestion",
        "lakehouse.ingestion",
        ("bronze",),
        _names((BRONZE_SPOTIFY_STREAMS, BRONZE_QUARANTINE, BRONZE_INGESTION_LOG)),
        "Landing a Bronce: valida contra el contrato, seudonimiza la IP y manda lo malo a cuarentena.",
    ),
    Step(
        "genres",
        "ingestion",
        "lakehouse.ingestion",
        ("genres",),
        (MUSICBRAINZ_CACHE, BRONZE_MUSICBRAINZ_ARTISTS.qualified_name),
        "Generos por artista (MusicBrainz o sinteticos segun GENRES_SOURCE), con cache en landing.",
        # Unico paso con red externa: MusicBrainz responde 503 seguido (F2).
        retries=2,
    ),
    Step(
        "silver",
        "transformation",
        "lakehouse.transformation",
        ("silver",),
        _names(SILVER_TABLES),
        "Bronce a Plata: tipado, dedup, hora local, sesiones, skips y artistas con genero.",
    ),
    Step(
        "gold",
        "transformation",
        "lakehouse.transformation",
        ("gold",),
        _names(GOLD_TABLES),
        "Plata a Oro: dimensiones, MERGE de fact_plays, OPTIMIZE y marts de KPIs.",
    ),
    Step(
        "ml_features",
        "ml",
        "lakehouse.ml",
        ("features",),
        (GOLD_ML_SKIP_FEATURES.qualified_name,),
        "Features sin fuga para la prediccion de skip (solo historia previa a cada reproduccion).",
    ),
    Step(
        "ml_train",
        "ml",
        "lakehouse.ml",
        ("train",),
        (ML_MODEL_NODE,),
        "Split temporal, modelo contra baseline, registro en MLflow y ML_METRICAS.md.",
    ),
    Step(
        "ml_predict",
        "ml",
        "lakehouse.ml",
        ("predict",),
        (GOLD_ML_SKIP_PREDICTIONS.qualified_name,),
        "Inferencia por lote con el modelo de MLflow (champion o el mas reciente).",
    ),
)

_CONTRACTS = {t.qualified_name: t for t in ALL_TABLES}


def contract_of(node: str):
    """Contrato de un nodo 'capa.tabla', o None si no es tabla con contrato."""
    return _CONTRACTS.get(node)


def sources_of(node: str) -> tuple[str, ...]:
    contract = contract_of(node)
    return contract.sources if contract else EXTRA_SOURCES.get(node, ())


def produced_nodes() -> list[str]:
    return [node for step in STEPS for node in step.outputs]


def step_of(node: str) -> Step:
    for step in STEPS:
        if node in step.outputs:
            return step
    raise KeyError(f"Ningun paso produce {node}")


def run_command(cmd: list[str], on_line: Callable[[str], None], ok_codes: tuple[int, ...] = (0,)) -> int:
    """Corre cmd mandando cada linea a on_line; si lo cancelan, termina tambien al hijo.

    Devuelve el codigo de salida; si no esta en ok_codes, lanza StepError con las ultimas lineas.
    """
    tail: collections.deque[str] = collections.deque(maxlen=TAIL_LINES)
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    try:
        for raw in proc.stdout:
            line = raw.rstrip()
            if line:
                tail.append(line)
                on_line(line)
        code = proc.wait()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
    if code not in ok_codes:
        shown = " ".join(cmd[2:]) if cmd[:1] == [sys.executable] else " ".join(cmd)
        raise StepError(f"'{shown}' termino con codigo {code}", code, list(tail))
    return code
