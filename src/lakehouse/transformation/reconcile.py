"""Reconciliacion de conteos entre capas: cada fila de Bronce tiene destino y Oro cuadra con Plata.

Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from lakehouse.common.delta import TableLocator, read_table
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import (
    BRONZE_SPOTIFY_STREAMS,
    GOLD_FACT_PLAYS,
    GOLD_KPI_LISTENING_DAILY,
    SILVER_DISCARDED,
    SILVER_EPISODES,
    SILVER_PLAYS,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = get_logger("lakehouse.transformation.reconcile")


class ReconciliationError(LakehouseError):
    """Los conteos entre capas no cuadran."""


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


@dataclass
class Reconciliation:
    counts: dict[str, int]
    discarded_by_cause: dict[str, int]
    checks: list[Check] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)


def _check(name: str, left: int, right: int, formula: str) -> Check:
    return Check(name, left == right, f"{formula}: {left} vs {right}")


def reconcile(spark: SparkSession, locator: TableLocator) -> Reconciliation:
    """Calcula los conteos y las igualdades que deben cumplirse; no lanza error (lo decide quien llama)."""
    from pyspark.sql import functions as F

    def rows(contract):
        return read_table(spark, locator, contract)

    fact = rows(GOLD_FACT_PLAYS)
    counts = {
        "bronze.spotify_streams": rows(BRONZE_SPOTIFY_STREAMS).count(),
        "silver.plays": rows(SILVER_PLAYS).count(),
        "silver.episodes": rows(SILVER_EPISODES).count(),
        "silver._discarded": rows(SILVER_DISCARDED).count(),
        "gold.fact_plays": fact.count(),
        "gold.fact_plays_distinct_play_id": fact.select("play_id").distinct().count(),
        "gold.fact_plays_with_date": fact.where("date_key IS NOT NULL").count(),
        "gold.kpi_listening_daily_plays": rows(GOLD_KPI_LISTENING_DAILY).agg(F.sum("plays")).first()[0] or 0,
    }
    causes = {r["cause"]: r["count"] for r in rows(SILVER_DISCARDED).groupBy("cause").count().collect()}
    c = counts
    result = Reconciliation(counts, dict(sorted(causes.items())))
    result.checks = [
        _check(
            "bronce = plata + descartes",
            c["bronze.spotify_streams"],
            c["silver.plays"] + c["silver.episodes"] + c["silver._discarded"],
            "spotify_streams = plays + episodes + _discarded",
        ),
        _check("plata = oro", c["silver.plays"], c["gold.fact_plays"], "plays = fact_plays"),
        _check(
            "play_id unico en oro",
            c["gold.fact_plays"],
            c["gold.fact_plays_distinct_play_id"],
            "filas = play_id distintos",
        ),
        _check(
            "kpi diario = fact con fecha",
            c["gold.fact_plays_with_date"],
            c["gold.kpi_listening_daily_plays"],
            "fact_plays con date_key = suma de plays diarios",
        ),
    ]
    log.info("Reconciliacion: %s", ", ".join(f"{k}={v}" for k, v in counts.items()))
    log.info("Reconciliacion: descartes por causa %s", result.discarded_by_cause)
    for check in result.checks:
        (log.info if check.ok else log.error)(
            "[%s] %s (%s)", "OK" if check.ok else "FALLA", check.name, check.detail
        )
    return result
