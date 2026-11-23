"""CLI de transformacion. Dentro del contenedor: python -m lakehouse.transformation <comando> (ver --help).

Comandos: silver (Bronce a Plata), gold (Plata a Oro), reconcile (conteos entre capas) y all (los tres).
Responsable: Juan Pablo Ramirez Carlon (Ingeniero de Datos, Transformacion)
"""

from __future__ import annotations

import argparse
import sys

from lakehouse.common.config import get_settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger

log = get_logger("lakehouse.transformation")

STEPS = {
    "silver": ["silver"],
    "gold": ["gold"],
    "reconcile": ["reconcile"],
    "all": ["silver", "gold", "reconcile"],
}


def run(steps: list[str]) -> None:
    from lakehouse.common.delta import TableLocator
    from lakehouse.common.spark import build_spark
    from lakehouse.transformation.gold import build_gold
    from lakehouse.transformation.reconcile import ReconciliationError, reconcile
    from lakehouse.transformation.silver import build_silver

    # svc-transformation: lee bronze, escribe silver y gold (D17).
    settings = get_settings().for_role("transformation")
    locator = TableLocator.from_settings(settings)
    spark = build_spark("transformation", settings)
    try:
        if "silver" in steps:
            build_silver(spark, locator)
        if "gold" in steps:
            build_gold(spark, locator)
        if "reconcile" in steps:
            result = reconcile(spark, locator)
            if not result.ok:
                failed = ", ".join(c.name for c in result.checks if not c.ok)
                raise ReconciliationError(f"No cuadran: {failed}")
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lakehouse.transformation", description=__doc__.split("\n")[0]
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("silver", help="Bronce a Plata (se recalcula completa)")
    sub.add_parser("gold", help="Plata a Oro: dimensiones, MERGE de fact_plays, OPTIMIZE y marts")
    sub.add_parser("reconcile", help="Conteos Bronce > Plata > Oro con causa de cada descarte")
    sub.add_parser("all", help="silver + gold + reconcile")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run(STEPS[args.command])
    except LakehouseError as exc:
        log.error("Transformacion FALLO: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
