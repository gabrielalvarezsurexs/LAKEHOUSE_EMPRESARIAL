"""CLI de gobernanza: python -m lakehouse.governance <comando> (ver --help).

docs (local, sin Spark ni MinIO): CATALOGO.md, LINAJE.md y politicas de MinIO desde los contratos.
check (contenedor): checks de calidad en Bronce, Plata y Oro, prueba de accesos y CALIDAD_Y_LINAJE.md;
falla si algo bloqueante no pasa. access (contenedor): solo la prueba de accesos de los usuarios de MinIO.
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from lakehouse.common.config import get_settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger

log = get_logger("lakehouse.governance")

DEFAULT_REPORT_DIR = "ENTREGABLES/03_calidad_y_linaje"
DEFAULT_POLICY_DIR = "infra/minio/policies"
REPORT_FILE = "CALIDAD_Y_LINAJE.md"


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def write_docs(out_dir: str | Path, policy_dir: str | Path) -> list[Path]:
    """Catalogo, linaje y politicas: todo sale de los contratos y de la matriz de acceso."""
    from lakehouse.governance.access import write_policies
    from lakehouse.governance.catalog import render_catalog
    from lakehouse.governance.lineage import render_lineage

    out = Path(out_dir)
    docs = [_write(out / "CATALOGO.md", render_catalog()), _write(out / "LINAJE.md", render_lineage())]
    return docs + write_policies(policy_dir)


def _docs(args: argparse.Namespace) -> int:
    paths = write_docs(args.out, args.policies)
    log.info("Docs de gobernanza: %d archivos (%s)", len(paths), ", ".join(p.name for p in paths))
    return 0


def _access(_: argparse.Namespace) -> int:
    from lakehouse.governance.access import probe_all, root_in_environment

    probes = probe_all(get_settings())
    root = root_in_environment()
    if root:
        log.error("Las llaves root de MinIO estan en el contenedor: quitalas de docker-compose.yml (D17).")
    return 0 if all(p.ok for p in probes) and not root else 1


def _check(args: argparse.Namespace) -> int:
    from lakehouse.common.delta import TableLocator
    from lakehouse.common.spark import build_spark
    from lakehouse.governance.access import probe_all, root_in_environment
    from lakehouse.governance.quality import run_quality
    from lakehouse.governance.report import render_report

    settings = get_settings()
    probes = None if args.skip_access else probe_all(settings)
    # svc-governance: solo lectura de bronze, silver y gold (D17).
    spark = build_spark("governance", settings.for_role("governance"))
    try:
        run = run_quality(spark, TableLocator.from_settings(settings))
    finally:
        spark.stop()
    root = root_in_environment()
    report = render_report(run, probes, root, datetime.now(UTC))
    path = _write(Path(args.out) / REPORT_FILE, report)
    log.info("Reporte: %s", path)
    access_ok = probes is None or all(p.ok for p in probes)
    if run.ok and access_ok and not root:
        log.info("Gobernanza OK: checks bloqueantes y accesos en verde (%d avisos).", len(run.warnings))
        return 0
    log.error(
        "Gobernanza FALLO: %d checks bloqueantes, accesos %s.", len(run.errors), "OK" if access_ok else "MAL"
    )
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lakehouse.governance", description=__doc__.split("\n")[0]
    )
    sub = parser.add_subparsers(dest="command", required=True)
    report_dir = os.environ.get("REPORT_DIR") or DEFAULT_REPORT_DIR

    docs = sub.add_parser("docs", help="CATALOGO.md, LINAJE.md y politicas de MinIO (local, sin Spark)")
    docs.add_argument("--out", default=DEFAULT_REPORT_DIR, help="Carpeta del catalogo y el linaje")
    docs.add_argument("--policies", default=DEFAULT_POLICY_DIR, help="Carpeta de las politicas JSON")

    check = sub.add_parser("check", help="Checks de calidad + accesos + CALIDAD_Y_LINAJE.md")
    check.add_argument("--out", default=report_dir, help="Carpeta del reporte (default: REPORT_DIR)")
    check.add_argument("--skip-access", action="store_true", help="No probar los usuarios de MinIO")

    sub.add_parser("access", help="Prueba en vivo de lo que cada usuario de MinIO puede y no puede hacer")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {"docs": _docs, "check": _check, "access": _access}
    try:
        return handlers[args.command](args)
    except LakehouseError as exc:
        log.error("Gobernanza FALLO: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
