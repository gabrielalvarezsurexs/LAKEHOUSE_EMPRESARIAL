"""CLI de orquestacion. Dentro del contenedor: python -m lakehouse.orchestration <comando> (ver --help).

health: salud del lakehouse (tamanos, versiones, filas y alertas); run: corre un job de Dagster completo en
este proceso (lo usa CI) y falla si un paso o un check bloqueante falla; status: ultimas corridas y checks.
Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from lakehouse.common.config import get_settings
from lakehouse.common.errors import LakehouseError
from lakehouse.common.log import get_logger

log = get_logger("lakehouse.orchestration")


def _health(args: argparse.Namespace) -> int:
    from lakehouse.orchestration.health import run_health

    report = run_health(get_settings())
    if args.json:
        path = Path(args.json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report.to_dict(), ensure_ascii=True, indent=1), encoding="utf-8")
    return 1 if report.errors else 0


def failed_checks(result) -> list[str]:
    """Checks que fallaron con severidad ERROR (los WARN no tumban la corrida)."""
    import dagster as dg

    return [
        f"{e.asset_key.to_user_string()}:{e.check_name}"
        for e in result.get_asset_check_evaluations()
        if not e.passed and e.severity == dg.AssetCheckSeverity.ERROR
    ]


def _run(args: argparse.Namespace) -> int:
    import dagster as dg

    from lakehouse.orchestration.definitions import defs

    # Con DAGSTER_HOME la corrida queda en el historial de la UI; sin el (CI), instancia temporal.
    instance = dg.DagsterInstance.get() if os.environ.get("DAGSTER_HOME") else dg.DagsterInstance.ephemeral()
    job = defs.resolve_job_def(args.job)
    result = job.execute_in_process(instance=instance, raise_on_error=False)
    failed = failed_checks(result)
    if result.success and not failed:
        log.info("Job %s OK (corrida %s).", args.job, result.run_id[:8])
        return 0
    steps = [e.step_key for e in result.all_events if e.is_step_failure]
    log.error(
        "Job %s FALLO. Pasos: %s. Checks: %s.",
        args.job,
        ", ".join(steps) or "ninguno",
        ", ".join(failed) or "ninguno",
    )
    return 1


def _status(args: argparse.Namespace) -> int:
    """Ultimas corridas y ultimo resultado de cada check, leidos de la instancia de Dagster (DAGSTER_HOME)."""
    from datetime import UTC, datetime

    import dagster as dg

    from lakehouse.orchestration.definitions import defs

    if not os.environ.get("DAGSTER_HOME"):
        log.error("Sin DAGSTER_HOME no hay historial: corre esto dentro del contenedor lakehouse.")
        return 1
    instance = dg.DagsterInstance.get()
    for record in instance.get_run_records(limit=args.limit):
        run = record.dagster_run
        start = record.start_time or record.create_timestamp.timestamp()
        minutes = f"{(record.end_time - start) / 60:.1f} min" if record.end_time else "en curso"
        when = datetime.fromtimestamp(start, UTC).strftime("%Y-%m-%d %H:%M UTC")
        print(f"{when}  {run.job_name:<22} {run.status.value:<10} {minutes:<9} {run.run_id[:8]}")
    keys = list(defs.resolve_asset_graph().asset_check_keys)
    latest = instance.event_log_storage.get_latest_asset_check_execution_by_key(keys)
    counts: dict[str, list[str]] = {}
    for check_key in keys:
        record = latest.get(check_key)
        status = record.status.value if record else "NUNCA"
        counts.setdefault(status, []).append(f"{check_key.asset_key.to_user_string()}:{check_key.name}")
    print("Checks: " + ", ".join(f"{s}={len(v)}" for s, v in sorted(counts.items())))
    for status, names in sorted(counts.items()):
        if status != "SUCCEEDED":
            print(f"  {status}: {', '.join(sorted(names))}")
    instance.dispose()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lakehouse.orchestration", description=__doc__.split("\n")[0]
    )
    sub = parser.add_subparsers(dest="command", required=True)
    health = sub.add_parser("health", help="Salud del lakehouse con alertas en los logs")
    health.add_argument("--json", help="Ademas escribe el resultado en este JSON (lo lee Dagster)")
    run = sub.add_parser("run", help="Corre un job de Dagster en este proceso")
    run.add_argument(
        "--job",
        default="lakehouse_full",
        choices=("lakehouse_full", "ingest_landing", "pipeline_from_landing", "health_only"),
    )
    status = sub.add_parser("status", help="Ultimas corridas y resultado de cada check (contenedor)")
    status.add_argument("--limit", type=int, default=8)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {"health": _health, "run": _run, "status": _status}
    try:
        return handlers[args.command](args)
    except LakehouseError as exc:
        log.error("Orquestacion FALLO: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
