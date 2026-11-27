"""CLI de orquestacion. Dentro del contenedor: python -m lakehouse.orchestration <comando> (ver --help).

health: salud del lakehouse (tamanos, versiones, filas y alertas); run: corre un job de Dagster completo en
este proceso (lo usa CI) y falla si un paso o un check bloqueante falla; status: ultimas corridas y checks;
wait: espera a que el pipeline automatico termine (replica y demo); recover: al arrancar el contenedor,
marca las corridas que murieron con el para que Dagster las reintente.
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


ACTIVE_STATUSES = ("QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING")


def active_runs(instance) -> list[str]:
    """Corridas en curso o en cola (las del daemon y las de este CLI comparten la instancia)."""
    import dagster as dg

    statuses = [dg.DagsterRunStatus[s] for s in ACTIVE_STATUSES]
    runs = instance.get_runs(filters=dg.RunsFilter(statuses=statuses))
    return [f"{r.job_name} {r.run_id[:8]} {r.status.value}" for r in runs]


def _run(args: argparse.Namespace) -> int:
    import dagster as dg

    from lakehouse.orchestration.definitions import defs

    # Con DAGSTER_HOME la corrida queda en el historial de la UI; sin el (CI), instancia temporal.
    instance = dg.DagsterInstance.get() if os.environ.get("DAGSTER_HOME") else dg.DagsterInstance.ephemeral()
    # execute_in_process no pasa por la cola del daemon (max_concurrent_runs: 1): dos pipelines con
    # su propio Spark a la vez agotan la memoria del contenedor (H-011).
    busy = active_runs(instance)
    if busy and not args.force:
        log.error(
            "Hay corridas de Dagster en curso o en cola: %s. Espera a que terminen (status) o usa --force.",
            "; ".join(busy),
        )
        return 1
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


ORPHAN_STATUSES = ("STARTING", "STARTED", "CANCELING")
ORPHAN_MESSAGE = "El contenedor se reinicio con esta corrida a medias (H-012); Dagster la reintenta sola."


def recover_orphan_runs(instance) -> list[str]:
    """Al arrancar el contenedor ningun proceso de corrida sigue vivo: las 'en curso' quedaron huerfanas.

    El lanzador por defecto no detecta un worker muerto (la corrida se queda en STARTED hasta
    max_runtime_seconds). Se marcan como falla del worker, no de un paso, para que run_retries las relance.
    """
    import dagster as dg
    from dagster._core.events import JobFailureData, RunFailureReason

    statuses = [dg.DagsterRunStatus[s] for s in ORPHAN_STATUSES]
    recovered = []
    for run in instance.get_runs(filters=dg.RunsFilter(statuses=statuses)):
        if run.status == dg.DagsterRunStatus.CANCELING:
            instance.report_run_canceled(run, message=ORPHAN_MESSAGE)  # ya la habian mandado cancelar
        else:
            failure = JobFailureData(error=None, failure_reason=RunFailureReason.RUN_WORKER_RESTART)
            instance.report_run_failed(run, message=ORPHAN_MESSAGE, job_failure_data=failure)
        recovered.append(f"{run.job_name} {run.run_id[:8]} {run.status.value}")
    return recovered


def _recover(args: argparse.Namespace) -> int:
    """Lo corre el contenedor antes de levantar Dagster (CMD de infra/app/Dockerfile)."""
    import dagster as dg

    if not os.environ.get("DAGSTER_HOME"):
        log.error("Sin DAGSTER_HOME no hay historial: corre esto dentro del contenedor lakehouse.")
        return 1
    with dg.DagsterInstance.get() as instance:
        recovered = recover_orphan_runs(instance)
    if recovered:
        log.warning(
            "Corridas huerfanas del arranque anterior: %s. Se reintentan solas.", "; ".join(recovered)
        )
    else:
        log.info("Sin corridas huerfanas.")
    return 0


PIPELINE_JOBS = ("pipeline_from_landing", "lakehouse_full")
WILL_RETRY_TAG = "dagster/will_retry"  # lo pone Dagster cuando run_retries va a relanzar la corrida


def last_pipeline_status(instance) -> str | None:
    """Estado de la corrida mas reciente que llega hasta Oro y ML (la dispara el sensor o el schedule)."""
    for run in instance.get_runs(limit=50):
        if run.job_name in PIPELINE_JOBS:
            if run.status.value == "FAILURE" and run.tags.get(WILL_RETRY_TAG) == "true":
                return "RETRYING"  # el reintento aun no aparece en la cola: no es el resultado final
            return run.status.value
    return None


def _wait(args: argparse.Namespace) -> int:
    """Espera a que Dagster quede sin corridas (los sensores ya dispararon todo) y reporta la del pipeline."""
    import time

    import dagster as dg

    if not os.environ.get("DAGSTER_HOME"):
        log.error("Sin DAGSTER_HOME no hay historial: corre esto dentro del contenedor lakehouse.")
        return 1
    instance = dg.DagsterInstance.get()
    deadline = time.monotonic() + args.timeout
    idle_since: float | None = None
    try:
        while True:
            busy = active_runs(instance)
            status = last_pipeline_status(instance)
            now = time.monotonic()
            idle_since = None if busy else (idle_since or now)
            # Tras ingest_landing, el sensor de landing tarda hasta 30 s en lanzar el pipeline.
            settled = idle_since is not None and now - idle_since >= args.settle
            if settled and status not in (None, "RETRYING"):
                if status == "SUCCESS":
                    log.info("Pipeline listo: la ultima corrida termino en SUCCESS.")
                    return 0
                log.error("La ultima corrida del pipeline termino en %s: revisa status o la UI.", status)
                return 1
            # El daemon relanza en segundos; si no aparece (p. ej. su primer arranque ignora fallas previas),
            # no tiene caso esperar hasta el timeout.
            if status == "RETRYING" and idle_since is not None and now - idle_since >= 3 * args.settle:
                log.error(
                    "Dagster no lanzo el reintento del pipeline: correlo con `run --job lakehouse_full`."
                )
                return 1
            if now > deadline:
                log.error("Tiempo agotado (%s s). En curso: %s", args.timeout, "; ".join(busy) or "nada")
                return 1
            log.info("Esperando: %s", "; ".join(busy) or f"sin corridas (ultimo pipeline: {status})")
            time.sleep(args.poll)
    finally:
        instance.dispose()


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
    run.add_argument(
        "--force", action="store_true", help="Corre aunque haya otra corrida en curso (riesgo de memoria)"
    )
    status = sub.add_parser("status", help="Ultimas corridas y resultado de cada check (contenedor)")
    status.add_argument("--limit", type=int, default=8)
    wait = sub.add_parser("wait", help="Espera a que termine el pipeline automatico (contenedor)")
    wait.add_argument("--timeout", type=int, default=3600, help="Segundos maximos de espera")
    wait.add_argument("--settle", type=int, default=90, help="Segundos sin corridas para darlo por terminado")
    wait.add_argument("--poll", type=int, default=15, help="Cada cuantos segundos revisa")
    sub.add_parser(
        "recover", help="Marca como fallidas las corridas que murieron con el contenedor (arranque)"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {"health": _health, "run": _run, "status": _status, "wait": _wait, "recover": _recover}
    try:
        return handlers[args.command](args)
    except LakehouseError as exc:
        log.error("Orquestacion FALLO: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
