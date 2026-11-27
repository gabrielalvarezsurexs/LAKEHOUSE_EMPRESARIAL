"""Definiciones de Dagster y corridas completas con comandos falsos (sin Spark ni MinIO).

Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

import json

import dagster as dg
import pytest

from lakehouse.governance.contracts import DELTA_TABLES, GOLD_FACT_PLAYS
from lakehouse.orchestration import __main__ as cli
from lakehouse.orchestration import assets, automation, checks, steps
from lakehouse.orchestration.definitions import defs


def key(node):
    return dg.AssetKey(node.split(".", 1))


@pytest.fixture(scope="module")
def graph():
    return defs.resolve_asset_graph()


def test_assets_cover_steps_sources_report_and_health(graph):
    expected = {key(n) for n in steps.produced_nodes()} | {key(n) for n in steps.EXTERNAL_SOURCES}
    expected |= {assets.REPORT_KEY, assets.HEALTH_KEY}
    assert set(graph.get_all_asset_keys()) == expected


def test_specs_come_from_the_contract(graph):
    node = graph.get(key(GOLD_FACT_PLAYS.qualified_name))
    schema = node.metadata["dagster/column_schema"]
    assert [c.name for c in schema.columns] == list(GOLD_FACT_PLAYS.column_names)
    assert getattr(node.metadata["owner"], "value", node.metadata["owner"]) == GOLD_FACT_PLAYS.owner
    assert node.group_name == "gold"
    assert {k.to_user_string() for k in node.parent_keys} == {"silver/plays"}


def test_every_delta_table_has_a_blocking_contract_check():
    specs = {(s.asset_key, s.name): s for c in checks.STEP_CHECKS for s in c.check_specs}
    for contract in DELTA_TABLES:
        spec = specs[(key(contract.qualified_name), checks.CONTRACT_CHECK)]
        assert spec.blocking
    assert specs[(key(GOLD_FACT_PLAYS.qualified_name), checks.RECONCILIATION_CHECK)].blocking


def test_jobs_schedules_and_sensors_start_on_their_own():
    for name in ("lakehouse_full", "ingest_landing", "pipeline_from_landing", "health_only"):
        defs.resolve_job_def(name)
    assert all(s.default_status == dg.DefaultScheduleStatus.RUNNING for s in automation.SCHEDULES)
    assert all(s.default_status == dg.DefaultSensorStatus.RUNNING for s in automation.SENSORS)


# --- Resumen de checks ---


def _result(table, ok=True, severity="error", kind="no_nulos"):
    return {
        "table": table,
        "kind": kind,
        "check": "x",
        "severity": severity,
        "ok": ok,
        "failed": 0 if ok else 2,
    }


def test_summarize_levels():
    passed, sev, meta = checks.summarize([_result("t"), _result("t")], rows=10)
    assert passed and meta["rows"] == 10
    passed, sev, _ = checks.summarize([_result("t"), _result("t", ok=False, severity="warn")])
    assert not passed and sev == dg.AssetCheckSeverity.WARN
    passed, sev, meta = checks.summarize([_result("t", ok=False, severity="warn"), _result("t", ok=False)])
    assert not passed and sev == dg.AssetCheckSeverity.ERROR and meta["errors"] == 1
    passed, sev, _ = checks.summarize([])
    assert not passed and sev == dg.AssetCheckSeverity.ERROR  # sin resultados no es verde


def test_gold_step_adds_cross_layer_checks():
    gold = next(s for s in steps.STEPS if s.name == checks.CROSS_LAYER_STEP)
    data = {
        "row_counts": {},
        "results": [
            _result("entre capas", kind="reconciliacion"),
            _result("a > b", ok=False, kind="frescura"),
        ],
    }
    by_name = {r.check_name: r for r in checks.check_results(gold, data) if r.check_name != "contrato"}
    assert by_name["reconciliacion"].passed
    assert not by_name["frescura"].passed
    assert by_name["frescura"].severity == dg.AssetCheckSeverity.WARN
    assert "--cross-layer" in checks.quality_command(gold, "x.json")


# --- Sensores ---


def test_evaluate_runs_first_time_and_on_change_only():
    decision, cursor = automation.evaluate(None, "abc", "x", "p")
    assert isinstance(decision, dg.RunRequest) and cursor == "abc"
    decision, cursor = automation.evaluate("abc", "abc", "x", "p")
    assert isinstance(decision, dg.SkipReason) and cursor == "abc"
    decision, _ = automation.evaluate("abc", None, "x", "p")
    assert isinstance(decision, dg.SkipReason)


def test_source_fingerprint_changes_with_files(tmp_path):
    assert automation.fingerprint(automation.source_items(tmp_path / "no-existe")) is None
    (tmp_path / "a.json").write_text("[]")
    (tmp_path / "notas.txt").write_text("no cuenta")
    first = automation.fingerprint(automation.source_items(tmp_path))
    (tmp_path / "b.json").write_text("[]")
    assert automation.fingerprint(automation.source_items(tmp_path)) not in (None, first)


def test_landing_items_only_json(store):
    store.put_bytes("landing", "spotify/2026-11-23/a.json", b"[]")
    store.put_bytes("landing", "spotify/2026-11-23/leeme.txt", b"")
    store.put_bytes("landing", "musicbrainz/api/x.json", b"{}")
    assert automation.landing_items(store) == ["spotify/2026-11-23/a.json"]


def test_source_sensor_requests_a_run_then_skips(tmp_path, monkeypatch, settings):
    from dataclasses import replace

    (tmp_path / "a.json").write_text("[]")
    monkeypatch.setattr(
        automation, "get_settings", lambda: replace(settings, spotify_source_dir=str(tmp_path))
    )
    ctx = dg.build_sensor_context()
    first = automation.source_folder_sensor(ctx)
    assert isinstance(first, dg.RunRequest)
    ctx = dg.build_sensor_context(cursor=ctx.cursor)
    assert isinstance(automation.source_folder_sensor(ctx), dg.SkipReason)


# --- Corridas completas con comandos falsos ---


def _health_data(alerts=()):
    return {
        "generated_at": "2026-11-23T00:00:00+00:00",
        "tables": [
            {
                "table": c.qualified_name,
                "exists": True,
                "rows": 5,
                "files": 1,
                "bytes": 2048,
                "version": 3,
                "last_operation": "WRITE",
                "last_commit": "2026-11-23T00:00:00+00:00",
            }
            for c in DELTA_TABLES
        ],
        "buckets": [{"bucket": "gold", "objects": 4, "bytes": 4096, "error": ""}],
        "alerts": list(alerts),
    }


class FakeRunner:
    """Reemplaza steps.run_command: anota cada comando y deja el JSON que esperan los checks."""

    def __init__(self, failing_table=None):
        self.calls = []
        self.failing_table = failing_table

    def __call__(self, cmd, on_line, ok_codes=(0,)):
        args = cmd[2:]
        self.calls.append(" ".join(a for a in args if not a.endswith(".json")))
        if "--json" in args:
            out = args[args.index("--json") + 1]
            if args[0] == steps.GOVERNANCE_MODULE:
                tables = args[args.index("--tables") + 1].split(",")
                results = [_result(t, ok=t != self.failing_table) for t in tables]
                if "--cross-layer" in args:
                    results.append(_result("entre capas", kind="reconciliacion"))
                data = {"row_counts": {t: 5 for t in tables}, "results": results}
            else:
                data = _health_data()
            with open(out, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
        on_line(f"falso: {args[0]}")
        return 0


def _execute(monkeypatch, runner, job="lakehouse_full"):
    monkeypatch.setattr(steps, "run_command", runner)
    return defs.resolve_job_def(job).execute_in_process(
        instance=dg.DagsterInstance.ephemeral(), raise_on_error=False
    )


def test_full_job_runs_every_step_in_order(monkeypatch):
    runner = FakeRunner()
    result = _execute(monkeypatch, runner)
    assert result.success
    assert cli.failed_checks(result) == []
    modules = [c.split()[0:2] for c in runner.calls]
    order = [" ".join(m) for m in modules]
    assert order.index("lakehouse.ingestion landing") < order.index("lakehouse.ingestion bronze")
    assert order.index("lakehouse.ingestion bronze") < order.index("lakehouse.ingestion genres")
    assert order.index("lakehouse.ingestion genres") < order.index("lakehouse.transformation silver")
    assert order.index("lakehouse.transformation silver") < order.index("lakehouse.transformation gold")
    assert order.index("lakehouse.transformation gold") < order.index("lakehouse.ml features")
    assert order.index("lakehouse.ml features") < order.index("lakehouse.ml train")
    assert order.index("lakehouse.ml train") < order.index("lakehouse.ml predict")
    # El reporte completo (check sin --tables) va despues de las predicciones.
    assert order.index("lakehouse.ml predict") < runner.calls.index("lakehouse.governance check")
    assert order[-1] == "lakehouse.orchestration health"
    # bronze, genres, silver, gold, ml_features y ml_predict (ml_train no escribe tablas)
    assert len([c for c in runner.calls if "--tables" in c]) == 6


def test_failed_blocking_check_stops_the_next_layer(monkeypatch):
    runner = FakeRunner(failing_table="bronze.spotify_streams")
    result = _execute(monkeypatch, runner)
    assert "bronze/spotify_streams:contrato" in cli.failed_checks(result)
    ran = " | ".join(runner.calls)
    assert "lakehouse.transformation silver" not in ran
    assert "lakehouse.transformation gold" not in ran


def test_failed_step_fails_the_run(monkeypatch):
    def broken(cmd, on_line, ok_codes=(0,)):
        raise steps.StepError("'lakehouse.ingestion landing' termino con codigo 1", 1, ["Ingesta FALLO"])

    result = _execute(monkeypatch, broken, job="ingest_landing")
    assert not result.success


def test_cli_run_exit_codes(monkeypatch):
    monkeypatch.delenv("DAGSTER_HOME", raising=False)
    monkeypatch.setattr(steps, "run_command", FakeRunner())
    assert cli.main(["run", "--job", "ingest_landing"]) == 0
    monkeypatch.setattr(steps, "run_command", FakeRunner(failing_table="gold.fact_plays"))
    assert cli.main(["run"]) == 1


def test_cli_run_refuses_while_another_run_is_active(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    runner = FakeRunner()
    monkeypatch.setattr(steps, "run_command", runner)
    with dg.DagsterInstance.get() as instance:
        instance.add_run(dg.DagsterRun(job_name="lakehouse_full", status=dg.DagsterRunStatus.STARTED))
        assert cli.active_runs(instance)
    assert cli.main(["run", "--job", "ingest_landing"]) == 1
    assert runner.calls == []  # no corrio ningun paso
    assert cli.main(["run", "--job", "ingest_landing", "--force"]) == 0


@pytest.mark.parametrize(
    ("runs", "expected"),
    [
        ([("ingest_landing", "SUCCESS"), ("pipeline_from_landing", "SUCCESS")], 0),
        ([("pipeline_from_landing", "FAILURE")], 1),
        ([("ingest_landing", "SUCCESS")], 1),  # el pipeline nunca llego: se agota el tiempo
        ([("lakehouse_full", "STARTED")], 1),  # sigue en curso: se agota el tiempo
    ],
)
def test_cli_wait_reports_the_last_pipeline_run(monkeypatch, tmp_path, runs, expected):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    with dg.DagsterInstance.get() as instance:
        for job, status in runs:
            instance.add_run(dg.DagsterRun(job_name=job, status=dg.DagsterRunStatus[status]))
    assert cli.main(["wait", "--timeout", "0", "--settle", "0", "--poll", "0"]) == expected


RETRIES_YAML = "run_retries:\n  enabled: true\n  max_retries: 1\n  retry_on_asset_or_op_failure: false\n"


def test_recover_marks_orphans_for_retry_and_wait_keeps_waiting(monkeypatch, tmp_path):
    """Un reinicio deja la corrida en STARTED: recover la marca como falla del worker y se reintenta."""
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    (tmp_path / "dagster.yaml").write_text(RETRIES_YAML, encoding="utf-8")
    # NOT_STARTED en lugar de QUEUED: una QUEUED de prueba pide el origen remoto del job.
    statuses = {"lakehouse_full": "STARTED", "health_only": "NOT_STARTED", "ingest_landing": "CANCELING"}
    with dg.DagsterInstance.get() as instance:
        ids = {}
        for job, status in statuses.items():
            run = dg.DagsterRun(job_name=job, status=dg.DagsterRunStatus[status])
            ids[job] = instance.add_run(run).run_id
    assert cli.main(["recover"]) == 0
    with dg.DagsterInstance.get() as instance:
        runs = {job: instance.get_run_by_id(run_id) for job, run_id in ids.items()}
        assert runs["lakehouse_full"].status == dg.DagsterRunStatus.FAILURE
        assert runs["lakehouse_full"].tags["dagster/failure_reason"] == "RUN_WORKER_RESTART"
        assert runs["lakehouse_full"].tags[cli.WILL_RETRY_TAG] == "true"
        assert runs["health_only"].status == dg.DagsterRunStatus.NOT_STARTED  # no es huerfana
        assert runs["ingest_landing"].status == dg.DagsterRunStatus.CANCELED
        instance.report_run_canceled(runs["health_only"])  # sin cola, wait solo ve el reintento pendiente
        assert cli.last_pipeline_status(instance) == "RETRYING"
        assert cli.recover_orphan_runs(instance) == []  # idempotente
    assert cli.main(["wait", "--timeout", "0", "--settle", "0", "--poll", "0"]) == 1  # no da por bueno


def test_cli_status_reads_runs_and_checks(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path))
    monkeypatch.setattr(steps, "run_command", FakeRunner(failing_table="silver.plays"))
    with dg.DagsterInstance.get() as instance:
        defs.resolve_job_def("lakehouse_full").execute_in_process(instance=instance, raise_on_error=False)
    assert cli.main(["status"]) == 0
    out = capsys.readouterr().out
    assert "lakehouse_full" in out
    assert "silver/plays:contrato" in out  # fallo y se lista por nombre


def test_health_metadata_and_check():
    data = _health_data(alerts=[{"level": "warn", "target": "gold.fact_plays", "message": "archivos chicos"}])
    meta = assets.health_metadata(data)
    assert meta["tables_present"] == len(DELTA_TABLES)
    assert meta["mib_gold"] == 0.0
    check = assets.health_check_result(data)
    assert not check.passed and check.severity == dg.AssetCheckSeverity.WARN
