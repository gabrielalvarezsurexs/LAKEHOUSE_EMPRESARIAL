"""Asset checks por paso: envuelven los checks de gobernanza (F4) y bloquean lo que sigue si fallan.

Tras cada paso que escribe Delta corre `python -m lakehouse.governance check --tables ... --json` en otro
proceso (svc-governance) y aqui solo se reparte el resultado por tabla. Es por paso y no por capa porque
generos escribe en Bronce despues de que Bronce ya paso sus checks. El ultimo (Oro) agrega reconciliacion
(bloqueante) y frescura (aviso).
Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

import tempfile
from collections.abc import Iterator
from pathlib import Path

import dagster as dg

from lakehouse.governance.contracts import GOLD_FACT_PLAYS, GOLD_KPI_LISTENING_DAILY
from lakehouse.orchestration import steps
from lakehouse.orchestration.assets import asset_key, run_json_command
from lakehouse.orchestration.steps import STEPS, Step

CONTRACT_CHECK = "contrato"
RECONCILIATION_CHECK = "reconciliacion"
FRESHNESS_CHECK = "frescura"
# Checks entre capas: a que asset de Oro se cuelgan, que tipo de resultado juntan y si bloquean.
CROSS_CHECKS = {
    RECONCILIATION_CHECK: (GOLD_FACT_PLAYS.qualified_name, "reconciliacion", True),
    FRESHNESS_CHECK: (GOLD_KPI_LISTENING_DAILY.qualified_name, "frescura", False),
}
CROSS_LAYER_STEP = "gold"
MAX_LISTED = 20


def checked_tables(step: Step) -> tuple[str, ...]:
    """Tablas Delta con contrato que produce el paso (landing y la cache de MusicBrainz no lo son)."""
    return tuple(n for n in step.outputs if not n.startswith("landing.") and steps.contract_of(n))


def summarize(results: list[dict], rows: int | None = None) -> tuple[bool, dg.AssetCheckSeverity, dict]:
    """passed solo si nada fallo; severidad ERROR si fallo algo bloqueante o no hubo resultados."""
    errors = [r for r in results if not r["ok"] and r["severity"] == "error"]
    warnings = [r for r in results if not r["ok"] and r["severity"] == "warn"]
    failed = [f"{r['kind']}: {r['check']} ({r['failed']})" for r in errors + warnings]
    metadata: dict = {"checks": len(results), "errors": len(errors), "warnings": len(warnings)}
    if rows is not None:
        metadata["rows"] = rows
    if failed:
        metadata["failed"] = dg.MetadataValue.md("\n".join(f"- {f}" for f in failed[:MAX_LISTED]))
    passed = bool(results) and not failed
    severity = dg.AssetCheckSeverity.ERROR if errors or not results else dg.AssetCheckSeverity.WARN
    return passed, severity, metadata


def check_results(step: Step, data: dict) -> Iterator[dg.AssetCheckResult]:
    """Un resultado por tabla del paso (y los de entre capas en Oro) a partir del JSON de gobernanza."""
    results, counts = data["results"], data["row_counts"]
    for table in checked_tables(step):
        mine = [r for r in results if r["table"] == table]
        passed, severity, metadata = summarize(mine, counts.get(table))
        yield dg.AssetCheckResult(
            asset_key=asset_key(table),
            check_name=CONTRACT_CHECK,
            passed=passed,
            severity=severity,
            metadata=metadata,
        )
    if step.name != CROSS_LAYER_STEP:
        return
    for name, (node, kind, blocking) in CROSS_CHECKS.items():
        passed, severity, metadata = summarize([r for r in results if r["kind"] == kind])
        if not blocking:
            severity = dg.AssetCheckSeverity.WARN
        yield dg.AssetCheckResult(
            asset_key=asset_key(node), check_name=name, passed=passed, severity=severity, metadata=metadata
        )


def check_specs(step: Step) -> list[dg.AssetCheckSpec]:
    specs = [
        dg.AssetCheckSpec(
            CONTRACT_CHECK,
            asset=asset_key(table),
            blocking=True,
            description="Contrato (F4): esquema, privacidad, nulos, valores, reglas, unicos y referencias",
        )
        for table in checked_tables(step)
    ]
    if step.name == CROSS_LAYER_STEP:
        specs += [
            dg.AssetCheckSpec(
                name,
                asset=asset_key(node),
                blocking=blocking,
                description=f"Checks de {kind} entre capas (governance/quality.py)",
            )
            for name, (node, kind, blocking) in CROSS_CHECKS.items()
        ]
    return specs


def quality_command(step: Step, out: Path) -> list[str]:
    args = ["check", "--tables", ",".join(checked_tables(step)), "--json", str(out)]
    if step.name == CROSS_LAYER_STEP:
        args.append("--cross-layer")
    return steps.python_command(steps.GOVERNANCE_MODULE, *args)


def step_check(step: Step) -> dg.AssetChecksDefinition:
    @dg.multi_asset_check(
        name=f"{step.name}_quality",
        specs=check_specs(step),
        description=f"Checks de gobernanza de lo que escribe el paso {step.name} (svc-governance)",
    )
    def _check(context: dg.AssetCheckExecutionContext):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "quality.json"
            data = run_json_command(context, quality_command(step, out), out)
        yield from check_results(step, data)

    return _check


STEP_CHECKS = [step_check(s) for s in STEPS if checked_tables(s)]
