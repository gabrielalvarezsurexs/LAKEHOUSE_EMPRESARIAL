"""Definiciones de Dagster del lakehouse; el contenedor las sirve con dagster dev (http://localhost:3300).

Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

from __future__ import annotations

import dagster as dg

from lakehouse.orchestration.assets import external_specs, lakehouse_health, quality_report, step_asset
from lakehouse.orchestration.automation import JOBS, SCHEDULES, SENSORS
from lakehouse.orchestration.checks import STEP_CHECKS
from lakehouse.orchestration.steps import STEPS

defs = dg.Definitions(
    assets=[*external_specs(), *(step_asset(s) for s in STEPS), quality_report, lakehouse_health],
    asset_checks=STEP_CHECKS,
    jobs=JOBS,
    schedules=SCHEDULES,
    sensors=SENSORS,
    # Un paso a la vez dentro de cada corrida: cada uno levanta su propio Spark (4 GB) y escriben Delta.
    executor=dg.in_process_executor,
)
