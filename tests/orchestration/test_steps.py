"""Pasos del pipeline: cubren todas las tablas, el grafo es el de LINAJE.md y el runner reporta bien.

Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

import sys

import pytest

from lakehouse.governance import lineage
from lakehouse.governance.contracts import ALL_TABLES
from lakehouse.ingestion import landing
from lakehouse.orchestration import automation, steps


def test_every_contract_table_is_produced_by_exactly_one_step():
    produced = steps.produced_nodes()
    assert len(produced) == len(set(produced))
    assert {t.qualified_name for t in ALL_TABLES} <= set(produced)


def test_graph_is_the_lineage_graph_without_consumers():
    """Mismo grafo que LINAJE.md (D23); los consumidores llegan con ML y BI."""
    ours = {(src, node) for node in steps.produced_nodes() for src in steps.sources_of(node)}
    theirs = {(a, b) for a, b in lineage.edges() if not b.startswith("consumer.")}
    assert ours == theirs


def test_external_sources_are_the_lineage_ext_nodes():
    ext = {n.id for n in lineage.nodes() if n.group == "ext"}
    assert set(steps.EXTERNAL_SOURCES) == ext
    produced = set(steps.produced_nodes())
    sources = {s for node in produced for s in steps.sources_of(node)}
    assert sources - produced == ext


def test_steps_are_in_dependency_order():
    seen = set(steps.EXTERNAL_SOURCES)
    for step in steps.STEPS:
        for node in step.outputs:
            pending = [s for s in steps.sources_of(node) if s not in seen and s not in step.outputs]
            assert not pending, f"{node} depende de {pending}, que sale de un paso posterior"
        seen |= set(step.outputs)


def test_landing_constants_match_ingestion():
    assert automation.LANDING_BUCKET == landing.LANDING_BUCKET
    assert automation.SPOTIFY_PREFIX == landing.SPOTIFY_PREFIX


def test_commands_use_the_current_interpreter():
    cmd = steps.step_of("silver.plays").command()
    assert cmd == [sys.executable, "-m", "lakehouse.transformation", "silver"]
    assert steps.step_of("bronze.musicbrainz_artists").retries > 0


def test_run_command_streams_lines_and_returns_code():
    lines = []
    code = steps.run_command([sys.executable, "-c", "print('uno'); print(''); print('dos')"], lines.append)
    assert code == 0
    assert lines == ["uno", "dos"]


def test_run_command_failure_keeps_tail():
    script = "import sys\nfor i in range(50): print(i)\nsys.exit(3)"
    with pytest.raises(steps.StepError) as err:
        steps.run_command([sys.executable, "-c", script], lambda _: None)
    assert err.value.code == 3
    assert err.value.tail[-1] == "49"
    assert len(err.value.tail) == steps.TAIL_LINES


def test_run_command_accepts_expected_codes():
    assert steps.run_command([sys.executable, "-c", "raise SystemExit(1)"], print, ok_codes=(0, 1)) == 1
