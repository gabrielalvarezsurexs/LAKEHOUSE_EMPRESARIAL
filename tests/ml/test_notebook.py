"""Notebook de presentacion: sin salidas guardadas y sus celdas corren con el metrics.json del repo.

Responsable: Fernando Pena Meade (Ingeniero de ML)
"""

import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "notebooks" / "ml_skip.ipynb"
METRICS = ROOT / "ENTREGABLES" / "04_modelo_ml" / "metrics.json"

# El contenedor solo monta src/ y tests/: aqui corre en local y en el job sin Docker de CI.
pytestmark = pytest.mark.skipif(not NOTEBOOK.exists(), reason="notebooks/ no esta montado (contenedor)")


def code_cells():
    nb = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return [c for c in nb["cells"] if c["cell_type"] == "code"]


def test_notebook_has_no_saved_outputs():
    # Las salidas podrian traer datos; el repo es publico.
    for cell in code_cells():
        assert cell["outputs"] == [] and cell["execution_count"] is None


@pytest.mark.skipif(not METRICS.exists(), reason="Sin metrics.json: falta correr ml_train")
def test_notebook_cells_run_on_the_report(monkeypatch):
    shown = []
    display = types.ModuleType("IPython.display")
    display.display = shown.append
    display.Markdown = display.Image = lambda *a, **k: (a, k)
    monkeypatch.setitem(sys.modules, "IPython", types.ModuleType("IPython"))
    monkeypatch.setitem(sys.modules, "IPython.display", display)
    monkeypatch.chdir(ROOT / "notebooks")
    scope = {}
    for cell in code_cells()[:-1]:  # la ultima consulta MLflow en vivo
        exec("".join(cell["source"]), scope)
    assert scope["REPORT"] == METRICS.parent
    assert len(shown) == 4  # veredicto y las 3 graficas
