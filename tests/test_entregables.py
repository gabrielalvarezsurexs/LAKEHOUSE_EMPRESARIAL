"""Entregables: el diagrama del README de ENTREGABLES y su fuente para la imagen no se separan.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import re
from pathlib import Path

import pytest

DIAGRAM_DIR = Path(__file__).resolve().parents[1] / "ENTREGABLES" / "01_diagrama_arquitectura"


@pytest.mark.skipif(not DIAGRAM_DIR.exists(), reason="ENTREGABLES/ no esta montado (contenedor)")
def test_diagram_png_comes_from_the_same_mermaid():
    """Si cambias el Mermaid de DIAGRAMA.md, copia el bloque a arquitectura.mmd y regenera la imagen:
    npx -y @mermaid-js/mermaid-cli@12.2.0 -i arquitectura.mmd -o arquitectura.png -b white -s 2"""
    md = (DIAGRAM_DIR / "DIAGRAMA.md").read_text(encoding="utf-8")
    block = re.search(r"```mermaid\n(.*?)```", md, re.S).group(1)
    assert block == (DIAGRAM_DIR / "arquitectura.mmd").read_text(encoding="utf-8")
    assert (DIAGRAM_DIR / "arquitectura.png").stat().st_size > 10_000
