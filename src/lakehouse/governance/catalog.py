"""Catalogo de datos en Markdown, generado solo desde los contratos (no necesita Spark ni MinIO).

Los acentos del Markdown van como escapes \\u para que el codigo siga en ASCII.
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

from lakehouse.governance.access import ACCESS, READ
from lakehouse.governance.contracts import (
    ALL_TABLES,
    CLASSIFICATIONS,
    NO_PII,
    PERSONAL,
    PSEUDONYM,
    Column,
    TableContract,
)

GENERATED_BY = "uv run python -m lakehouse.governance docs"
PRIVACY_LABELS = {NO_PII: "", PERSONAL: "personal", PSEUDONYM: "seud\u00f3nimo"}
LAYER_TITLES = {
    "landing": "landing (archivos crudos)",
    "bronze": "bronze (Bronce)",
    "silver": "silver (Plata)",
    "gold": "gold (Oro)",
}


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def column_rules(col: Column) -> str:
    """Reglas de la columna en texto corto: valores aceptados y rango."""
    parts = []
    if col.values:
        parts.append("en {" + ", ".join(col.values) + "}")
    if col.min_value is not None or col.max_value is not None:
        low = "" if col.min_value is None else f"{col.min_value:g}"
        high = "" if col.max_value is None else f"{col.max_value:g}"
        parts.append(f"[{low}, {high}]" if low and high else (f">= {low}" if low else f"<= {high}"))
    return "; ".join(parts)


def readers(contract: TableContract) -> str:
    """Usuarios de MinIO que pueden leer la tabla (por su bucket)."""
    roles = [r for r, grants in ACCESS.items() if any(g.bucket == contract.layer for g in grants)]
    return ", ".join(f"svc-{r}" for r in roles) or "solo root"


def writers(contract: TableContract) -> str:
    roles = [
        r
        for r, grants in ACCESS.items()
        if any(
            g.bucket == contract.layer and g.mode != READ and contract.name.startswith(g.prefix)
            for g in grants
        )
    ]
    return ", ".join(f"svc-{r}" for r in roles) or "nadie"


def _table_section(t: TableContract) -> list[str]:
    lines = [f"### `{t.qualified_name}`", "", t.description, ""]
    facts = [
        ("Due\u00f1o", t.owner),
        ("Clasificaci\u00f3n", f"{t.classification}: {CLASSIFICATIONS[t.classification]}"),
        ("Se alimenta de", ", ".join(f"`{s}`" for s in t.sources) or "n/a"),
        ("Acceso en MinIO", f"leen {readers(t)}; escriben {writers(t)}"),
    ]
    if t.partition_by:
        facts.append(("Partici\u00f3n", ", ".join(t.partition_by)))
    if t.unique_keys:
        facts.append(("Llaves \u00fanicas", "; ".join("(" + ", ".join(k) + ")" for k in t.unique_keys)))
    if t.references:
        facts.append(
            ("Referencias", "; ".join(f"{r.column} > `{r.ref}.{r.ref_column}`" for r in t.references))
        )
    if t.rules:
        facts.append(("Reglas", "; ".join(r.name for r in t.rules)))
    lines += [f"- **{k}:** {_cell(v)}" for k, v in facts]
    lines += [
        "",
        "| Columna | Tipo | Nulos | Privacidad | Reglas | Descripci\u00f3n |",
        "|---|---|---|---|---|---|",
    ]
    for c in t.columns:
        nulls = "s\u00ed" if c.nullable else "no"
        lines.append(
            f"| `{c.name}` | `{_cell(c.dtype)}` | {nulls} | {PRIVACY_LABELS[c.privacy]} | "
            f"{_cell(column_rules(c))} | {_cell(c.description)} |"
        )
    return lines + [""]


def render_catalog() -> str:
    lines = [
        "# Cat\u00e1logo de datos",
        "",
        f"> Generado desde `src/lakehouse/governance/contracts.py` con `{GENERATED_BY}`. No se edita a mano:",
        "> se cambia el contrato y se regenera. Las mismas reglas corren como checks de calidad",
        "> ([CALIDAD_Y_LINAJE.md](CALIDAD_Y_LINAJE.md)) y el linaje est\u00e1 en [LINAJE.md](LINAJE.md).",
        "",
        "## Clasificaci\u00f3n",
        "",
        "| Nivel | Qu\u00e9 significa |",
        "|---|---|",
        *[f"| {k} | {v} |" for k, v in CLASSIFICATIONS.items()],
        "",
        "Privacidad por columna: **personal** identifica a una persona (la IP); **seud\u00f3nimo** es",
        "un HMAC con sal que no se puede revertir sin la sal (D8). Ninguna tabla de Plata u Oro tiene",
        "columnas con privacidad.",
        "",
        "## \u00cdndice",
        "",
        "| Tabla | Clasificaci\u00f3n | Columnas | Due\u00f1o | Descripci\u00f3n |",
        "|---|---|---|---|---|",
    ]
    for t in ALL_TABLES:
        lines.append(
            # Ancla de GitHub: el titulo sin el punto (los guiones bajos se quedan).
            f"| [`{t.qualified_name}`](#{t.layer}{t.name}) | {t.classification} | "
            f"{len(t.columns)} | {t.owner} | {_cell(t.description)} |"
        )
    for layer, title in LAYER_TITLES.items():
        lines += ["", f"## {title}", ""]
        for t in ALL_TABLES:
            if t.layer == layer:
                lines += _table_section(t)
    return "\n".join(lines).rstrip() + "\n"
