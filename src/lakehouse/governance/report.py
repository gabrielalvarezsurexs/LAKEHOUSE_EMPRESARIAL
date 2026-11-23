"""Reporte CALIDAD_Y_LINAJE.md con los resultados de la ultima corrida: solo conteos (el repo es publico).

Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

from datetime import datetime

from lakehouse.common.config import LAYERS, SERVICE_ROLES, service_user
from lakehouse.governance.access import ACCESS, ROLE_PURPOSE, WRITE, Probe
from lakehouse.governance.contracts import DELTA_TABLES
from lakehouse.governance.lineage import mermaid
from lakehouse.governance.quality import ERROR, QualityRun

LAYER_NAMES = {"bronze": "Bronce", "silver": "Plata", "gold": "Oro"}
YES, NO = "\u2705", "\u274c"
WARN_MARK = "\u26a0\ufe0f"


def _mark(ok: bool, severity: str = ERROR) -> str:
    if ok:
        return YES
    return NO if severity == ERROR else WARN_MARK


def _summary(run: QualityRun) -> list[str]:
    lines = [
        "| Capa | Tablas | Filas | Checks | OK | Fallas | Avisos |",
        "|---|---|---|---|---|---|---|",
    ]
    for layer, title in LAYER_NAMES.items():
        names = {t.qualified_name for t in DELTA_TABLES if t.layer == layer}
        results = [r for r in run.results if r.table in names]
        rows = sum(run.row_counts.get(n) or 0 for n in names)
        failed = [r for r in results if not r.ok]
        errors = sum(r.severity == ERROR for r in failed)
        lines.append(
            f"| {title} | {len(names)} | {rows:,} | {len(results)} | {len(results) - len(failed)} | "
            f"{errors} | {len(failed) - errors} |"
        )
    return lines


def _access_matrix() -> list[str]:
    header = "| Usuario | Para qu\u00e9 | " + " | ".join(LAYERS) + " |"
    lines = [header, "|---|---|" + "---|" * len(LAYERS)]
    for role in SERVICE_ROLES:
        cells = []
        for bucket in LAYERS:
            grants = [g for g in ACCESS[role] if g.bucket == bucket]
            write = [g for g in grants if g.mode == WRITE]
            if not grants:
                cells.append("sin acceso")
            elif not write:
                cells.append("lee")
            elif all(not g.prefix for g in write):
                cells.append("lee y escribe")
            else:
                cells.append("lee; escribe " + ", ".join(f"`{g.prefix}*`" for g in write))
        lines.append(f"| `{service_user(role)}` | {ROLE_PURPOSE[role]} | " + " | ".join(cells) + " |")
    return lines


def _probes(probes: list[Probe]) -> list[str]:
    ok = sum(p.ok for p in probes)
    lines = [
        f"Prueba en vivo con cada usuario: **{ok} de {len(probes)}** operaciones dieron lo esperado "
        "(permitido o negado).",
        "",
        "| Usuario | Bucket | Operaci\u00f3n | Esperado | Real | |",
        "|---|---|---|---|---|---|",
    ]
    label = {True: "permitido", False: "negado", None: "error"}
    for p in probes:
        user = service_user(p.role) if p.role in SERVICE_ROLES else p.role
        lines.append(
            f"| `{user}` | {p.bucket} | {p.operation} | "
            f"{label[p.expected]} | {label[p.actual]} | {_mark(p.ok)} |"
        )
    return lines


def _table_results(run: QualityRun) -> list[str]:
    lines = []
    for contract in DELTA_TABLES:
        name = contract.qualified_name
        results = [r for r in run.results if r.table == name]
        total = run.row_counts.get(name)
        rows = "no existe" if total is None else f"{total:,} filas"
        bad = sum(not r.ok for r in results)
        lines += [
            "",
            f"### `{name}` ({rows}; {len(results) - bad} de {len(results)} OK)",
            "",
            "| | Tipo | Check | Detalle |",
            "|---|---|---|---|",
        ]
        for r in results:
            lines.append(f"| {_mark(r.ok, r.severity)} | {r.kind} | {r.check} | {r.detail} |")
    return lines


def render_report(
    run: QualityRun,
    probes: list[Probe] | None,
    root_present: bool,
    generated_at: datetime,
) -> str:
    cross = [r for r in run.results if r.kind in ("reconciliacion", "frescura")]
    verdict = "todo en verde" if run.ok else f"{len(run.errors)} fallas"
    root = "S\u00cd (revisar)" if root_present else "no"
    lines = [
        "# Calidad y linaje",
        "",
        "> Generado por `python -m lakehouse.governance check` el "
        f"{generated_at:%Y-%m-%d %H:%M} UTC sobre las tablas de MinIO.",
        "> No se edita a mano. Solo conteos: ning\u00fan valor de los datos.",
        "> Cat\u00e1logo: [CATALOGO.md](CATALOGO.md). Linaje de columnas: [LINAJE.md](LINAJE.md).",
        "",
        f"**Resultado:** {verdict}; {len(run.warnings)} avisos. "
        f"{YES} pasa, {NO} falla (bloquea), {WARN_MARK} aviso (no bloquea).",
        "",
        "## Resumen por capa",
        "",
        *_summary(run),
        "",
        "## Reconciliaci\u00f3n y frescura entre capas",
        "",
        "| | Check | Detalle |",
        "|---|---|---|",
        *[f"| {_mark(r.ok, r.severity)} | {r.check} | {r.detail} |" for r in cross],
        "",
        "## Seguridad: usuarios de MinIO (D17)",
        "",
        "Cada servicio entra con su propio usuario y una pol\u00edtica de m\u00ednimo privilegio",
        "(`infra/minio/policies/`, generadas desde `src/lakehouse/governance/access.py`).",
        "",
        *_access_matrix(),
        "",
        f"- Llaves root de MinIO dentro del contenedor del pipeline: {root} {_mark(not root_present)}",
        "- `ip_addr`: HMAC-SHA256 con sal en Bronce y eliminada en Plata (check de privacidad por tabla).",
        "",
    ]
    skipped = ["Prueba en vivo de accesos: no se corri\u00f3 en esta ejecuci\u00f3n."]
    lines += _probes(probes) if probes else skipped
    lines += [
        "",
        "## Linaje tabla a tabla",
        "",
        "```mermaid",
        mermaid(),
        "```",
        "",
        "## Resultados por tabla",
        *_table_results(run),
    ]
    return "\n".join(lines) + "\n"
