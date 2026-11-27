"""Prueba del tablero en vivo: entra a la API de Superset y corre la consulta de cada grafica contra Oro.

Corre dentro del contenedor de Superset (CI y Gabriel): python /opt/lakehouse/bi/check.py
Imprime solo conteos de filas y tiempos (nunca datos). Falla si una grafica no responde o falta.
Responsable: Emilio Rendon Trias (Analista de Datos / BI)
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dashboard import CHARTS, SLUG  # noqa: E402

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SLOW_SECONDS = 5.0  # la rubrica pide un tablero que cargue en segundos


def call(path: str, token: str | None = None, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method="POST" if body is not None else "GET")
    if data is not None:
        # En un GET, Superset intenta leer el JSON si viene este encabezado y responde 400 con cuerpo vacio.
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def login() -> str:
    password = os.environ.get("SUPERSET_ADMIN_PASSWORD", "")
    if not password:
        raise SystemExit("Falta SUPERSET_ADMIN_PASSWORD en el entorno del contenedor")
    body = {"username": "admin", "password": password, "provider": "db", "refresh": False}
    return call("/api/v1/security/login", body=body)["access_token"]


def chart_ids(token: str) -> dict[str, int]:
    found = call("/api/v1/chart/?q=(columns:!(id,slice_name),page_size:100)", token)["result"]
    return {c["slice_name"]: c["id"] for c in found}


def check_chart(token: str, chart_id: int) -> tuple[bool, int, float, str]:
    """(ok, filas, segundos, error) de la consulta guardada de la grafica."""
    start = time.monotonic()
    try:
        result = call(f"/api/v1/chart/{chart_id}/data/?format=json&force=true", token)["result"]
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace")[:300]
        return False, 0, time.monotonic() - start, f"HTTP {err.code}: {detail}"
    elapsed = time.monotonic() - start
    errors = [q.get("error") for q in result if q.get("error")]
    rows = sum(q.get("rowcount", 0) for q in result)
    return not errors, rows, elapsed, "; ".join(map(str, errors))


def main() -> int:
    token = login()
    call(f"/api/v1/dashboard/{SLUG}", token)  # 404 si el import fallo
    ids = chart_ids(token)
    failures = 0
    total = 0.0
    for chart in CHARTS:
        if chart.title not in ids:
            print(f"FALTA   {chart.key}: no se importo")
            failures += 1
            continue
        ok, rows, seconds, error = check_chart(token, ids[chart.title])
        total += seconds
        status = "OK" if ok else "FALLA"
        slow = " (lenta)" if seconds > SLOW_SECONDS else ""
        print(
            f"{status:<7} {chart.key:<18} filas={rows:<6} {seconds:5.2f} s{slow}"
            + (f"  {error}" if error else "")
        )
        failures += not ok
    print(f"Tablero {SLUG}: {len(CHARTS) - failures} de {len(CHARTS)} graficas OK; {total:.1f} s en total")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
