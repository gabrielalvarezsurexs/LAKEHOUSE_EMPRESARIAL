"""Parseo y validacion de los JSON de Spotify contra el contrato; separa filas validas de la cuarentena.

Python puro (sin Spark) para poder probarlo en segundos; bronze.py convierte el resultado en tablas Delta.
Responsable: Ricardo Zeron del Valle (Ingeniero de Datos, Ingesta)
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from lakehouse.governance.contracts import SPOTIFY_STREAMS_SOURCE, Column

_SOURCE_NAMES = frozenset(SPOTIFY_STREAMS_SOURCE.column_names)
_RAW_LIMIT = 4000  # tope de caracteres del registro crudo en cuarentena


@dataclass
class QuarantineRecord:
    record_index: int | None  # None = fallo el archivo completo
    cause: str
    raw_record: str | None


@dataclass
class ParsedFile:
    """Resultado de un archivo: filas listas para Bronce (sin metadatos de ingesta) y rechazos."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    quarantine: list[QuarantineRecord] = field(default_factory=list)
    rows_read: int = 0
    unknown_fields: set[str] = field(default_factory=set)


def file_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def hash_ip(ip: str, salt: str) -> str:
    """Seudonimo estable de una IP: HMAC-SHA256 con la sal del .env (sin la sal no se puede revertir)."""
    return hmac.new(salt.encode("utf-8"), ip.encode("utf-8"), hashlib.sha256).hexdigest()


def _parse_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    # Sin zona horaria la hora es ambigua: mejor rechazarla que adivinar.
    return parsed.astimezone(UTC) if parsed.tzinfo else None


def _coerce(col: Column, value: Any) -> tuple[Any, str | None]:
    """Valida un valor contra el tipo del contrato; devuelve (valor, causa de rechazo o None)."""
    if col.dtype == "STRING":
        ok = isinstance(value, str)
    elif col.dtype == "BIGINT":
        ok = isinstance(value, int) and not isinstance(value, bool)  # en Python True es un int
    elif col.dtype == "BOOLEAN":
        ok = isinstance(value, bool)
    elif col.dtype == "TIMESTAMP":
        parsed = _parse_timestamp(value) if isinstance(value, str) else None
        return (parsed, None) if parsed else (None, f"bad_timestamp:{col.name}")
    else:
        raise ValueError(f"Tipo sin validador: {col.dtype}")
    return (value, None) if ok else (None, f"bad_type:{col.name}")


def validate_record(record: Any, salt: str) -> tuple[dict[str, Any] | None, str | None]:
    """Convierte un registro crudo en fila de Bronce; si no cumple, devuelve (None, causa)."""
    if not isinstance(record, dict):
        return None, "not_an_object"
    row: dict[str, Any] = {}
    for col in SPOTIFY_STREAMS_SOURCE.columns:
        value = record.get(col.name)
        if value is None:
            if not col.nullable:
                return None, f"missing_required:{col.name}"
            row[col.name] = None
            continue
        coerced, cause = _coerce(col, value)
        if cause:
            return None, cause
        row[col.name] = coerced
    if row["ms_played"] < 0:
        return None, "negative:ms_played"
    ip = row.pop("ip_addr")
    row["ip_addr_hash"] = hash_ip(ip, salt) if ip else None
    return row, None


def _safe_raw(record: Any, salt: str) -> str:
    """JSON del registro rechazado sin la IP en claro (la cuarentena tambien es Bronce)."""
    if isinstance(record, dict):
        record = dict(record)
        ip = record.pop("ip_addr", None)
        if ip is not None:
            record["ip_addr_hash"] = hash_ip(str(ip), salt)
    return json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)[:_RAW_LIMIT]


def parse_spotify_file(content: bytes, salt: str) -> ParsedFile:
    """Parsea un archivo completo; un archivo ilegible es un solo renglon de cuarentena, no una excepcion."""
    result = ParsedFile()
    try:
        data = json.loads(content.decode("utf-8-sig"))
    except UnicodeDecodeError:
        result.quarantine.append(QuarantineRecord(None, "invalid_encoding", None))
        return result
    except json.JSONDecodeError:
        result.quarantine.append(QuarantineRecord(None, "invalid_json", None))
        return result
    if not isinstance(data, list):
        result.quarantine.append(QuarantineRecord(None, "not_a_json_array", None))
        return result

    result.rows_read = len(data)
    for index, record in enumerate(data):
        if isinstance(record, dict):
            result.unknown_fields.update(record.keys() - _SOURCE_NAMES)
        row, cause = validate_record(record, salt)
        if cause:
            result.quarantine.append(QuarantineRecord(index, cause, _safe_raw(record, salt)))
        else:
            row["_record_index"] = index
            result.rows.append(row)
    return result
