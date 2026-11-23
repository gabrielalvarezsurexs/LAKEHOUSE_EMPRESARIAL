"""Usuarios de servicio de MinIO (D17): matriz de acceso, politicas que la implementan y prueba en vivo.

La matriz ACCESS es la unica definicion: de ella salen los JSON de infra/minio/policies/ (los aplica
minio-init) y la prueba que confirma, con cada usuario, que MinIO permite y niega lo esperado.
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from lakehouse.common.config import LAYERS, SERVICE_ROLES, Settings, service_user
from lakehouse.common.errors import AccessDeniedError, LakehouseError
from lakehouse.common.log import get_logger

log = get_logger("lakehouse.governance.access")

READ, WRITE = "r", "rw"
PROBE_DIR = "_access_probe"
POLICY_PREFIX = "lakehouse-"
ROOT_ENV_VARS = ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")


@dataclass(frozen=True)
class Grant:
    """Permiso de un rol sobre un bucket; prefix limita a las llaves que empiezan asi ("" = todo)."""

    bucket: str
    mode: str
    prefix: str = ""


# Minimo privilegio: landing (IP en claro) solo la toca la ingesta; BI solo lee Oro; ML solo escribe
# sus tablas ml_* en Oro; gobernanza lee todo menos landing.
ACCESS: dict[str, tuple[Grant, ...]] = {
    "ingestion": (Grant("landing", WRITE), Grant("bronze", WRITE)),
    "transformation": (Grant("bronze", READ), Grant("silver", WRITE), Grant("gold", WRITE)),
    "governance": (Grant("bronze", READ), Grant("silver", READ), Grant("gold", READ)),
    "ml": (Grant("gold", READ), Grant("gold", WRITE, "ml_")),
    "bi": (Grant("gold", READ),),
}

ROLE_PURPOSE = {
    "ingestion": "Sube los JSON a landing y escribe Bronce",
    "transformation": "Lee Bronce y escribe Plata y Oro",
    "governance": "Lee Bronce, Plata y Oro para los checks de calidad",
    "ml": "Lee Oro y escribe solo sus tablas ml_* (F6)",
    "bi": "Superset: solo lectura de Oro (F7)",
}

# Acciones S3 que usan boto3 y S3A (Spark): lectura, escritura y subidas multiparte de archivos grandes.
_BUCKET_ACTIONS = ["s3:GetBucketLocation", "s3:ListBucket"]
_MULTIPART_BUCKET_ACTIONS = ["s3:ListBucketMultipartUploads"]
_READ_ACTIONS = ["s3:GetObject"]
_WRITE_ACTIONS = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]


def can_read(role: str, bucket: str) -> bool:
    return any(g.bucket == bucket for g in ACCESS[role])


def can_write(role: str, bucket: str, key: str) -> bool:
    return any(g.bucket == bucket and g.mode == WRITE and key.startswith(g.prefix) for g in ACCESS[role])


def _arn(bucket: str, prefix: str | None = None) -> str:
    return f"arn:aws:s3:::{bucket}" if prefix is None else f"arn:aws:s3:::{bucket}/{prefix}*"


def policy_name(role: str) -> str:
    return f"{POLICY_PREFIX}{role}"


def policy_document(role: str) -> dict:
    """Politica IAM de MinIO para el rol; leer un bucket con grant de escritura parcial lee todo el bucket."""
    grants = ACCESS[role]
    buckets = sorted({g.bucket for g in grants})
    statements = [
        {
            "Sid": "ListBuckets",
            "Effect": "Allow",
            "Action": _BUCKET_ACTIONS,
            "Resource": [_arn(b) for b in buckets],
        },
        {
            "Sid": "Read",
            "Effect": "Allow",
            "Action": _READ_ACTIONS,
            "Resource": [_arn(b, "") for b in buckets],
        },
    ]
    writes = [g for g in grants if g.mode == WRITE]
    if writes:
        write_buckets = sorted({g.bucket for g in writes})
        statements += [
            {
                "Sid": "MultipartList",
                "Effect": "Allow",
                "Action": _MULTIPART_BUCKET_ACTIONS,
                "Resource": [_arn(b) for b in write_buckets],
            },
            {
                "Sid": "Write",
                "Effect": "Allow",
                "Action": _WRITE_ACTIONS,
                "Resource": sorted(_arn(g.bucket, g.prefix) for g in writes),
            },
        ]
    return {"Version": "2012-10-17", "Statement": statements}


def policy_json(role: str) -> str:
    return json.dumps(policy_document(role), indent=2) + "\n"


def write_policies(directory: str | Path) -> list[Path]:
    """Escribe <rol>.json por rol (los lee infra/minio/init.sh); regresa las rutas."""
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for role in SERVICE_ROLES:
        path = out / f"{role}.json"
        path.write_text(policy_json(role), encoding="utf-8", newline="\n")
        paths.append(path)
    return paths


@dataclass(frozen=True)
class Probe:
    """Resultado de intentar una operacion con un usuario: expected/actual son True si se permite."""

    role: str
    bucket: str
    operation: str
    expected: bool
    actual: bool | None  # None = error que no es de permisos
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.actual is not None and self.actual == self.expected


def _attempt(action) -> tuple[bool | None, str]:
    try:
        action()
    except AccessDeniedError:
        return False, ""
    except LakehouseError as exc:
        return None, type(exc).__name__
    return True, ""


def probe_role(settings: Settings, role: str, store_factory=None) -> list[Probe]:
    """Con el usuario del rol: lista cada bucket y escribe (y borra) un objeto de prueba donde aplique."""
    from lakehouse.common.storage import S3ObjectStore

    factory = store_factory or S3ObjectStore
    try:
        store = factory(settings.for_role(role))
    except LakehouseError as exc:
        return [Probe(role, "-", "credenciales", True, None, str(exc))]
    run_id = uuid.uuid4().hex[:8]
    probes = []
    for bucket in LAYERS:
        actual, detail = _attempt(lambda b=bucket: store.list_keys(b, f"{PROBE_DIR}/"))
        probes.append(Probe(role, bucket, "listar", can_read(role, bucket), actual, detail))
        prefixes = [""] + sorted({g.prefix for g in ACCESS[role] if g.bucket == bucket and g.prefix})
        for prefix in prefixes:
            key = f"{prefix}{PROBE_DIR}/{run_id}"
            actual, detail = _attempt(lambda b=bucket, k=key: store.put_bytes(b, k, b"probe"))
            if actual:
                # Se escribio (esperado o no): se borra para no dejar basura.
                _attempt(lambda b=bucket, k=key: store.delete(b, k))
            label = f"escribir {prefix}*" if prefix else "escribir"
            probes.append(Probe(role, bucket, label, can_write(role, bucket, key), actual, detail))
    return probes


def probe_all(settings: Settings, store_factory=None) -> list[Probe]:
    probes = [p for role in SERVICE_ROLES for p in probe_role(settings, role, store_factory)]
    failed = [p for p in probes if not p.ok]
    log.info(
        "Acceso MinIO: %d pruebas, %d como se esperaba, %d fallas",
        len(probes),
        len(probes) - len(failed),
        len(failed),
    )
    for p in failed:
        log.error(
            "[FALLA] %s en %s (%s): esperado=%s, real=%s %s",
            service_user(p.role),
            p.bucket,
            p.operation,
            "permitido" if p.expected else "negado",
            {True: "permitido", False: "negado", None: "error"}[p.actual],
            p.detail,
        )
    return probes


def root_in_environment(env=None) -> bool:
    """True si las llaves root de MinIO estan en el proceso (no deberian, D17)."""
    env = os.environ if env is None else env
    return any(env.get(name) for name in ROOT_ENV_VARS)
