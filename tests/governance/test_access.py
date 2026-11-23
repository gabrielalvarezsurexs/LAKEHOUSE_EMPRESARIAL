"""Usuarios de MinIO (D17): politicas al dia, minimo privilegio y prueba de accesos con un MinIO falso.

Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

import json
from fnmatch import fnmatchcase
from pathlib import Path

import pytest

from lakehouse.common.config import LAYERS, SERVICE_ROLES, secret_env_var
from lakehouse.common.errors import AccessDeniedError
from lakehouse.governance import access

ROOT = Path(__file__).resolve().parents[2]
POLICY_DIR = ROOT / "infra" / "minio" / "policies"
KEYS = ("plays/part-0.parquet", "ml_skip_predictions/_delta_log/0.json", "_access_probe/x")


def repo_text(path: Path) -> str:
    """Archivos del repo que no estan en la imagen (infra/, compose): la prueba corre en local y en CI."""
    if not path.exists():
        pytest.skip(f"{path.name} no esta en este entorno (contenedor)")
    return path.read_text(encoding="utf-8")


def allowed(policy: dict, action: str, resource: str) -> bool:
    """Evaluador minimo de politicas IAM (solo Allow, como las nuestras)."""
    return any(
        action in st["Action"] and any(fnmatchcase(resource, r) for r in st["Resource"])
        for st in policy["Statement"]
        if st["Effect"] == "Allow"
    )


def test_policy_files_are_up_to_date():
    repo_text(POLICY_DIR / "bi.json")
    files = sorted(p.stem for p in POLICY_DIR.glob("*.json"))
    assert files == sorted(SERVICE_ROLES), "corre: uv run python -m lakehouse.governance docs"
    for role in SERVICE_ROLES:
        assert repo_text(POLICY_DIR / f"{role}.json") == access.policy_json(role)


@pytest.mark.parametrize("role", SERVICE_ROLES)
def test_policy_implements_the_matrix(role):
    policy = json.loads(access.policy_json(role))
    for bucket in LAYERS:
        assert allowed(policy, "s3:ListBucket", f"arn:aws:s3:::{bucket}") == access.can_read(role, bucket)
        for key in KEYS:
            obj = f"arn:aws:s3:::{bucket}/{key}"
            assert allowed(policy, "s3:GetObject", obj) == access.can_read(role, bucket)
            assert allowed(policy, "s3:PutObject", obj) == access.can_write(role, bucket, key)
            assert allowed(policy, "s3:DeleteObject", obj) == access.can_write(role, bucket, key)
    assert not any(a in ("*", "s3:*") for st in policy["Statement"] for a in st["Action"])


def test_least_privilege():
    readers_of_landing = [r for r in SERVICE_ROLES if access.can_read(r, "landing")]
    assert readers_of_landing == ["ingestion"]  # la IP en claro solo la ve la ingesta
    for role in ("governance", "bi"):
        assert not any(access.can_write(role, b, k) for b in LAYERS for k in KEYS)
    assert access.can_write("ml", "gold", "ml_skip_predictions/x")
    assert not access.can_write("ml", "gold", "fact_plays/x")
    assert [b for b in LAYERS if access.can_read("bi", b)] == ["gold"]


class FakeMinio:
    """MinIO en memoria que aplica la matriz (o la ignora, si leaky) para probar la prueba de accesos."""

    def __init__(self, leaky_role=None):
        self.objects = set()
        self.leaky_role = leaky_role

    def factory(self, settings):
        minio = self

        class Store:
            role = settings.role

            def _deny(self, ok):
                if not ok and self.role != minio.leaky_role:
                    raise AccessDeniedError("negado")

            def list_keys(self, bucket, prefix=""):
                self._deny(access.can_read(self.role, bucket))
                return sorted(k for b, k in minio.objects if b == bucket and k.startswith(prefix))

            def put_bytes(self, bucket, key, data, metadata=None):
                self._deny(access.can_write(self.role, bucket, key))
                minio.objects.add((bucket, key))

            def delete(self, bucket, key):
                self._deny(access.can_write(self.role, bucket, key))
                minio.objects.discard((bucket, key))

        return Store()


def test_probe_passes_when_minio_enforces_the_matrix(settings):
    minio = FakeMinio()
    probes = access.probe_all(settings, minio.factory)
    assert probes and all(p.ok for p in probes)
    assert minio.objects == set()  # lo que se escribio se borro
    ml_gold = {p.operation: p.expected for p in probes if p.role == "ml" and p.bucket == "gold"}
    assert ml_gold == {"listar": True, "escribir": False, "escribir ml_*": True}


def test_probe_catches_a_leaky_policy(settings):
    minio = FakeMinio(leaky_role="bi")
    probes = access.probe_all(settings, minio.factory)
    failed = {(p.role, p.bucket, p.operation) for p in probes if not p.ok}
    assert ("bi", "gold", "escribir") in failed and ("bi", "landing", "listar") in failed
    assert all(role == "bi" for role, _, _ in failed)
    assert minio.objects == set()


def test_probe_reports_missing_credentials():
    from lakehouse.common.config import load_settings

    probes = access.probe_role(load_settings({"S3_ENDPOINT": "http://minio:9000"}), "bi", FakeMinio().factory)
    assert len(probes) == 1 and not probes[0].ok and "MINIO_SECRET_BI" in probes[0].detail


def test_root_detection():
    assert access.root_in_environment({"MINIO_ROOT_PASSWORD": "x"})
    assert not access.root_in_environment({"MINIO_SECRET_BI": "x"})


def test_compose_and_init_use_service_users():
    compose = repo_text(ROOT / "docker-compose.yml")
    init = repo_text(ROOT / "infra" / "minio" / "init.sh")
    for role in SERVICE_ROLES:
        assert secret_env_var(role) in compose
    assert "S3_ACCESS_KEY" not in compose and "S3_SECRET_KEY" not in compose
    lakehouse = compose.split("\n  lakehouse:")[1]
    assert "MINIO_ROOT" not in lakehouse, "el root no entra al contenedor del pipeline"
    assert "/init/policies/*.json" in init and 'user="svc-$role"' in init
    assert "lakehouse-$role" in init and access.POLICY_PREFIX == "lakehouse-"
