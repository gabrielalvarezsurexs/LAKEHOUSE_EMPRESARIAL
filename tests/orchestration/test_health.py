"""Reglas de alerta y medicion de buckets del monitoreo (sin Spark ni MinIO).

Responsable: Nora Valentina Frias Alpizar (Ingeniera DevOps / MLOps)
"""

from lakehouse.common.config import LAYERS
from lakehouse.common.errors import AccessDeniedError
from lakehouse.governance.contracts import DELTA_TABLES
from lakehouse.orchestration import health
from lakehouse.orchestration.health import BucketUsage, TableHealth


def _ok(table, **kw):
    base = {"rows": 10, "files": 2, "bytes": 5 * 2**20, "version": 4}
    return TableHealth(table, exists=True, **(base | kw))


def test_no_alerts_when_everything_is_fine():
    tables = [_ok(c.qualified_name) for c in DELTA_TABLES]
    assert health.alerts_for(tables, [BucketUsage("gold", 3, 100)]) == []


def test_alert_rules():
    allow_empty = next(c.qualified_name for c in DELTA_TABLES if c.allow_empty)
    tables = [
        TableHealth("gold.fact_plays", exists=False),
        _ok("silver.plays", rows=0),
        _ok(allow_empty, rows=0),
        _ok("gold.dim_track", files=health.SMALL_FILE_MIN_FILES, bytes=health.SMALL_FILE_MIN_FILES * 1000),
        _ok("silver.artists", version=health.MANY_VERSIONS),
    ]
    alerts = health.alerts_for(tables, [BucketUsage("landing", None, None, "Permiso negado")])
    found = {(a.level, a.target) for a in alerts}
    assert found == {
        ("error", "bucket landing"),
        ("error", "gold.fact_plays"),
        ("warn", "silver.plays"),
        ("warn", "gold.dim_track"),
        ("warn", "silver.artists"),
    }


def test_bucket_usage_uses_the_role_that_can_read_it(settings, store):
    store.put_bytes("landing", "spotify/a.json", b"12345")
    roles = []

    def factory(role_settings):
        roles.append(role_settings.role)
        return store

    usage = health.bucket_usage(settings, "landing", factory)
    assert (usage.objects, usage.bytes) == (1, 5)
    assert roles == ["ingestion"]
    assert set(health.BUCKET_READERS) == set(LAYERS)


def test_bucket_usage_error_becomes_alert(settings):
    class Denied:
        def __init__(self, _):
            pass

        def usage(self, bucket, prefix=""):
            raise AccessDeniedError(f"Permiso negado al medir s3://{bucket}/")

    usage = health.bucket_usage(settings, "gold", Denied)
    assert usage.objects is None and "Permiso negado" in usage.error


def test_report_serializes_counts_only():
    report = health.HealthReport("2026-11-23T00:00:00+00:00", [_ok("gold.fact_plays")], [], [])
    data = report.to_dict()
    assert set(data["tables"][0]) == {
        "table",
        "exists",
        "rows",
        "files",
        "bytes",
        "version",
        "last_operation",
        "last_commit",
    }
