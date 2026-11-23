"""Contratos de esquema (sin Spark): columnas unicas, particiones validas y nada de PII despues de Bronce.

Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

import pytest

from lakehouse.common.errors import SchemaError
from lakehouse.governance import contracts as c

ALL = [v for v in vars(c).values() if isinstance(v, c.TableContract)]


@pytest.mark.parametrize("contract", ALL, ids=lambda t: f"{t.layer}.{t.name}")
def test_contract_is_consistent(contract):
    names = contract.column_names
    assert len(names) == len(set(names)), "columna repetida"
    assert set(contract.partition_by) <= set(names)
    assert contract.layer in ("landing", "bronze", "silver", "gold")
    assert all(col.description for col in contract.columns)


@pytest.mark.parametrize("contract", c.SILVER_TABLES + c.GOLD_TABLES, ids=lambda t: f"{t.layer}.{t.name}")
def test_no_personal_data_after_bronze(contract):
    assert not any(col.pii for col in contract.columns)
    assert not any("ip_addr" in name for name in contract.column_names)


def test_conform_reports_missing_columns():
    class FakeFrame:
        columns = ["play_id", "ts_start"]

    with pytest.raises(SchemaError, match="ts_end"):
        c.SILVER_PLAYS.conform(FakeFrame())
