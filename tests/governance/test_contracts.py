"""Contratos de esquema (sin Spark): consistencia, privacidad, clasificacion y reglas de calidad validas.

Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

import pytest

from lakehouse.common.errors import SchemaError
from lakehouse.governance import contracts as c
from lakehouse.transformation import catalogs, silver

ALL = list(c.ALL_TABLES)
NUMERIC = ("INT", "BIGINT", "DOUBLE")
SENSITIVITY = list(c.CLASSIFICATIONS)  # de mas a menos sensible


def _ids(t):
    return t.qualified_name


def test_registry_has_every_contract():
    declared = [v for v in vars(c).values() if isinstance(v, c.TableContract)]
    assert {t.qualified_name for t in declared} == {t.qualified_name for t in ALL}
    assert len(ALL) == len({t.qualified_name for t in ALL})


@pytest.mark.parametrize("contract", ALL, ids=_ids)
def test_contract_is_consistent(contract):
    names = contract.column_names
    assert len(names) == len(set(names)), "columna repetida"
    assert set(contract.partition_by) <= set(names)
    assert contract.layer in ("landing", "bronze", "silver", "gold")
    assert contract.classification in c.CLASSIFICATIONS
    assert all(col.description for col in contract.columns)
    assert all(col.privacy in c.PRIVACY_LEVELS for col in contract.columns)
    assert contract.sources, "toda tabla dice de que se alimenta (linaje)"


@pytest.mark.parametrize("contract", ALL, ids=_ids)
def test_quality_rules_point_to_real_columns(contract):
    names = set(contract.column_names)
    for key in contract.unique_keys:
        assert set(key) <= names
    for fk in contract.references:
        ref = c.by_name(fk.ref)
        assert fk.column in names and fk.ref_column in ref.column_names
        assert contract.column(fk.column).dtype == ref.column(fk.ref_column).dtype
    for col in contract.columns:
        if col.values:
            assert col.dtype == "STRING"
        if col.min_value is not None or col.max_value is not None:
            assert col.dtype in NUMERIC
    assert all(r.name and r.sql for r in contract.rules)


@pytest.mark.parametrize("contract", ALL, ids=_ids)
def test_classification_matches_privacy(contract):
    level = SENSITIVITY.index(contract.classification)
    if any(col.privacy == c.PERSONAL for col in contract.columns):
        assert contract.classification == "restringido"
    if any(col.privacy == c.PSEUDONYM for col in contract.columns):
        assert level <= SENSITIVITY.index("confidencial")


@pytest.mark.parametrize("contract", c.SILVER_TABLES + c.GOLD_TABLES, ids=_ids)
def test_no_personal_data_after_bronze(contract):
    assert not any(col.pii for col in contract.columns)
    assert not any("ip_addr" in name for name in contract.column_names)
    assert contract.classification == "interno"


def test_ip_only_as_pseudonym_in_bronze():
    for contract in c.BRONZE_TABLES:
        assert "ip_addr" not in contract.column_names
    assert c.BRONZE_SPOTIFY_STREAMS.column("ip_addr_hash").privacy == c.PSEUDONYM


def test_accepted_values_match_transformation_catalogs():
    families = {name for name, _ in catalogs.PLATFORM_FAMILIES} | {catalogs.OTHER_PLATFORM}
    assert set(c.PLATFORM_FAMILY_VALUES) == families
    assert set(c.DEVICE_TYPE_VALUES) == set(catalogs.DEVICE_TYPES.values())
    assert set(c.DISCARD_CAUSES) == {silver.CAUSE_DUPLICATE, silver.CAUSE_NO_CONTENT, silver.CAUSE_AUDIOBOOK}


def test_table_properties_carry_owner_and_classification():
    props = c.SILVER_PLAYS.table_properties()
    assert props == {
        "lakehouse.owner": c.TRANSFORMATION_OWNER,
        "lakehouse.layer": "silver",
        "lakehouse.classification": "interno",
    }


def test_conform_reports_missing_columns():
    class FakeFrame:
        columns = ["play_id", "ts_start"]

    with pytest.raises(SchemaError, match="ts_end"):
        c.SILVER_PLAYS.conform(FakeFrame())
