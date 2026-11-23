"""Escritura con contrato: Delta rechaza nulls en columnas NOT NULL y guarda los comentarios de columna.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import pytest

from lakehouse.common.delta import TableLocator, count, ensure_table, overwrite, read_table
from lakehouse.governance.contracts import Column, TableContract

pytestmark = pytest.mark.spark

CONTRACT = TableContract(
    name="demo",
    layer="silver",
    owner="pruebas",
    description="Tabla de prueba",
    columns=(Column("id", "STRING", "llave", nullable=False), Column("value", "INT", "valor")),
)


def test_overwrite_enforces_contract(spark, tmp_path):
    from delta.tables import DeltaTable

    locator = TableLocator.at(str(tmp_path))
    assert count(spark, locator, CONTRACT) == 0
    overwrite(
        spark, spark.createDataFrame([("a", 1), ("b", None)], "id STRING, value INT"), locator, CONTRACT
    )

    with pytest.raises(Exception, match="NOT NULL"):
        overwrite(spark, spark.createDataFrame([(None, 3)], "id STRING, value INT"), locator, CONTRACT)

    table = read_table(spark, locator, CONTRACT)
    assert table.count() == 2  # la escritura fallida no dejo nada a medias
    assert table.schema["id"].metadata.get("comment") == "llave"
    assert table.schema["id"].nullable is False
    props = DeltaTable.forPath(spark, locator.uri(CONTRACT)).detail().first()["properties"]
    assert props["lakehouse.owner"] == "pruebas" and props["lakehouse.classification"] == "interno"


def test_ensure_table_syncs_metadata_of_an_old_table(spark, tmp_path):
    """Una tabla escrita sin contrato (Bronce de F2) recibe comentarios y propiedades, una sola vez."""
    from delta.tables import DeltaTable

    uri = str(tmp_path / "vieja")
    spark.createDataFrame([("a", 1)], "id STRING, value INT").write.format("delta").save(uri)
    versions = DeltaTable.forPath(spark, uri).history().count()

    assert ensure_table(spark, uri, CONTRACT) == 3  # propiedades + 2 comentarios
    assert ensure_table(spark, uri, CONTRACT) == 0  # ya al dia: sin commits nuevos
    table = DeltaTable.forPath(spark, uri)
    assert table.history().count() == versions + 3
    detail = table.detail().first()
    assert detail["description"] == "Tabla de prueba"
    assert detail["properties"]["lakehouse.layer"] == "silver"
    schema = spark.read.format("delta").load(uri).schema
    assert [f.metadata.get("comment") for f in schema.fields] == ["llave", "valor"]
    assert spark.read.format("delta").load(uri).count() == 1
