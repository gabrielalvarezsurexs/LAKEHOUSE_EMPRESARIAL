"""Escritura con contrato: Delta rechaza nulls en columnas NOT NULL y guarda los comentarios de columna.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import pytest

from lakehouse.common.delta import TableLocator, count, overwrite, read_table
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
