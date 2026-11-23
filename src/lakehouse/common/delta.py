"""Utilidades de Delta Lake compartidas: rutas por capa, lectura y escritura con el esquema del contrato.

Las tablas se crean desde su contrato (tipos, NOT NULL y comentario por columna), asi Delta rechaza una
escritura que lo rompa en vez de cambiar el esquema en silencio.
Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.common.config import Settings
from lakehouse.common.errors import TableMissingError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.governance.contracts import TableContract


@dataclass(frozen=True)
class TableLocator:
    """Ruta de cada tabla segun su capa; en pruebas apunta a un directorio local."""

    root: Callable[[str, str], str]

    @classmethod
    def from_settings(cls, settings: Settings) -> TableLocator:
        return cls(settings.table_uri)

    @classmethod
    def at(cls, base: str) -> TableLocator:
        """<base>/<capa>/<tabla>; misma forma que BronzeTables.at(<base>/bronze)."""
        return cls(lambda layer, name: f"{base.rstrip('/')}/{layer}/{name}")

    def uri(self, contract: TableContract) -> str:
        return self.root(contract.layer, contract.name)


def is_delta(spark: SparkSession, uri: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, uri)


def read_table(spark: SparkSession, locator: TableLocator, contract: TableContract) -> DataFrame:
    """Lee una tabla que debe existir; si falta, el error dice cual y de que capa."""
    uri = locator.uri(contract)
    if not is_delta(spark, uri):
        raise TableMissingError(f"No existe {contract.layer}.{contract.name} ({uri}); corre antes su etapa.")
    return spark.read.format("delta").load(uri)


def read_optional(spark: SparkSession, locator: TableLocator, contract: TableContract) -> DataFrame | None:
    uri = locator.uri(contract)
    return spark.read.format("delta").load(uri) if is_delta(spark, uri) else None


def ensure_table(spark: SparkSession, uri: str, contract: TableContract) -> None:
    """Crea la tabla vacia con el esquema del contrato (NOT NULL y comentarios) si aun no existe."""
    from delta.tables import DeltaTable

    if is_delta(spark, uri):
        return
    builder = DeltaTable.createIfNotExists(spark).location(uri).comment(contract.description)
    for col in contract.columns:
        # API de Python: addColumn(nombre, tipo DDL, nullable, comment=...); no hay columnBuilder.
        builder = builder.addColumn(col.name, col.dtype, nullable=col.nullable, comment=col.description)
    if contract.partition_by:
        builder = builder.partitionedBy(*contract.partition_by)
    builder.execute()


def overwrite(spark: SparkSession, df: DataFrame, locator: TableLocator, contract: TableContract) -> None:
    """Reemplaza el contenido completo; mergeSchema permite agregar columnas nuevas, no cambiar tipos."""
    uri = locator.uri(contract)
    ensure_table(spark, uri, contract)
    writer = contract.conform(df).write.format("delta").mode("overwrite").option("mergeSchema", "true")
    if contract.partition_by:
        writer = writer.partitionBy(*contract.partition_by)
    writer.save(uri)


def count(spark: SparkSession, locator: TableLocator, contract: TableContract) -> int:
    """Filas de la tabla; 0 si no existe."""
    df = read_optional(spark, locator, contract)
    return 0 if df is None else df.count()
