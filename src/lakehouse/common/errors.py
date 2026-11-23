"""Errores propios del lakehouse; cada rol puede heredar de LakehouseError.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""


class LakehouseError(Exception):
    """Base de todos los errores del proyecto."""


class ConfigError(LakehouseError):
    """Falta una variable de entorno o tiene un valor invalido."""


class StorageError(LakehouseError):
    """MinIO no responde o falta un bucket esperado."""


class AccessDeniedError(StorageError):
    """MinIO nego la operacion: el usuario de servicio no tiene permiso (D17)."""


class SchemaError(LakehouseError):
    """Un DataFrame o una tabla no cumple su contrato de esquema."""


class TableMissingError(LakehouseError):
    """Falta una tabla Delta que la etapa necesita (normalmente: no se corrio la etapa anterior)."""
