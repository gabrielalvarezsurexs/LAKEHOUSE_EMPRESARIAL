"""Errores propios del lakehouse; cada rol puede heredar de LakehouseError.

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""


class LakehouseError(Exception):
    """Base de todos los errores del proyecto."""


class ConfigError(LakehouseError):
    """Falta una variable de entorno o tiene un valor invalido."""


class StorageError(LakehouseError):
    """MinIO no responde o falta un bucket esperado."""
