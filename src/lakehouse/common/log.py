"""Logs con un solo formato para todo el pipeline (nivel por LOG_LEVEL).

Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
"""

import logging
import os

_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Devuelve un logger configurado; la configuracion raiz se aplica una sola vez."""
    root = logging.getLogger()
    if not getattr(root, "_lakehouse_configured", False):
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=level if level in logging.getLevelNamesMapping() else "INFO", format=_FORMAT
        )
        # py4j (Spark) y alembic (migraciones de Dagster) son muy ruidosos en INFO y no aportan al pipeline.
        for noisy in ("py4j", "alembic"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        root._lakehouse_configured = True  # type: ignore[attr-defined]
    return logging.getLogger(name)
