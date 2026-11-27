# Responsable: Emilio Rendon Trias (Analista de Datos / BI)
# Superset (D13, D26) con DuckDB para leer Oro (Delta en MinIO). Imagen oficial 6.1.0 fijada por digest; trae
# Python 3.10, por eso DuckDB 1.5 (la 2.x pide 3.11). Mismas versiones que el grupo dev de uv (pruebas locales).
FROM apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88

USER root

# Extensiones de DuckDB instaladas al construir: al arrancar no se descarga nada.
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions

RUN uv pip install --python /app/.venv/bin/python --no-cache duckdb==1.5.6 duckdb-engine==0.17.0 \
    && /app/.venv/bin/python -c "import duckdb; c = duckdb.connect(config={'extension_directory': '/opt/duckdb/extensions'}); c.install_extension('delta'); c.install_extension('httpfs')" \
    && chmod -R a+rX /opt/duckdb

USER superset
