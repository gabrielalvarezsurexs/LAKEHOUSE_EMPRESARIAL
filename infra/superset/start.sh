#!/bin/sh
# Responsable: Emilio Rendon Trias (Analista de Datos / BI)
# Arranque de Superset (D26): metadatos nuevos en cada arranque, admin del .env, import del tablero desde
# bi/ y luego el servidor. El importador de Superset no sobrescribe graficas ni datasets que ya existen; por
# eso se empieza de cero: el tablero que se ve siempre es el del repo (cambios hechos en la UI se pierden).
set -eu

BI_DIR=/opt/lakehouse/bi
BUNDLE=/tmp/lakehouse_dashboard.zip

rm -f "${SUPERSET_HOME}/superset.db"
superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname Lakehouse \
    --email admin@lakehouse.local --password "${SUPERSET_ADMIN_PASSWORD}"
superset init

python "${BI_DIR}/dashboard.py" --out "${BUNDLE}"
superset import-dashboards --path "${BUNDLE}" --username admin
echo "Tablero importado: /superset/dashboard/spotify-lakehouse/"

exec /app/docker/entrypoints/run-server.sh
