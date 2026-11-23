#!/bin/sh
# Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider), usuarios y politicas: Victor Gabriel
# Torres Guerrero (Gobernanza). Corre en minio-init en cada `docker compose up`: espera a MinIO, crea un bucket
# por capa (D3) y un usuario por servicio con politica minima (D17). Todo es idempotente.
set -eu

ALIAS=local
BUCKETS="landing bronze silver gold"
MIN_SECRET=8

tries=0
until mc alias set "$ALIAS" "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
        echo "MinIO no respondio en 60 s en $MINIO_ENDPOINT" >&2
        exit 1
    fi
    sleep 1
done
echo "MinIO listo en $MINIO_ENDPOINT"

for bucket in $BUCKETS; do
    mc mb --ignore-existing "$ALIAS/$bucket"
done
mc ls "$ALIAS"

# Un usuario svc-<rol> por cada politica <rol>.json. Las politicas se generan desde
# src/lakehouse/governance/access.py (uv run python -m lakehouse.governance docs); no se editan a mano.
# La contrasena de cada usuario viene de MINIO_SECRET_<ROL> en .env y nunca se imprime.
for policy in /init/policies/*.json; do
    role=$(basename "$policy" .json)
    user="svc-$role"
    var="MINIO_SECRET_$(echo "$role" | tr '[:lower:]' '[:upper:]')"
    eval "secret=\${$var:-}"
    if [ "${#secret}" -lt "$MIN_SECRET" ]; then
        echo "Falta $var en .env (minimo $MIN_SECRET caracteres; README, paso 1)" >&2
        exit 1
    fi
    mc admin policy create "$ALIAS" "lakehouse-$role" "$policy" >/dev/null
    mc admin user add "$ALIAS" "$user" "$secret" >/dev/null
    # attach falla si la politica ya estaba asignada: eso cuenta como exito.
    if ! out=$(mc admin policy attach "$ALIAS" "lakehouse-$role" --user "$user" 2>&1); then
        case "$out" in
            *already*) ;;
            *) echo "$out" >&2; exit 1 ;;
        esac
    fi
    echo "Usuario $user con politica lakehouse-$role"
done
