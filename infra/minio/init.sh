#!/bin/sh
# Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
# Corre una vez en el servicio minio-init: espera a MinIO y crea un bucket por capa (D3).
# F4 (Victor) agrega aqui los usuarios y politicas por servicio (D17).
set -eu

ALIAS=local
BUCKETS="landing bronze silver gold"

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
