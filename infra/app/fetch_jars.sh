#!/bin/sh
# Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
# Baja los JAR de Delta y S3A que corresponden a las versiones del uv.lock y los deja en pyspark/jars,
# asi Spark los carga sin descargar nada en cada corrida. Verifica el SHA-1 publicado en Maven Central.
set -eu

PY="${UV_PROJECT_ENVIRONMENT:-/opt/venv}/bin/python"
MAVEN="https://repo1.maven.org/maven2"
# hadoop-aws 3.3.4 se compilo contra este bundle; cambiarlo rompe S3A.
AWS_SDK_VERSION="${AWS_SDK_VERSION:-1.12.262}"

JARS_DIR=$("$PY" -c "import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
DELTA_VERSION=$("$PY" -c "import importlib.metadata as m; print(m.version('delta-spark'))")
SCALA_VERSION=$(ls "$JARS_DIR" | sed -n 's/^scala-library-\([0-9]*\.[0-9]*\)\..*\.jar$/\1/p')
HADOOP_VERSION=$(ls "$JARS_DIR" | sed -n 's/^hadoop-client-api-\(.*\)\.jar$/\1/p')

if [ -z "$SCALA_VERSION" ] || [ -z "$HADOOP_VERSION" ]; then
    echo "No pude detectar Scala o Hadoop en $JARS_DIR" >&2
    exit 1
fi
echo "Delta $DELTA_VERSION | Scala $SCALA_VERSION | Hadoop $HADOOP_VERSION | AWS SDK $AWS_SDK_VERSION"

fetch() {
    # $1 = ruta del artefacto en Maven (grupo/artefacto/version), $2 = nombre del JAR
    url="$MAVEN/$1/$2"
    echo "Bajando $2 ..."
    # Timeouts para que una conexion colgada falle en vez de dejar el build mudo para siempre.
    curl -fsSL --connect-timeout 20 --max-time 900 --retry 3 --retry-delay 3 -o "$JARS_DIR/$2" "$url"
    expected=$(curl -fsSL --connect-timeout 20 --max-time 60 --retry 3 "$url.sha1" | cut -c1-40)
    actual=$(sha1sum "$JARS_DIR/$2" | cut -c1-40)
    if [ "$expected" != "$actual" ]; then
        echo "SHA-1 no coincide para $2" >&2
        exit 1
    fi
    echo "OK $2"
}

fetch "io/delta/delta-spark_$SCALA_VERSION/$DELTA_VERSION" "delta-spark_$SCALA_VERSION-$DELTA_VERSION.jar"
fetch "io/delta/delta-storage/$DELTA_VERSION" "delta-storage-$DELTA_VERSION.jar"
fetch "org/apache/hadoop/hadoop-aws/$HADOOP_VERSION" "hadoop-aws-$HADOOP_VERSION.jar"
fetch "com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION" "aws-java-sdk-bundle-$AWS_SDK_VERSION.jar"
