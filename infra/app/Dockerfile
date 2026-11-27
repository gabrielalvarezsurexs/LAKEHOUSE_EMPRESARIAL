# Responsable: Gabriel Alvarez Mendoza (Arquitecto de Datos / Lider)
# Imagen del lakehouse: Python 3.11 + Java 17 + PySpark/Delta (del uv.lock) + JARs de S3A (D5, D18).

FROM ghcr.io/astral-sh/uv:0.11.16 AS uv

FROM python:3.11.18-slim-bookworm

# Java 17 para Spark 3.5; procps porque los scripts de Spark usan `ps`.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /bin/

# El venv vive fuera de /app para que montar ./src en desarrollo no lo tape.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Capa de dependencias: solo se reconstruye si cambia el lock.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project

COPY infra/app/fetch_jars.sh /tmp/fetch_jars.sh
RUN sh /tmp/fetch_jars.sh && rm /tmp/fetch_jars.sh

COPY src ./src
COPY tests ./tests
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen

# F5: Dagster (UI en el puerto 3000, daemon de schedules y sensores). Los CLIs siguen disponibles con
# `docker compose exec lakehouse python -m lakehouse...`.
ENV DAGSTER_HOME=/opt/dagster/home
RUN mkdir -p /opt/dagster/home
# Antes de Dagster: las corridas que murieron con el contenedor anterior se marcan para reintento (H-012).
# Con `;` un error de recover no impide arrancar; exec deja a Dagster recibir las senales de docker stop.
CMD ["/bin/sh", "-c", "python -m lakehouse.orchestration recover; exec dagster dev -m lakehouse.orchestration.definitions -h 0.0.0.0 -p 3000"]
