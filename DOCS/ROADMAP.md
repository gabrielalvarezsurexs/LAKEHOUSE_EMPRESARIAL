# Roadmap

**Última actualización:** 2026-11-22 (F0 cerrada, salvo el commit).
**Entrega:** lunes 30 de noviembre de 2026 (clase 4 pm; Blackboard 11:59 pm).

Gabriel y Claude implementan todo; Gabriel corre lo pesado (Docker, Spark, pipeline completo) y hace todos los
commits. Al cerrar la fase de cada rol se escribe su guía en `MDS_DE_ESTUDIO/` (plantilla en
[MDS_DE_ESTUDIO/README.md](../MDS_DE_ESTUDIO/README.md)).

## Retomar aquí

- 2026-11-22: F0 lista. Falta que Gabriel reescriba el historial (comandos en el chat del 2026-11-22). Lo
  siguiente es F1: compose con MinIO + imagen de Spark/Delta/Dagster y un smoke que escriba y lea una tabla Delta en
  MinIO.

## Tracker

| Fase | Qué | Responsable del rol | Fecha objetivo | Estado | Guía de estudio |
|---|---|---|---|---|---|
| F0 | Base: limpieza del repo, CLAUDE.md, docs, roadmap | Gabriel | dom 22 | Hecha (falta commit) | n/a |
| F1 | Infraestructura: compose, MinIO, imagen Spark+Delta, smoke | Gabriel | lun 23 | Pendiente | `01_GABRIEL_ARQUITECTO.md` (borrador) |
| F2 | Ingesta: landing, Bronce, MusicBrainz, sintético | Ricardo | lun 23 a mar 24 | Pendiente | `02_RICARDO_INGESTA.md` |
| F3 | Transformación: Plata y Oro | Juan Pablo | mar 24 a mié 25 | Pendiente | `03_JUAN_PABLO_TRANSFORMACION.md` |
| F4 | Gobernanza: contratos, calidad, catálogo, linaje, seguridad | Victor | mié 25 | Pendiente | `04_VICTOR_GOBERNANZA.md` |
| F5 | Orquestación, CI/CD y monitoreo | Nora | jue 26 | Pendiente | `07_NORA_DEVOPS.md` |
| F6 | Modelo de ML (skip) | Fernando | jue 26 a vie 27 | Pendiente | `06_FERNANDO_ML.md` |
| F7 | Tablero de BI | Emilio | vie 27 a sáb 28 | Pendiente | `05_EMILIO_BI.md` |
| F8 | Cierre: README, diagrama final, reportes, prueba de réplica | Gabriel | sáb 28 | Pendiente | `01_GABRIEL_ARQUITECTO.md` (final) |
| F9 | Guion, ensayo, video y entrega | Todos | dom 29 a lun 30 | Pendiente | `00_GUION_VIDEO.md` |

Holgura: las fechas dejan el domingo 29 libre de código. Si una fase se atrasa, se recorta alcance opcional
(marcado "opcional"), nunca un entregable.

---

## F0. Base del proyecto (dom 22)

- [x] Leer instrucciones y rúbrica; perfil de los datos (solo conteos).
- [x] Sacar del repo los archivos personales de configuración y el PDF a `DOCS/`.
- [x] `.gitignore`: datos reales, `handoff/`, runtime del stack.
- [x] `CLAUDE.md`, `DOCS/` (EQUIPO, ARQUITECTURA, DATOS_FUENTE, REFERENCIAS_TECNICAS, RUBRICA, HALLAZGOS, ROADMAP).
- [x] Plantilla de `MDS_DE_ESTUDIO/`.
- [ ] Gabriel: reescribir el historial y forzar push (repo limpio desde el primer commit).

## F1. Infraestructura (Gabriel, lun 23)

- [ ] `ENTREGABLES/docker-compose.yml`: `minio`, `minio-init`, `lakehouse` (Spark+Delta+Dagster); healthchecks.
- [ ] `infra/app/Dockerfile`: Python 3.11 + Java 17 + PySpark + delta-spark + JARs de S3A; versiones fijadas.
- [ ] `infra/minio/`: buckets `landing`, `bronze`, `silver`, `gold`.
- [ ] `.env.example` y carga de config en `src/lakehouse/common/config.py`; SparkSession en `common/spark.py`;
      logging y errores en `common/`.
- [ ] `pyproject.toml` (deps, ruff, pytest) y estructura de `src/lakehouse/` por rol con `Responsable:`.
- [ ] `.github/CODEOWNERS` (documental: carpeta por rol).
- [ ] Smoke: escribir y leer una tabla Delta en MinIO desde el contenedor (lo corre Gabriel).
- [ ] Diagrama v1 en `ENTREGABLES/diagramas/`; README raíz con el índice.
- **Listo cuando:** `docker compose up` levanta todo sano y el smoke pasa en una máquina limpia.

## F2. Ingesta (Ricardo, lun 23 a mar 24)

- [ ] Carga a landing: sube los JSON de `DATOS_SPOTIFY/` a `landing/spotify/<fecha>/` con hash SHA-256.
- [ ] Bronce: JSON a Delta con esquema explícito, metadatos de ingesta, `ip_addr` seudonimizada (D8).
- [ ] Idempotencia por hash con `_ingestion_log` (D15); registros malformados a `_quarantine` (D16).
- [ ] MusicBrainz: cliente con User-Agent genérico, 1 req/s, reintentos con espera, caché en landing (D9).
- [ ] Generador de datos sintéticos con el esquema exacto (D14).
- [ ] Pruebas: parser, hash, idempotencia (re-correr no duplica), cuarentena, generador.
- [ ] Gabriel corre la ingesta real (incluye la corrida única de MusicBrainz, ~25 min).
- [ ] `MDS_DE_ESTUDIO/02_RICARDO_INGESTA.md`.
- **Listo cuando:** filas en Bronce = filas de los JSON (15,743) + 0 en cuarentena, y re-correr no cambia conteos.

## F3. Transformación (Juan Pablo, mar 24 a mié 25)

- [ ] Plata: tipado, dedup (medir antes de fijar llave), música vs podcast, hora local, familia de plataforma,
      sesiones, `is_skip` (regla de DATOS_FUENTE §1.3, confirmada con conteos), artistas con género.
- [ ] Oro: dimensiones, `fact_plays` particionada, marts de KPIs; MERGE incremental; OPTIMIZE + Z-ORDER.
- [ ] Reconciliación de conteos Bronce > Plata > Oro con causa de cada descarte.
- [ ] Pruebas de cada transformación con DataFrames chicos.
- [ ] `MDS_DE_ESTUDIO/03_JUAN_PABLO_TRANSFORMACION.md`.
- **Listo cuando:** Oro completo y la reconciliación cuadra.

## F4. Gobernanza y seguridad (Victor, mié 25)

- [ ] Contratos de esquema por tabla (fuente única: YAML) con descripción de columnas, dueño y clasificación (PII).
- [ ] Checks de calidad por capa (no nulos, únicos, valores aceptados, rangos, frescura, reconciliación).
- [ ] Catálogo generado (`reportes/CATALOGO.md`) y linaje (tabla a tabla y columnas clave, Mermaid).
- [ ] `reportes/CALIDAD_Y_LINAJE.md` generado con los resultados de la última corrida.
- [ ] Usuarios y políticas de MinIO por servicio (D17); comentarios de columna en las tablas Delta.
- [ ] `MDS_DE_ESTUDIO/04_VICTOR_GOBERNANZA.md`.
- **Listo cuando:** todos los checks corren sobre las tres capas y el reporte se regenera solo.

## F5. Orquestación, CI/CD y monitoreo (Nora, jue 26)

- [ ] Dagster: un asset por tabla, asset checks de F4, job completo, schedule diario y sensor sobre landing.
- [ ] Monitoreo: asset de salud (tamaño por bucket, versiones Delta, frescura, conteos) y alertas en logs.
- [ ] GitHub Actions: ruff + pytest + pipeline completo con datos sintéticos contra MinIO como servicio.
- [ ] MLflow en el compose (lo usa F6).
- [ ] `MDS_DE_ESTUDIO/07_NORA_DEVOPS.md`.
- **Listo cuando:** soltar un JSON en landing dispara todo hasta Oro sin tocar nada, y CI pasa en verde.

## F6. Machine Learning (Fernando, jue 26 a vie 27)

- [ ] `gold.ml_skip_features` sin fuga de información (features solo con historia previa a cada reproducción).
- [ ] Entrenamiento con split temporal; baseline (tasa de skip histórica) vs modelo (p. ej. gradient boosting).
- [ ] Métricas: ROC-AUC, PR-AUC, F1, matriz de confusión, importancia de features; registro en MLflow.
- [ ] Inferencia por lote a `gold.ml_skip_predictions`; asset en Dagster.
- [ ] Notebook de presentación y `reportes/ML_METRICAS.md`.
- [ ] `MDS_DE_ESTUDIO/06_FERNANDO_ML.md`.
- **Listo cuando:** el modelo le gana al baseline en el periodo de prueba y la inferencia corre desde Dagster.

## F7. Tablero de BI (Emilio, vie 27 a sáb 28)

- [ ] Superset en el compose con `duckdb-engine`; vistas sobre Oro (`delta_scan` en MinIO).
- [ ] KPIs: horas escuchadas y tendencia, top artistas y géneros, retención de artistas, hábitos por hora/día,
      tasa de skip y predicciones del modelo; filtros de fecha, género y plataforma probados.
- [ ] Exportar el tablero al repo e importarlo al arrancar; capturas en `bi/capturas/`.
- [ ] `MDS_DE_ESTUDIO/05_EMILIO_BI.md`.
- **Listo cuando:** el tablero carga en segundos, ningún filtro se rompe y se levanta solo desde un clon limpio.

## F8. Cierre técnico (Gabriel, sáb 28)

- [ ] README raíz: qué es, diagrama, requisitos, cómo replicar paso a paso (sintéticos y reales), equipo.
- [ ] `ENTREGABLES/README.md`: índice entregable por entregable contra la rúbrica.
- [ ] Diagrama final; reportes regenerados.
- [ ] Prueba de réplica: clon limpio, `.env` desde el ejemplo, compose up, pipeline con sintéticos, tablero arriba.
- [ ] `MDS_DE_ESTUDIO/01_GABRIEL_ARQUITECTO.md` final; revisar las 7 guías.

## F9. Presentación y entrega (todos, dom 29 a lun 30)

- [ ] `MDS_DE_ESTUDIO/00_GUION_VIDEO.md`: 10 min, ~1:20 por integrante, en el orden del flujo de datos.
- [ ] Ensayo cronometrado; grabar el video (máx. 10 min).
- [ ] Link del video en `ENTREGABLES/README.md`; repo y video a Blackboard.
- [ ] Lunes 30, 4 pm: presentación en clase (demo en vivo desde la laptop de Gabriel, con el video de respaldo).

## Opcional (solo si sobra tiempo)

- Tabla de evolución de esquema demostrada en el video (agregar columna sin romper Plata).
- Linaje a nivel columna con OpenLineage.
- Segundo modelo (clustering de sesiones) para comparar.
