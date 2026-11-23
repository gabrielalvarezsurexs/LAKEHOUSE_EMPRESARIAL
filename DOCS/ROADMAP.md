# Roadmap

**Última actualización:** 2026-11-22 (F0 cerrada).
**Entrega:** lunes 30 de noviembre de 2026 (clase 4 pm; Blackboard 11:59 pm).

Gabriel y Claude implementan todo; Gabriel corre lo pesado y hace todos los commits. Lo que no se puede olvidar
(fechas, privacidad, uv, estructura, decisiones cerradas) está en [NO_OLVIDAR.md](NO_OLVIDAR.md).

## Retomar aquí

- 2026-11-22: F0 cerrada (historial del repo ya reescrito por Gabriel). **Siguiente: F1.** Arrancar leyendo
  `CLAUDE.md`, `DOCS/NO_OLVIDAR.md` y la sección F1 de abajo.

## Protocolo de cada fase

1. Claude implementa la fase y corre las pruebas chicas (`uv run ruff check .`, `uv run pytest`).
2. Claude le pasa a Gabriel los comandos pesados completos (Docker, Spark, pipeline); Gabriel los corre y Claude
   revisa los logs.
3. Claude escribe la guía de estudio del rol en `MDS_DE_ESTUDIO/` (plantilla en su README).
4. Claude actualiza este ROADMAP (casillas, tracker, "Retomar aquí", "Última actualización"), `RUBRICA.md`,
   `HALLAZGOS.md`, `NO_OLVIDAR.md` y el `README.md` raíz si cambió cómo se corre algo.
5. Claude pide el commit con un título de máximo 10 palabras; Gabriel lo hace.
6. **Gabriel compacta** la conversación y se arranca la siguiente fase.

## Tracker

| Fase | Qué | Rol dueño | Fecha objetivo | Estado | Guía de estudio |
|---|---|---|---|---|---|
| F0 | Base: limpieza del repo, CLAUDE.md, docs, roadmap | Gabriel | dom 22 | Hecha | n/a |
| F1 | Infraestructura: uv, compose, MinIO, imagen Spark+Delta, smoke | Gabriel | lun 23 | Pendiente | `01_GABRIEL_ARQUITECTO.md` (borrador) |
| F2 | Ingesta: landing, Bronce, MusicBrainz, sintéticos | Ricardo | lun 23 a mar 24 | Pendiente | `02_RICARDO_INGESTA.md` |
| F3 | Transformación: Plata y Oro | Juan Pablo | mar 24 a mié 25 | Pendiente | `03_JUAN_PABLO_TRANSFORMACION.md` |
| F4 | Gobernanza: contratos, calidad, catálogo, linaje, seguridad | Victor | mié 25 | Pendiente | `04_VICTOR_GOBERNANZA.md` |
| F5 | Orquestación, CI/CD, monitoreo y MLflow | Nora | jue 26 | Pendiente | `07_NORA_DEVOPS.md` |
| F6 | Modelo de ML (skip) | Fernando | jue 26 a vie 27 | Pendiente | `06_FERNANDO_ML.md` |
| F7 | Tablero de BI | Emilio | vie 27 a sáb 28 | Pendiente | `05_EMILIO_BI.md` |
| F8 | Cierre técnico: README, ENTREGABLES, diagrama final, prueba de réplica | Gabriel | sáb 28 | Pendiente | `01_GABRIEL_ARQUITECTO.md` (final) |
| F9 | Guion, ensayo, video y entrega | Todos | dom 29 a lun 30 | Pendiente | `00_GUION_VIDEO.md` |
| F10 | Explicación a fondo para Gabriel y paquete para el equipo | Gabriel + Claude | dom 29 | Pendiente | `PAQUETE_EQUIPO.md` |

Holgura: el domingo 29 queda libre de código. Si una fase se atrasa se recorta lo opcional, nunca un entregable.

---

## F0. Base del proyecto (dom 22) : hecha

- [x] Leer instrucciones y rúbrica; perfil de los datos (solo conteos).
- [x] Sacar del repo la configuración personal; el PDF a `DOCS/INSTRUCCIONES_PROYECTO.pdf`.
- [x] `.gitignore`: datos reales, `handoff/`, runtime del stack.
- [x] `CLAUDE.md`, `DOCS/` (NO_OLVIDAR, EQUIPO, ARQUITECTURA, DATOS_FUENTE, REFERENCIAS_TECNICAS, RUBRICA,
      HALLAZGOS, ROADMAP), plantilla de `MDS_DE_ESTUDIO/`, README raíz.
- [x] Historial reescrito y forzado por Gabriel.

## F1. Infraestructura (Gabriel, lun 23)

- [ ] uv: `pyproject.toml` (layout `src/`, grupos `ml` y `dev`, config de ruff y pytest), `.python-version` 3.11,
      `uv.lock`. Claude corre `uv sync` y `uv run pytest` (prueba de humo sin Spark).
- [ ] Esqueleto `src/lakehouse/` con un subpaquete por rol y encabezado `Responsable:`; `tests/` espejo.
- [ ] `src/lakehouse/common/`: `config.py` (lee `.env`), `spark.py` (SparkSession con Delta y S3A), `logging.py`,
      `errors.py`.
- [ ] `infra/app/Dockerfile`: Python 3.11 + Java 17 + JARs de S3A; instala con `uv sync --frozen`.
- [ ] `docker-compose.yml`: `minio` (imagen fijada de quay.io), `minio-init` (buckets `landing`, `bronze`,
      `silver`, `gold`), `lakehouse`; healthchecks y volúmenes.
- [ ] `.env.example` completo y comentado.
- [ ] `.github/CODEOWNERS` (carpeta por rol, documental).
- [ ] Smoke (lo corre Gabriel): desde el contenedor, escribir y leer una tabla Delta en MinIO.
- [ ] `ENTREGABLES/01_diagrama_arquitectura/` v1 y `ENTREGABLES/02_infraestructura/README.md`.
- [ ] README raíz: requisitos y cómo levantar la infraestructura.
- **Listo cuando:** `docker compose up` levanta todo sano y el smoke pasa.

## F2. Ingesta (Ricardo, lun 23 a mar 24)

- [ ] Carga a landing: sube los JSON de `DATOS_SPOTIFY/` a `landing/spotify/<fecha>/` con su SHA-256.
- [ ] Bronce: JSON a Delta con esquema explícito, metadatos de ingesta, `ip_addr` seudonimizada (D8).
- [ ] Idempotencia por hash con `_ingestion_log` (D15); registros malformados a `_quarantine` (D16).
- [ ] MusicBrainz: cliente con User-Agent genérico, 1 req/s, reintentos con espera, caché en landing (D9).
- [ ] Generador de datos sintéticos con el esquema exacto (D14); CLI con `uv run`.
- [ ] Pruebas: parser, hash, idempotencia (re-correr no duplica), cuarentena, generador, cliente MusicBrainz
      con respuestas grabadas (sin red).
- [ ] Gabriel corre la ingesta real (incluye la corrida única de MusicBrainz, ~25 min).
- [ ] `MDS_DE_ESTUDIO/02_RICARDO_INGESTA.md`.
- **Listo cuando:** filas en Bronce = 15,743 (filas de los JSON) con 0 en cuarentena, y re-correr no cambia conteos.

## F3. Transformación (Juan Pablo, mar 24 a mié 25)

- [ ] Plata: tipado, dedup (medir antes de fijar llave), música vs podcast, hora local, familia de plataforma,
      sesiones, `is_skip` (regla de DATOS_FUENTE §1.3 confirmada con conteos), artistas con género.
- [ ] Oro: dimensiones, `fact_plays` particionada, marts de KPIs; MERGE incremental; OPTIMIZE + Z-ORDER.
- [ ] Reconciliación de conteos Bronce > Plata > Oro con causa de cada descarte.
- [ ] Pruebas de cada transformación con DataFrames chicos (corren en el contenedor o en CI).
- [ ] `MDS_DE_ESTUDIO/03_JUAN_PABLO_TRANSFORMACION.md`.
- **Listo cuando:** Oro completo y la reconciliación cuadra.

## F4. Gobernanza y seguridad (Victor, mié 25)

- [ ] Contratos de esquema por tabla (fuente única: YAML) con descripción de columnas, dueño y clasificación (PII).
- [ ] Checks de calidad por capa (no nulos, únicos, valores aceptados, rangos, frescura, reconciliación).
- [ ] Generadores de `ENTREGABLES/03_calidad_y_linaje/`: `CATALOGO.md`, linaje (Mermaid, tabla a tabla y
      columnas clave) y `CALIDAD_Y_LINAJE.md` con los resultados de la última corrida (solo conteos).
- [ ] Usuarios y políticas de MinIO por servicio (D17); comentarios de columna en las tablas Delta.
- [ ] `MDS_DE_ESTUDIO/04_VICTOR_GOBERNANZA.md`.
- **Listo cuando:** todos los checks corren sobre las tres capas y el reporte se regenera solo.

## F5. Orquestación, CI/CD, monitoreo y MLflow (Nora, jue 26)

- [ ] `src/lakehouse/orchestration/`: un asset de Dagster por tabla, asset checks de F4, job completo, schedule
      diario y sensor sobre landing.
- [ ] Monitoreo: asset de salud (tamaño por bucket, versiones Delta, frescura, conteos) y alertas en logs.
- [ ] `.github/workflows/`: `uv sync`, ruff, pytest y pipeline completo con datos sintéticos contra MinIO como
      servicio.
- [ ] MLflow en el compose (lo usa F6).
- [ ] Captura del grafo de linaje de Dagster a `ENTREGABLES/03_calidad_y_linaje/`.
- [ ] `MDS_DE_ESTUDIO/07_NORA_DEVOPS.md`.
- **Listo cuando:** soltar un JSON en landing dispara todo hasta Oro sin tocar nada, y CI pasa en verde.

## F6. Machine Learning (Fernando, jue 26 a vie 27)

- [ ] `gold.ml_skip_features` sin fuga de información (features solo con historia previa a cada reproducción).
- [ ] Entrenamiento con split temporal; baseline (tasa de skip histórica) vs modelo (p. ej. gradient boosting).
- [ ] Métricas: ROC-AUC, PR-AUC, F1, matriz de confusión, importancia de features; registro en MLflow.
- [ ] Inferencia por lote a `gold.ml_skip_predictions`; asset en Dagster.
- [ ] `notebooks/` de presentación; `ENTREGABLES/04_modelo_ml/ML_METRICAS.md` y gráficas (generados).
- [ ] `MDS_DE_ESTUDIO/06_FERNANDO_ML.md`.
- **Listo cuando:** el modelo le gana al baseline en el periodo de prueba y la inferencia corre desde Dagster.

## F7. Tablero de BI (Emilio, vie 27 a sáb 28)

- [ ] `infra/superset/` (imagen con `duckdb-engine`) en el compose; `bi/` con las vistas sobre Oro
      (`delta_scan` en MinIO).
- [ ] KPIs: horas escuchadas y tendencia, top artistas y géneros, retención de artistas, hábitos por hora/día,
      tasa de skip y predicciones del modelo; filtros de fecha, género y plataforma probados.
- [ ] Export del tablero en `bi/` que se importa al arrancar.
- [ ] `ENTREGABLES/05_tablero_bi/`: capturas (aprobadas por el dueño de los datos), copia del export y cómo
      abrirlo.
- [ ] `MDS_DE_ESTUDIO/05_EMILIO_BI.md`.
- **Listo cuando:** el tablero carga en segundos, ningún filtro se rompe y se levanta solo desde un clon limpio.

## F8. Cierre técnico (Gabriel, sáb 28)

- [ ] README raíz final: qué es, diagrama, requisitos, cómo replicar paso a paso (sintéticos y reales), equipo.
- [ ] `ENTREGABLES/README.md`: índice entregable por entregable contra la rúbrica, con rutas al código.
- [ ] Diagrama final; reportes regenerados con la última corrida.
- [ ] Prueba de réplica: clon limpio, `.env` desde el ejemplo, `uv sync`, compose up, pipeline con sintéticos,
      tablero arriba.
- [ ] `MDS_DE_ESTUDIO/01_GABRIEL_ARQUITECTO.md` final; revisar las 7 guías contra el código final.

## F9. Presentación y entrega (todos, dom 29 a lun 30)

- [ ] `MDS_DE_ESTUDIO/00_GUION_VIDEO.md`: 10 min, ~1:20 por integrante, en el orden del flujo de datos.
- [ ] Ensayo cronometrado; grabar el video (máx. 10 min).
- [ ] Link del video en `ENTREGABLES/06_video/` y en `ENTREGABLES/README.md`; repo y video a Blackboard.
- [ ] Lunes 30, 4 pm: presentación (demo en vivo; checklist de demo en NO_OLVIDAR).

## F10. Explicación a fondo y paquete para el equipo (Gabriel + Claude, dom 29)

- [ ] Sesión donde Claude le explica a Gabriel todo el proyecto a fondo: flujo completo, cada decisión y su
      porqué, cómo se corre, qué puede fallar en la demo y cómo responder.
- [ ] `MDS_DE_ESTUDIO/PAQUETE_EQUIPO.md`: qué mandarle a cada integrante (su guía, su parte del guion, qué
      revisar y qué preguntas practicar) y en qué orden.

## Opcional (solo si sobra tiempo)

- Evolución de esquema demostrada en el video (agregar columna sin romper Plata).
- Linaje a nivel columna con OpenLineage.
- Segundo modelo (clustering de sesiones) para comparar.
