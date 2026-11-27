# Roadmap

**Última actualización:** 2026-11-24 (código de F7 a F10 listo; falta la sesión de verificación de Gabriel).
**Entrega:** lunes 30 de noviembre de 2026 (clase 4 pm; Blackboard 11:59 pm).

Gabriel y Claude implementan todo; Gabriel corre lo pesado y hace todos los commits. Lo que no se puede olvidar
(fechas, privacidad, uv, estructura, decisiones cerradas) está en [NO_OLVIDAR.md](NO_OLVIDAR.md).

## Retomar aquí

- 2026-11-23: F4 cerrada.
  - 325 checks sobre 17 tablas, 0 fallas.
  - 5 usuarios de MinIO con mínimo privilegio, verificados en vivo (41 de 41).
  - Catálogo, linaje y reporte generados en `ENTREGABLES/03_calidad_y_linaje/`.
- 2026-11-24 (noche): **código y docs de F7, F8, F9 y F10 listos de un jalón.** Sigue la **sesión de verificación
  de Gabriel**: un solo bloque de comandos, el que Claude le pasó en el chat. Ese bloque:
  1. agrega `SUPERSET_*` al `.env`;
  2. levanta el stack con Superset;
  3. corre `orchestration wait`, `status` y `bi/check.py` con los datos reales;
  4. hace la réplica desde un clon limpio (`infra/replica.ps1`);
  5. toma las capturas del tablero.

  Con lo que salga se ajustan los docs (tiempos reales del tablero, minutos de la réplica) y se marcan las casillas
  pendientes de abajo.
- 2026-11-26: **verificación en vivo con datos reales.**
  - Pipeline: `lakehouse_full` SUCCESS en 6.8 min, `Checks: SUCCEEDED=22`, salud sin alertas.
  - Tablero: 15 de 15 gráficas OK, todas en menos de 0.6 s.
  - Se arreglaron H-012 (corridas huérfanas tras reiniciar el contenedor), H-013 y H-014 (Superset).
  - Nuevo `infra/verificar.ps1`: cada paso deja su log en `logs/verificacion/` y Claude lo lee de ahí.
  - Falta: commit y push (CI), la réplica (`pwsh -File infra/verificar.ps1 -Steps replica`) y las capturas.
  - **F7 (BI):**
    - `bi/sql/` (6 datasets que solo leen `s3://gold`) y `bi/dashboard.py` (15 gráficas, 3 filtros y el acomodo).
      Este último genera el export de Superset que el contenedor importa en cada arranque (D26).
    - `bi/check.py`: prueba en vivo por la API, con el tiempo de cada gráfica.
    - `infra/superset/`: Superset 6.1.0 por digest, DuckDB 1.5.6 con `delta` y `httpfs`, y el secreto de `svc-bi`
      solo en la conexión.
    - Servicio `superset` en el compose (8088) y paso de Superset en CI.
    - Pruebas locales: el SQL de cada dataset y gráfica corre en DuckDB contra el esquema de los contratos, y el
      export de `ENTREGABLES/05_tablero_bi/` está al día.
  - **F8:**
    - README final: opción de un comando y paso a paso.
    - `infra/replica.ps1`: réplica de punta a punta, sin pasos manuales.
    - `orchestration wait`: espera a que los sensores terminen el pipeline.
    - Diagrama final con su PNG (mermaid-cli 12.2.0; una prueba lo mantiene igual al Mermaid).
    - `ENTREGABLES/README.md` contra la rúbrica.
    - Guía de Gabriel final; guías 06 y 07 revisadas contra el código.
  - **F9:** `00_GUION_VIDEO.md` (orden, tiempos, pantallas y checklist para grabar).
  - **F10:** `PAQUETE_EQUIPO.md`. Falta la sesión de explicación a fondo (Claude con Gabriel).
  - **Local:** ruff limpio; 275 en verde, 17 saltadas (Spark). `uv.lock` agrega DuckDB al grupo `dev`.
- 2026-11-24: F6 cerrada. CI con el commit de F6 sin confirmar todavía (lo cubre el push de F7 a F10).
  - **Código:** `src/lakehouse/ml/` (`features`, `model`, `registry`, `report`, `__main__` con `features`,
    `train`, `predict` y `all`); contratos `gold.ml_skip_features` y `gold.ml_skip_predictions`; pasos de Dagster
    `ml_features`, `ml_train` y `ml_predict` (grupo `ml`, usuario `svc-ml`); notebook `notebooks/ml_skip.ipynb`.
  - **Resultado real:** 15,628 filas (14,174 con etiqueta). En prueba, ROC-AUC 0.746 contra 0.619 del baseline y
    PR-AUC 0.511 contra 0.289. Le gana: `lakehouse_skip` v6 con alias `champion`.
  - **Corrida de Gabriel:** `pipeline_from_landing` SUCCESS en 6.2 min; `status`: `Checks: SUCCEEDED=22`.
  - **H-011:** al levantar el stack, el daemon corrió el `daily_pipeline` perdido a la vez que un `run` manual y
    se acabó la memoria. Ahora `run` se niega si hay otra corrida activa (`--force`), y el contenedor usa
    `init: true` (quedaban `java <defunct>`).
  - **Local:** ruff limpio, 242 en verde. Nuevo grupo de uv `notebook` (ipykernel, solo local).
  - **Docs al día:** guía `06_FERNANDO_ML.md`, D25, HALLAZGOS, RUBRICA, NO_OLVIDAR, README (paso 6), CLAUDE.md,
    `ENTREGABLES/`.
- 2026-11-23: F5 cerrada y CI en verde.
  - **Código:**
    - `src/lakehouse/orchestration/`: `steps`, `assets`, `checks`, `automation`, `health`, `definitions` y
      `__main__` (`run`, `health`, `status`).
    - Cada paso es un subproceso de su CLI con su usuario `svc-*`.
    - Checks bloqueantes por paso, no por capa: géneros escribe en Bronce después de los checks de Bronce y por
      capa salía un ciclo.
    - Sensores de carpeta y de landing, schedule diario a las 6:00, salud cada 6 h y alerta de fallas en logs.
  - **Cambios fuera de orquestación:**
    - `governance check` acepta `--tables`, `--cross-layer` y `--json`;
    - `GENRES_SOURCE` en `config`;
    - `S3ObjectStore.usage`.
  - **Infra:**
    - `infra/dagster/dagster.yaml`: 1 corrida a la vez, monitoreo de corridas y sin telemetría;
    - Dockerfile con CMD `dagster dev`;
    - compose: Dagster en **localhost:3300** (el 3000 lo ocupa `ammia-grafana` de Gabriel; `DAGSTER_PORT`), MLflow
      3.18 por digest en el 5000;
    - `.github/workflows/ci.yml`.
  - **Local:** ruff limpio, 222 en verde.
  - **Corrida pesada de Gabriel (2026-11-23), hecha:**
    - `pytest` en el contenedor: 234 en verde, 3 saltadas (archivos que el contenedor no monta). MLflow `/health`: OK.
    - `pipeline_from_landing` SUCCESS en 5.6 min, disparado solo por los sensores; 19 de 20 checks en verde.
    - Prueba del sensor OK: el `touch` dio un `ingest_landing` más y ningún `pipeline_from_landing` nuevo.
    - `linaje_dagster.png` guardado: SVG exportado de Dagster, con flechas aclaradas, fondo oscuro y recorte; el
      script quedó en el scratchpad, no en el repo.
    - Se apagaron los logs INFO de `alembic` en `common/log.py` (ensuciaban `orchestration status`).
    - `health`: 17 de 17 tablas, 71,923 filas; landing 1303 objetos (15.9 MiB), bronze 303 (7.6), silver 80 (7.5),
      gold 242 (11.1).
  - **Archivos chicos en Bronce (H-009), resuelto:**
    - `ingestion bronze` compacta las 3 tablas (OPTIMIZE) cuando hubo archivos nuevos; `--compact` las compacta
      aunque no haya.
    - Corrida de Gabriel: prueba de Bronce 2/2. `--compact` dejó 1 archivo por tabla. `health`: `Salud OK: sin
      alertas`. `run --job health_only`: SUCCESS. `status`: `Checks: SUCCEEDED=20`.
  - **Docs al día:**
    - guía `MDS_DE_ESTUDIO/07_NORA_DEVOPS.md`;
    - ARQUITECTURA (D24 y servicios) y HALLAZGOS (H-007, H-008, H-009);
    - RUBRICA, NO_OLVIDAR, REFERENCIAS_TECNICAS, README (paso 5), CLAUDE.md y `ENTREGABLES/`.
  - **CI:** los dos primeros runs fallaron porque `setup-uv@v10` no existe (H-010). Con `setup-uv@v10.4.0`,
    el run #3 (commit `0ea42c0`) pasó en verde los dos jobs en 12 min 13 s.

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
| F1 | Infraestructura: uv, compose, MinIO, imagen Spark+Delta, smoke | Gabriel | lun 23 | Hecha (dom 22) | `01_GABRIEL_ARQUITECTO.md` (borrador) |
| F2 | Ingesta: landing, Bronce, MusicBrainz, sintéticos | Ricardo | lun 23 a mar 24 | Hecha (lun 23) | `02_RICARDO_INGESTA.md` |
| F3 | Transformación: Plata y Oro | Juan Pablo | mar 24 a mié 25 | Hecha (lun 23) | `03_JUAN_PABLO_TRANSFORMACION.md` |
| F4 | Gobernanza: contratos, calidad, catálogo, linaje, seguridad | Victor | mié 25 | Hecha (lun 23) | `04_VICTOR_GOBERNANZA.md` |
| F5 | Orquestación, CI/CD, monitoreo y MLflow | Nora | jue 26 | Hecha (lun 23, CI en verde) | `07_NORA_DEVOPS.md` |
| F6 | Modelo de ML (skip) | Fernando | jue 26 a vie 27 | Hecha (mar 24) | `06_FERNANDO_ML.md` |
| F7 | Tablero de BI | Emilio | vie 27 a sáb 28 | Código listo (mar 24); falta corrida real y capturas | `05_EMILIO_BI.md` |
| F8 | Cierre técnico: README, ENTREGABLES, diagrama final, prueba de réplica | Gabriel | sáb 28 | Código listo (mar 24); falta correr la réplica | `01_GABRIEL_ARQUITECTO.md` (final) |
| F9 | Guion, ensayo, video y entrega | Todos | dom 29 a lun 30 | Guion listo; falta ensayo y video | `00_GUION_VIDEO.md` |
| F10 | Explicación a fondo para Gabriel y paquete para el equipo | Gabriel + Claude | dom 29 | Paquete listo; falta la sesión | `PAQUETE_EQUIPO.md` |

Holgura: el domingo 29 queda libre de código. Si una fase se atrasa se recorta lo opcional, nunca un entregable.

---

## F0. Base del proyecto (dom 22) : hecha

- [x] Leer instrucciones y rúbrica; perfil de los datos (solo conteos).
- [x] Sacar del repo la configuración personal; el PDF a `DOCS/INSTRUCCIONES_PROYECTO.pdf`.
- [x] `.gitignore`: datos reales, `handoff/`, runtime del stack.
- [x] `CLAUDE.md`, `DOCS/` (NO_OLVIDAR, EQUIPO, ARQUITECTURA, DATOS_FUENTE, REFERENCIAS_TECNICAS, RUBRICA,
      HALLAZGOS, ROADMAP), plantilla de `MDS_DE_ESTUDIO/`, README raíz.
- [x] Historial reescrito y forzado por Gabriel.

## F1. Infraestructura (Gabriel, lun 23) : hecha

- [x] uv: `pyproject.toml` (layout `src/`, grupo `dev`; Dagster en F5; ML en F6 como dependencias normales y el grupo `notebook` solo local), `.python-version`
      3.11, `uv.lock` (PySpark 3.5.10, delta-spark 3.3.3).
- [x] Esqueleto `src/lakehouse/` con un subpaquete por rol y encabezado `Responsable:`; `tests/` espejo.
- [x] `src/lakehouse/common/`: `config.py`, `spark.py`, `log.py` (no `logging.py` para no tapar el estándar),
      `errors.py`, `smoke.py`.
- [x] `infra/app/Dockerfile` + `fetch_jars.sh` (JARs derivados del lock, SHA-1 verificado).
- [x] `docker-compose.yml`: `minio` (Chainguard por digest, H-002), `minio-init` (`infra/minio/init.sh`),
      `lakehouse` (monta `src/` y `tests/`).
- [x] `.env.example` comentado (F2 agrega la sal de IP).
- [x] `.github/CODEOWNERS` (carpeta por rol en comentarios).
- [x] Smoke (lo corrió Gabriel): buckets OK, Delta 150/100/2 versiones; `pytest` 18 en verde en el contenedor.
- [x] `ENTREGABLES/01_diagrama_arquitectura/DIAGRAMA.md` v1 y `ENTREGABLES/02_infraestructura/README.md`.
- [x] README raíz: requisitos y cómo levantar la infraestructura.
- [x] `MDS_DE_ESTUDIO/01_GABRIEL_ARQUITECTO.md` (borrador).
- **Listo cuando:** `docker compose up` levanta todo sano y el smoke pasa.

## F2. Ingesta (Ricardo, lun 23 a mar 24)

- [x] Carga a landing: sube los JSON de `DATOS_SPOTIFY/` a `landing/spotify/<fecha>/` con su SHA-256.
- [x] Bronce: JSON a Delta con esquema explícito, metadatos de ingesta, `ip_addr` seudonimizada (D8).
- [x] Idempotencia por hash con `_ingestion_log` (D15, D20); registros malformados a `_quarantine` (D16).
- [x] MusicBrainz: cliente con User-Agent genérico, 1 req/s, reintentos con espera, caché en landing (D9).
- [x] Generador de datos sintéticos con el esquema exacto (D14); CLI `python -m lakehouse.ingestion synthetic`.
- [x] Contratos de Bronce en `governance/contracts.py` (F4 los extiende a Plata y Oro).
- [x] Pruebas: parser, hash, idempotencia (re-correr no duplica), cuarentena, generador, cliente MusicBrainz
      con respuestas grabadas (sin red). Local: 63 en verde; validación local de los reales: 15,743 / 0.
- [x] Gabriel corrió la ingesta real (2026-11-23): `pytest` 66 en verde en el contenedor; Bronce 15,743 filas,
      0 en cuarentena, bitácora 2; segunda corrida `ya ingeridos=2` con los mismos totales. MusicBrainz: 1,301
      artistas en Bronce, 1,099 con match (84%); los 5 que fallaron por 503 entraron al re-correr `genres`. ~55 min
      por los 503 frecuentes de la API. Logs de Spark sin avisos benignos (`infra/app/spark-conf/`).
- [x] `MDS_DE_ESTUDIO/02_RICARDO_INGESTA.md`.
- **Listo cuando:** filas en Bronce = 15,743 (filas de los JSON) con 0 en cuarentena, y re-correr no cambia conteos.

## F3. Transformación (Juan Pablo, mar 24 a mié 25)

- [x] Plata: tipado, dedup (medido: 27), música vs podcast, hora local (con relleno por sesión para `ZZ`),
      familia de plataforma, sesiones, `is_skip` (regla confirmada con conteos, DATOS_FUENTE §1.3), artistas con
      género, `offline_timestamp` normalizado (H-003). Plata se recalcula completa (D21).
- [x] Oro: 4 dimensiones, `fact_plays` particionada por año con MERGE incremental y OPTIMIZE + Z-ORDER por
      `date_key`, 4 marts de KPIs (diario, top artistas, géneros, retención por cohorte).
- [x] Contratos de Plata y Oro en `governance/contracts.py`; tablas creadas desde el contrato (D22).
- [x] Reconciliación Bronce > Plata > Oro con causa de cada descarte (`transformation/reconcile.py`).
- [x] Pruebas: reglas con filas hechas a mano, paridad Spark/Python, pipeline sintético completo, MERGE
      (0 cambios al re-correr; borra y actualiza solo lo que cambió), NOT NULL. Local: 126 en verde.
- [x] Gabriel corrió con los reales (2026-11-23): `pytest` 137 en verde en el contenedor; Plata 15,628 + 50 +
      65 descartes (27 duplicadas, 38 sin contenido); 1,619 sesiones; 10 sin hora local; 14,174 etiquetadas y
      2,557 skips (18%); géneros en 678 de 1,301 artistas (H-004). Oro: `fact_plays` 15,628, `dim_date` 3,692,
      `dim_track` 3,628. Reconciliación 4/4 OK; segunda corrida: MERGE 0/0/0 y sin OPTIMIZE.
- [x] `MDS_DE_ESTUDIO/03_JUAN_PABLO_TRANSFORMACION.md`.
- **Listo cuando:** Oro completo y la reconciliación cuadra.

## F4. Gobernanza y seguridad (Victor, mié 25)

- [x] Contratos de las 18 tablas (`governance/contracts.py`):
  - columnas con descripción y privacidad (`personal`, `pseudonym`);
  - dueño, clasificación (restringido, confidencial, interno) y `sources` (linaje);
  - llaves únicas, referencias, valores aceptados, rangos y reglas SQL (D23).
- [x] Checks de calidad por capa (`governance/quality.py`):
  - **Por tabla:** existe, esquema, privacidad, metadatos.
  - **Por fila:** no nulos, valores, rangos, reglas, únicos, referencias.
  - **Entre capas:** 7 reconciliaciones y frescura.
- [x] Generadores de `ENTREGABLES/03_calidad_y_linaje/`:
  - `CATALOGO.md` y `LINAJE.md`, locales y sin Spark (`governance docs`);
  - `CALIDAD_Y_LINAJE.md` con la última corrida (`governance check`, solo conteos).
- [x] Usuarios de MinIO por servicio (D17):
  - `svc-ingestion`, `svc-transformation`, `svc-governance`, `svc-ml` y `svc-bi`;
  - políticas generadas desde `governance/access.py` y aplicadas por `init.sh`;
  - el root salió del contenedor del pipeline;
  - prueba en vivo de accesos (`governance access`).
- [x] Comentarios de columna y propiedades (dueño, capa, clasificación) en todas las tablas Delta: se crean o se
      sincronizan desde el contrato al escribir (`common/delta.py`).
- [x] Pruebas:
  - **Locales:** contratos, políticas contra la matriz, prueba de accesos con MinIO falso, docs al día y reporte.
  - **Con Spark:** pipeline sintético sin fallas, errores sembrados detectados y sincronización de metadatos.
  - **Resultado local:** 188 en verde.
- [x] Gabriel corrió con los reales (2026-11-23):
  - `pytest`: 201 en verde en el contenedor;
  - minio-init creó los 5 usuarios y el smoke pasó con ellos;
  - `governance check`: 325 checks en 17 tablas, 0 fallas y 0 avisos; accesos 41 de 41.
- [x] `MDS_DE_ESTUDIO/04_VICTOR_GOBERNANZA.md`.
- **Listo cuando:** todos los checks corren sobre las tres capas y el reporte se regenera solo.

## F5. Orquestación, CI/CD, monitoreo y MLflow (Nora, jue 26)

- [x] `src/lakehouse/orchestration/`: un asset de Dagster por tabla, asset checks de F4, job completo, schedule
      diario y sensor sobre landing.
- [x] Monitoreo: asset de salud (tamaño por bucket, versiones Delta, frescura, conteos) y alertas en logs.
- [x] `.github/workflows/`: `uv sync`, ruff, pytest y pipeline completo con datos sintéticos contra MinIO como
      servicio. En verde en GitHub (run #3, H-010).
- [x] MLflow en el compose (lo usa F6).
- [x] Captura del grafo de linaje de Dagster a `ENTREGABLES/03_calidad_y_linaje/`.
- [x] `MDS_DE_ESTUDIO/07_NORA_DEVOPS.md`.
- **Listo cuando:** soltar un JSON en landing dispara todo hasta Oro sin tocar nada, y CI pasa en verde.

## F6. Machine Learning (Fernando, jue 26 a vie 27) : hecha (mar 24)

- [x] `gold.ml_skip_features` sin fuga de información (features solo con historia previa a cada reproducción).
- [x] Entrenamiento con split temporal; baseline (tasa de skip histórica) vs modelo (gradient boosting).
- [x] Métricas: ROC-AUC, PR-AUC, F1, matriz de confusión, importancia de features; registro en MLflow.
- [x] Inferencia por lote a `gold.ml_skip_predictions`; asset en Dagster.
- [x] `notebooks/ml_skip.ipynb`; `ENTREGABLES/04_modelo_ml/ML_METRICAS.md` y gráficas (generados).
- [x] `MDS_DE_ESTUDIO/06_FERNANDO_ML.md`.
- **Listo cuando:** el modelo le gana al baseline en el periodo de prueba y la inferencia corre desde Dagster.

## F7. Tablero de BI (Emilio, vie 27 a sáb 28)

- [x] `infra/superset/` (imagen con `duckdb-engine`) en el compose; `bi/` con las vistas sobre Oro
      (`delta_scan` en MinIO). Son datasets virtuales, no vistas de DuckDB (D26).
- [x] KPIs: horas escuchadas y tendencia, top artistas y géneros, retención de artistas, hábitos por hora/día,
      tasa de skip y predicciones del modelo; filtros de fecha, género y plataforma (columnas probadas en local).
- [x] Export del tablero que se importa al arrancar: lo genera `bi/dashboard.py`.
- [x] Corrida real: `bi/check.py` con `15 de 15 graficas OK`, todas en menos de 0.6 s (2026-11-26).
- [ ] CI en verde con el tablero.
- [ ] `ENTREGABLES/05_tablero_bi/`: capturas aprobadas por el dueño de los datos. Ya están la copia del export y cómo
      abrirlo.
- [x] `MDS_DE_ESTUDIO/05_EMILIO_BI.md`.
- **Listo cuando:** el tablero carga en segundos, ningún filtro se rompe y se levanta solo desde un clon limpio.

## F8. Cierre técnico (Gabriel, sáb 28)

- [x] README raíz final: qué es, diagrama, requisitos, cómo replicar paso a paso (sintéticos y reales), equipo.
- [x] `ENTREGABLES/README.md`: índice entregable por entregable contra la rúbrica, con rutas al código.
- [x] Diagrama final (Mermaid + `arquitectura.png`). Los reportes se regeneran con la corrida final de Gabriel.
- [ ] Prueba de réplica: clon limpio, `.env` desde el ejemplo, `uv sync`, compose up, pipeline con sintéticos,
      tablero arriba. El script `infra/replica.ps1` ya está; falta que Gabriel lo corra.
- [x] `MDS_DE_ESTUDIO/01_GABRIEL_ARQUITECTO.md` final; revisar las 7 guías contra el código final.

## F9. Presentación y entrega (todos, dom 29 a lun 30)

- [x] `MDS_DE_ESTUDIO/00_GUION_VIDEO.md`: 10 min, ~1:20 por integrante, en el orden del flujo de datos.
- [ ] Ensayo cronometrado; grabar el video (máx. 10 min).
- [ ] Link del video en `ENTREGABLES/06_video/` y en `ENTREGABLES/README.md`; repo y video a Blackboard.
- [ ] Lunes 30, 4 pm: presentación (demo en vivo; checklist de demo en NO_OLVIDAR).

## F10. Explicación a fondo y paquete para el equipo (Gabriel + Claude, dom 29)

- [ ] Sesión donde Claude le explica a Gabriel todo el proyecto a fondo: flujo completo, cada decisión y su
      porqué, cómo se corre, qué puede fallar en la demo y cómo responder.
- [x] `MDS_DE_ESTUDIO/PAQUETE_EQUIPO.md`: qué mandarle a cada integrante (su guía, su parte del guion, qué
      revisar y qué preguntas practicar) y en qué orden.

## Opcional (solo si sobra tiempo)

- Evolución de esquema demostrada en el video (agregar columna sin romper Plata).
- Linaje a nivel columna con OpenLineage.
- Segundo modelo (clustering de sesiones) para comparar.
