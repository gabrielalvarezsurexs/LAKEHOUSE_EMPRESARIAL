# No olvidar

Lo que **no se puede perder** entre sesiones ni después de compactar. Se lee al arrancar cada sesión, junto con
la sección "Retomar aquí" de [ROADMAP.md](ROADMAP.md). Si algo de aquí cambia, se actualiza el mismo día.

## Fechas y entrega

- **Lunes 30 de noviembre de 2026:** presentación en clase a las **4 pm** (demo en vivo desde la laptop de
  Gabriel) y cierre en Blackboard a las **11:59 pm** (link al repo y link al video).
- Video técnico de **máximo 10 minutos**: **los 7 integrantes** explican su contribución exacta (~1:20 cada uno).
- El domingo 29 queda libre de código: solo guion, ensayo y video.

## Forma de trabajo

- **Gabriel y Claude implementan todo.** Los otros 6 integrantes no programan: estudian su parte con su guía de
  `MDS_DE_ESTUDIO/` y la presentan.
- **Al cerrar cada fase del roadmap**, en este orden: (1) pruebas chicas en verde y Gabriel corre la pesada,
  (2) guía de estudio del rol de esa fase, (3) actualizar ROADMAP (tracker, casillas, "Retomar aquí",
  "Última actualización"), RUBRICA y este archivo si aplica, (4) pedir commit con título de máximo 10 palabras,
  (5) **Gabriel compacta** y la siguiente fase arranca leyendo CLAUDE.md, este archivo y "Retomar aquí".
- **Al final de todas las fases (F10):** sesión donde Claude le explica a Gabriel todo el proyecto a fondo y arma
  lo que hay que pasarle al equipo (guías, guion, qué decir cada quien, qué contestar).
- **Git:** Claude solo lee. Todos los commits los hace Gabriel; Claude le da el comando completo o le dice "haz
  commit" con el título.
- **Quién corre qué:** Claude corre solo pruebas chicas (segundos, sin Docker). Gabriel corre Docker, Spark, el
  pipeline completo, la corrida de MusicBrainz y CI. Todo comando para Gabriel va completo y listo para pegar.
- Sin subagentes. Respuestas cortas.

## Estructura del repo

- **El código vive en la raíz** (`src/lakehouse/`, `infra/`, `bi/`, `notebooks/`, `tests/`, `docker-compose.yml`,
  `pyproject.toml`).
- **`ENTREGABLES/` guarda solo lo que se entrega y no es código:** diagrama, reporte de calidad y linaje, catálogo,
  métricas de ML, capturas y export del tablero, link del video. Su `README.md` es el índice contra la rúbrica y
  apunta al código de la raíz.
- `DOCS/` = docs de trabajo. `MDS_DE_ESTUDIO/` = guías por integrante. `.github/` en la raíz (CI y CODEOWNERS).
- Cada archivo de código abre con `Responsable: <nombre> (<rol>)`; el mapa de carpetas por rol está en
  [EQUIPO.md](EQUIPO.md).

## Dependencias

- **Todo lo que corre directo en el repo usa uv:** `uv sync`, `uv run ...`, `uv add ...`. Nada de `pip install` ni
  venvs a mano. `pyproject.toml` y `uv.lock` se versionan.
- Python del proyecto: **3.11** (`.python-version`; uv lo instala solo). El 3.14 de la máquina no sirve con
  PySpark 3.5.
- La imagen Docker del lakehouse instala **con uv desde el mismo `uv.lock`** (`uv sync --frozen`): local y
  contenedor usan exactamente las mismas versiones.
- Spark corre en el contenedor (Java 17 + JARs de S3A). Localmente solo corren las pruebas que no necesitan Spark
  ni MinIO.

## Datos y privacidad (el repo es público)

- `DATOS_SPOTIFY/` (datos reales) **nunca** entra a git; tampoco Parquet, Delta ni carpetas `data/`.
- Es ~99% seguro que esos JSON son los datos finales. Si cambian: reemplazar los archivos, re-correr (la ingesta es
  idempotente por hash) y actualizar el perfil de [DATOS_FUENTE.md](DATOS_FUENTE.md).
- Salidas, logs y reportes versionados: **solo conteos y métricas agregadas**, nunca renglones, IPs ni
  canciones/artistas concretos.
- **Las capturas del tablero sí muestran artistas y canciones reales:** el dueño de los datos las aprueba antes de
  subirlas a `ENTREGABLES/`.
- `ip_addr`: hash con sal en Bronce, se elimina en Plata (un check de privacidad lo verifica en cada tabla). La
  sal y las llaves viven en `.env` (nunca se imprime).
- `CALIDAD_Y_LINAJE.md` se versiona porque solo trae conteos. Antes del commit, revisar que siga así.
  **La sal no se cambia nunca** (`IP_HASH_SALT`): si cambia, los hashes ya no coinciden entre corridas.
- **Nunca mezclar sintéticos y reales en el mismo MinIO.** La carga a landing lo impide; para cambiar de datos:
  `docker compose down -v` (borra el volumen) y volver a correr.
- MusicBrainz: User-Agent genérico sin datos personales; 1 petición por segundo; corrida única cacheada.

## Decisiones técnicas que no se reabren (detalle en ARQUITECTURA.md)

- MinIO con imagen de **Chainguard fijada por digest** (Docker Hub y quay.io ya no la dan gratis, H-002); un
  bucket por capa.
- El contenedor `lakehouse` monta `./src` y `./tests`: cambiar código **no** requiere reconstruir; solo cambiar
  `uv.lock` o el Dockerfile (`docker compose up -d --build`).
- Delta Lake + **Spark** (lo vimos en clase) en contenedor; Dagster orquesta; Superset + DuckDB para BI; MLflow
  para el modelo.
- Géneros por MusicBrainz; la API de Spotify queda descartada (plan B documentado, no se implementa).
- ML = predicción de skip con split temporal; la etiqueta queda `null` cuando la reproducción no terminó por
  decisión del usuario.
- Plata se recalcula completa; `fact_plays` se actualiza con MERGE y re-correr escribe 0 filas (D21). Las tablas
  de Plata y Oro nacen de su contrato con NOT NULL (D22). `transformation all` falla si la reconciliación no cuadra.
- Antes de usar una API de una librería, revisar que exista en la versión instalada (`.venv`), no de memoria
  (H-005).
- **El contrato manda (D23):** una regla de calidad, un dueño o una clasificación se cambian en
  `governance/contracts.py`. Luego se corre `uv run python -m lakehouse.governance docs`; si no, la prueba de docs
  falla. Los accesos de MinIO se cambian en `governance/access.py` y se regeneran igual (las políticas JSON).
- **Usuarios de MinIO (D17):** cada comando usa `get_settings().for_role("<rol>")`; el root nunca entra al
  contenedor del pipeline. El `.env` necesita las 5 `MINIO_SECRET_*` (README, paso 1); tras cambiarlas,
  `docker compose up -d --force-recreate minio-init lakehouse`.
- El código va en ASCII: los acentos de los `.md` generados van como escapes Unicode (barra, u y 4 dígitos hex) dentro de los textos.
  La herramienta de escritura convierte `\u` en el carácter, así que hay que revisar con
  `rg "[^\x00-\x7F]" src`.
- **ML y BI leen solo de Oro.** Los módulos de cada rol se comunican por tablas, no por imports.
- **Dagster (D24):**
  - **Dónde:** <http://localhost:3300>. No va en el 3000, que en la laptop de Gabriel ocupa otra app (H-007).
  - **Corre solo:** la primera vez que se levanta el stack, los sensores corren el pipeline completo, y el
    schedule lo repite diario a las 6:00. Cada corrida reescribe `CALIDAD_Y_LINAJE.md`, así que antes de cada
    commit hay que revisar su diff: solo debe traer conteos.
  - **Datos reales:** el sensor de landing no se demuestra subiendo un sintético, porque mezclaría datos.
- "Sin ejecuciones manuales" (rúbrica): schedule + sensor en Dagster. "README para replicar" (rúbrica): datos
  sintéticos + prueba desde un clon limpio en F8.

## Demo del 30

- Antes de clase: Docker prendido, stack arriba, pipeline ya corrido con los datos reales, tablero cargado y el
  video listo como respaldo.
