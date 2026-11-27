# Nora Valentina Frías Alpízar: Ingeniera DevOps / MLOps

## Tu parte en 30 segundos

Tú haces que **el pipeline corra solo y que se note cuando algo sale mal**:

- **Dagster orquesta:** landing > bronce > plata > oro, con los checks de gobernanza después de cada paso, el
  reporte de calidad y la salud del lakehouse.
- **CI (GitHub Actions) prueba cada cambio:** lint, pruebas, el pipeline completo con datos sintéticos y el
  tablero de Superset con cada gráfica.
- **MLflow** es el servidor donde ML registra cada entrenamiento y su modelo.

No transformas datos: envuelves los comandos de cada rol, los encadenas y los vigilas.

## Lo que tienes que saber (máx. 5 conceptos)

- **Assets y linaje:** en Dagster cada tabla Delta es un *asset*. El grafo no se dibuja a mano: sale de los
  `sources` de los contratos de gobernanza (D23). Por eso el linaje de Dagster coincide con `LINAJE.md`.
- **Cada paso es un proceso aparte con su usuario de MinIO (D17):**
  - Dagster corre `python -m lakehouse.ingestion bronze`, `... transformation silver`, etc.
  - Cada paso usa el usuario de su rol (`svc-ingestion`, `svc-transformation`).
  - Así se respeta el mínimo privilegio, y cada rol sigue siendo dueño de su código (los roles no se importan
    entre sí).
- **Asset checks que bloquean:**
  - Después de cada paso corren los checks de gobernanza de sus tablas.
  - Si falla uno de tipo "error", lo que sigue no corre (Plata no se calcula sobre un Bronce malo).
  - Son **22 checks**: uno por tabla (incluidas las 2 de ML desde F6), más reconciliación, frescura y
    `sin_alertas`.
  - Van por paso y no por capa: géneros escribe en Bronce después de que Bronce ya pasó sus checks, y por capa se
    formaba un ciclo (H-008).
- **Automatización sin clics:**
  - **Sensores:**
    - un JSON nuevo en la carpeta de origen dispara la carga a landing;
    - un archivo nuevo en landing dispara de Bronce a Oro.
  - **Schedules:**
    - el pipeline completo todos los días a las 6:00 (CDMX);
    - la salud cada 6 horas.
  - **`alert_on_failure`:** deja una alerta en los logs si una corrida falla.
  - Todo es idempotente: si nada cambió, no se escribe nada.
- **Monitoreo (`health`):**
  - **Qué mide:** tamaño de cada bucket y, por tabla, archivos, versiones y filas.
  - **Alertas:**
    - "error" si falta una tabla;
    - "aviso" si hay muchos archivos chicos (conviene OPTIMIZE) o muchas versiones (conviene VACUUM).
  - **Ejemplo real:** el aviso de 48 archivos chicos en Bronce llevó a compactar Bronce al final de la ingesta
    (H-009).

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| [steps.py](../src/lakehouse/orchestration/steps.py) | Los 5 pasos: comando, usuario y tablas que produce | `STEPS` y `retries=2` en géneros (MusicBrainz) |
| [assets.py](../src/lakehouse/orchestration/assets.py) | Un asset por tabla, más el reporte y la salud | `table_spec`: columnas y dueño salen del contrato |
| [checks.py](../src/lakehouse/orchestration/checks.py) | Checks por paso que bloquean | `CROSS_CHECKS`: reconciliación bloquea, frescura avisa |
| [automation.py](../src/lakehouse/orchestration/automation.py) | Jobs, sensores y schedules | `landing_sensor` y `daily_schedule` |
| [health.py](../src/lakehouse/orchestration/health.py) | Salud y alertas | `alerts_for` y sus umbrales |
| [ci.yml](../.github/workflows/ci.yml) | CI en cada push | El job `pipeline`: MinIO + Dagster con sintéticos, luego Superset |
| [docker-compose.yml](../docker-compose.yml) | Servicios | Dagster en 3300 (H-007), MLflow en 5000, Superset en 8088 |
| [__main__.py](../src/lakehouse/orchestration/__main__.py) | CLI: `run`, `status`, `health`, `wait`, `recover` | `run` se niega si ya hay otra corrida (H-011); `recover` rescata las huérfanas (H-012) |

## Cómo verlo funcionando

```powershell
docker compose up -d --build
docker compose exec lakehouse python -m lakehouse.orchestration health                 # salud y alertas
docker compose exec lakehouse python -m lakehouse.orchestration run --job health_only  # un job por Dagster
docker compose exec lakehouse python -m lakehouse.orchestration status                 # corridas y checks
docker compose exec lakehouse python -m lakehouse.orchestration wait                   # espera al pipeline automatico
```

- **Qué debe aparecer:**
  - `Salud: 17 de 17 tablas`;
  - `Salud OK: sin alertas`;
  - `Checks: SUCCEEDED=22`.
- **Dagster:** <http://localhost:3300>.
  - En **Lineage** se ve el grafo (la captura está en `ENTREGABLES/03_calidad_y_linaje/linaje_dagster.png`).
  - En **Runs**, las corridas.
  - En **Automation**, los 3 sensores y los 2 schedules encendidos.
- **MLflow:** <http://localhost:5000>.
- **CI:** la pestaña *Actions* de GitHub.

## Tu minuto en el video (guion)

> "Yo me encargué de que todo corra solo. Dagster orquesta el pipeline: cada tabla es un asset, y el grafo de
> linaje sale de los contratos de gobernanza, no lo dibujamos a mano. *(pantalla: Lineage en Dagster)*. Cada paso
> corre con su propio usuario de MinIO y, al terminar, corren los checks de calidad de sus tablas. Si uno falla,
> lo que sigue no corre, así nunca calculamos Oro sobre datos malos. Nadie le da clic a nada: un sensor detecta
> archivos nuevos en landing y dispara el pipeline, y además corre diario a las 6 de la mañana. *(pantalla:
> Automation y una corrida en verde)*. También monitoreamos la salud: tamaño, versiones y archivos por tabla. Una
> alerta nos avisó de 48 archivos chicos en Bronce y por eso ahora la ingesta compacta. Y cada cambio pasa por CI:
> GitHub levanta MinIO y corre el pipeline completo con datos sintéticos, nunca con los reales. *(pantalla:
> Actions en verde)*."

## Preguntas que te pueden hacer

- **¿Por qué cada paso es un proceso y no una función de Python?** Una sesión de Spark no cambia de credenciales.
  Con un proceso por paso, cada uno entra con el usuario de su rol. Además, Dagster solo envuelve los mismos
  comandos que se pueden correr a mano (D7).
- **¿Qué pasa si falla MusicBrainz?** El paso de géneros se reintenta 2 veces. Si sigue fallando, la corrida queda
  en rojo, `alert_on_failure` deja la alerta y lo de después no corre.
- **¿Correr dos veces duplica datos?** No: cada archivo se identifica por su SHA-256 y Oro usa MERGE. El schedule
  diario sin archivos nuevos no escribe nada.
- **¿Por qué CI usa datos sintéticos?** El repo es público y los datos reales son personales. Los sintéticos
  tienen el mismo esquema, así que prueban lo mismo.
- **¿Qué pasa si se juntan dos corridas?** Dos Spark a la vez agotan la memoria (nos pasó: H-011). Dagster corre
  una a la vez y el comando `run` se niega si ya hay otra en curso.
- **¿Y si se apaga o reinicia el contenedor a media corrida?** Esa corrida quedaba "en curso" para siempre (H-012).
  Ahora, al arrancar, `orchestration recover` la marca como caída del worker y Dagster la reintenta una vez
  (`run_retries` en `dagster.yaml`); si falló un paso o un check, no se reintenta.
- **¿Cuál es la diferencia entre OPTIMIZE y VACUUM?** OPTIMIZE junta archivos chicos en uno grande sin cambiar los
  datos. VACUUM borra los archivos viejos que ya no usa ninguna versión (después de eso ya no se puede viajar en el
  tiempo a esas versiones).
