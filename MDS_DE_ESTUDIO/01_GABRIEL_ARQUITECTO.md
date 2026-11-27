# Gabriel Álvarez Mendoza: Arquitecto de Datos / Líder

## Tu parte en 30 segundos

- **La casa donde vive todo:** MinIO como S3 local, con un bucket por capa; Spark con Delta Lake como motor y
  formato; y un stack de Docker Compose que cualquiera levanta con un comando.
- **Las reglas del juego:** módulos por rol que solo se hablan por tablas Delta, el contrato como única definición
  y versiones fijadas.
- **Tu papel en el video:** abres y cierras. Explicas el flujo completo (landing > bronce > plata > oro > ML/BI) y
  demuestras que se replica desde un clon limpio.

## Lo que tienes que saber (máx. 5 conceptos)

- **Lakehouse:** lo barato y flexible de un data lake (archivos en S3) con lo confiable de un data warehouse (tablas
  con esquema y transacciones). Aquí son archivos en MinIO y tablas Delta.
- **Medallion y un bucket por capa:**
  - Landing es el crudo intacto; Bronce lo vuelve tabla; Plata lo limpia; Oro lo deja listo para BI y ML.
  - Cada servicio tiene su usuario de MinIO con lo mínimo. Por ejemplo, BI solo lee `gold` y solo la ingesta ve
    landing, donde está la IP en claro.
- **Delta Lake:**
  - Es Parquet más un `_delta_log` con cada cambio. Da ACID, esquema forzado, MERGE y *time travel*.
  - `fact_plays` se actualiza con MERGE: re-correr escribe 0 filas.
- **Separación por rol (D7, D24):** cada rol es un subpaquete con su CLI. Dagster corre cada paso como un proceso
  aparte, con el usuario de ese rol. Nadie importa el código de otro; se comunican por tablas. Así la autoría de cada
  integrante queda separada y cada pieza se prueba sola.
- **Reproducibilidad:**
  - Versiones fijadas: `uv.lock` para Python, imágenes por digest, y JARs derivados del lock y verificados.
  - Datos sintéticos con el mismo esquema, porque el repo es público y los reales son personales.
  - `infra/replica.ps1` levanta todo desde un clon limpio y lo verifica de punta a punta.
  - CI hace lo mismo en cada push.

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| [docker-compose.yml](../docker-compose.yml) | Los 5 servicios | `minio`, `minio-init`, `lakehouse` (Dagster), `mlflow`, `superset` y el orden con `depends_on` |
| [infra/app/Dockerfile](../infra/app/Dockerfile) | Imagen de Spark + Delta | Java 17 y `uv sync --frozen` |
| [infra/replica.ps1](../infra/replica.ps1) | Prueba de réplica | Los 7 pasos y el `REPLICA OK` final |
| [src/lakehouse/common/](../src/lakehouse/common/) | Configuración, Spark y smoke | `config.py` (falla claro si falta algo), `spark.py` (S3A con path style) |
| [ENTREGABLES/01_diagrama_arquitectura/](../ENTREGABLES/01_diagrama_arquitectura/DIAGRAMA.md) | Diagrama final | Que cada caja tenga su carpeta |
| [DOCS/ARQUITECTURA.md](../DOCS/ARQUITECTURA.md) | Decisiones D1 a D26 | El porqué de cada pieza y su alternativa |
| [ENTREGABLES/README.md](../ENTREGABLES/README.md) | Índice contra la rúbrica | Dónde está cada entregable |

## Cómo verlo funcionando

```powershell
docker compose up -d --build
docker compose exec lakehouse python -m lakehouse.common.smoke          # termina con "Smoke OK"
docker compose exec lakehouse python -m lakehouse.orchestration status  # corridas en SUCCESS, 22 checks
```

- **MinIO:** en <http://localhost:9001> se ven los 4 buckets y los 5 usuarios `svc-*`.
- **Réplica:** en un clon limpio, `pwsh -File infra/replica.ps1` termina con `REPLICA OK`.

## Tu minuto en el video (guion)

> "Somos el equipo y construimos un lakehouse local y de costo cero sobre el historial de Spotify de uno de
> nosotros. *(pantalla: diagrama)*. Todo corre en Docker: MinIO hace de S3, con un bucket por capa del Medallion
> (landing, bronce, plata y oro), y encima Spark con Delta Lake, que nos da transacciones, esquema forzado y
> *time travel*. Cada integrante tiene su módulo y se comunican solo por tablas, nunca por código, así que cada quien
> es dueño de su parte. Cada servicio entra a MinIO con su propio usuario y solo ve lo que necesita. Para que
> cualquiera lo replique, las versiones están fijadas y hay datos sintéticos con el mismo esquema: con un solo
> script, un clon limpio levanta todo, corre el pipeline y verifica el tablero. *(pantalla: `REPLICA OK`)*. Ahora
> cada quien les cuenta su parte, en el orden en que viajan los datos."

Al cierre (20 s): "Eso es todo: de un JSON crudo a un tablero y un modelo, sin pasos manuales, con calidad
verificada en cada capa y probado en cada cambio. Gracias."

## Preguntas que te pueden hacer

- **¿Por qué Delta y no Iceberg?** Delta funciona con Spark sin catálogo extra; Iceberg necesita un catálogo REST o
  Hive, más piezas para el mismo resultado.
- **¿Por qué un bucket por capa y no prefijos?** Para dar permisos por capa: BI solo lee `gold` y la IP en claro
  nunca sale de landing.
- **¿Por qué Spark en modo local y no un cluster?** Son ~16 mil filas; un cluster solo agrega RAM y piezas. El código
  es el mismo.
- **¿Qué pasó con la imagen de MinIO?** MinIO dejó de publicarla gratis (Docker Hub en 2025, quay.io en 2026);
  usamos la de Chainguard, fijada por digest.
- **¿Cómo sé que es reproducible?** `infra/replica.ps1` lo demuestra desde un clon limpio, y CI corre el pipeline
  completo con sintéticos en cada push.
- **¿Qué fue lo más difícil?** Los detalles de operación: la memoria cuando coincidían dos corridas de Spark
  (H-011), los archivos chicos en Bronce (H-009) y el ciclo en los checks por capa (H-008). Cada uno quedó anotado en
  `DOCS/HALLAZGOS.md` con su causa y su arreglo.
