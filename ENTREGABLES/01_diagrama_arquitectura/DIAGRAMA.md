# Diagrama de arquitectura

Entregable 1. Versión 1 (F1); la final con imagen exportada queda en F8. GitHub dibuja el Mermaid directo.

```mermaid
flowchart LR
  subgraph Fuentes
    S[JSON historial extendido<br/>de Spotify]
    MB[API MusicBrainz<br/>géneros]
  end

  subgraph Docker["Docker Compose (local, $0)"]
    subgraph MinIO["MinIO: almacenamiento S3"]
      L[(landing<br/>crudo intacto)]
      B[(bronze<br/>Delta)]
      P[(silver<br/>Delta)]
      O[(gold<br/>Delta: estrella + KPIs)]
    end
    subgraph App["Contenedor lakehouse: Spark 3.5 + Delta Lake"]
      I[Ingesta]
      T1[Bronce a Plata]
      T2[Plata a Oro]
      Q[Checks de calidad<br/>catálogo y linaje]
      ML[ML: predicción de skip]
    end
    D[Dagster<br/>schedule, sensor, linaje]
    MF[MLflow<br/>experimentos]
    SS[Superset + DuckDB<br/>tablero]
  end

  S --> I
  MB --> I
  I --> L --> B --> T1 --> P --> T2 --> O
  Q -. valida .-> B & P & O
  O --> ML -->|predicciones| O
  ML -. métricas .-> MF
  O -->|delta_scan| SS
  D -. orquesta .-> I & T1 & T2 & Q & ML
  GH[GitHub Actions CI<br/>datos sintéticos] -. prueba .-> App
```

| Pieza | Qué hace | Dónde está |
|---|---|---|
| MinIO | Almacenamiento S3 local, un bucket por capa | `docker-compose.yml` (servicio `minio`), `infra/minio/` |
| Spark + Delta Lake | Cómputo y formato de tabla con ACID, historial y time travel | `infra/app/Dockerfile`, `src/lakehouse/common/spark.py` |
| Medallion | landing > bronze > silver > gold | `src/lakehouse/ingestion/`, `src/lakehouse/transformation/` |
| Gobernanza | Contratos, calidad, catálogo, linaje, seudonimización | `src/lakehouse/governance/` |
| Dagster | Orquestación sin ejecuciones manuales | `src/lakehouse/orchestration/` |
| ML | Predicción de skip leyendo de Oro, MLflow | `src/lakehouse/ml/`, `notebooks/` |
| BI | Superset sobre Oro vía DuckDB | `bi/`, `infra/superset/` |

Decisiones y su porqué: [DOCS/ARQUITECTURA.md](../../DOCS/ARQUITECTURA.md).
