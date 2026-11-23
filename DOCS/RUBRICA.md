# Rúbrica y entregables

Resumen operativo del PDF ([INSTRUCCIONES_PROYECTO.pdf](INSTRUCCIONES_PROYECTO.pdf)). Se marca al cerrar cada fase.

**Fecha límite:** lunes 30 de noviembre de 2026, 11:59 pm. Presentación en clase ese día a las 4 pm. En
Blackboard: link al repo y link al video (respaldo por si no alcanzan a presentar).

## Entregables (un solo repo de GitHub)

El código vive en la raíz; `ENTREGABLES/` guarda lo que no es código y un `README.md` que apunta a todo.

| # | Entregable | Código (raíz) | En `ENTREGABLES/` | Responsable | Fase | Listo |
|---|---|---|---|---|---|---|
| 1 | Diagrama de arquitectura (MinIO, capas, motores, consumo) | | `01_diagrama_arquitectura/` (Mermaid + imagen) | Gabriel | F1, F8 | [ ] |
| 2 | Scripts de despliegue / infraestructura (Compose, SQL, config) | `docker-compose.yml`, `infra/`, `.env.example`, SQL en `transformation/` y `bi/` | `02_infraestructura/README.md` (qué es cada pieza y dónde está) | Gabriel | F1 | [ ] |
| 3 | Reporte de calidad y linaje (catálogo y pruebas por capa) | `src/lakehouse/governance/` (lo genera) | `03_calidad_y_linaje/` (`CALIDAD_Y_LINAJE.md`, `CATALOGO.md`, `LINAJE.md`, captura del linaje en Dagster) | Victor | F4, F8 | [ ] (F4 listo; falta la captura de F5) |
| 4 | Código de ML (entrenamiento, métricas, inferencia) | `src/lakehouse/ml/`, `notebooks/` | `04_modelo_ml/` (`ML_METRICAS.md`, gráficas) | Fernando | F6 | [ ] |
| 5 | Tablero de BI activo (KPIs: tendencias, géneros top, retención) | `bi/`, `infra/superset/` | `05_tablero_bi/` (capturas aprobadas, export, cómo abrirlo) | Emilio | F7 | [ ] |
| 6 | Video técnico, **máx. 10 min**, cada integrante explica su contribución | | `06_video/` (link); también en `ENTREGABLES/README.md` | Todos | F9 | [ ] |

## Rúbrica (10 pts) y cómo la cubrimos

| Criterio | Pts | Qué piden | Evidencia que vamos a mostrar |
|---|---|---|---|
| Arquitectura y Lakehouse | 2 | Medallion con Delta/Iceberg sobre MinIO; manejo de esquemas | Buckets por capa, tablas Delta, contratos de esquema, evolución de esquema controlada, diagrama |
| Pipelines y orquestación | 2 | Código limpio, modular, manejo de errores; flujo automatizado sin ejecuciones manuales | Módulos por rol, reintentos, cuarentena, idempotencia, Dagster con schedule + sensor |
| Gobernanza y calidad | 1.5 | Catálogo y mapeo de linaje | Catálogo generado, grafo de linaje (Dagster + Mermaid), checks por capa, seudonimización, políticas MinIO. **Hecho (F4):** 325 checks en 17 tablas en verde, 5 usuarios con mínimo privilegio probados en vivo (41/41), catálogo y linaje generados del contrato. Falta: grafo de Dagster (F5) |
| Machine Learning | 1.5 | Valor de negocio, consume directo de Oro, métricas claras | Predicción de skip, baseline vs modelo, ROC-AUC/PR-AUC/F1, MLflow, predicciones de vuelta a Oro |
| BI y analítica | 1.5 | Atractivo, responsivo, KPIs útiles, consultas optimizadas desde Oro | Superset sobre marts de Oro, filtros probados, tiempos de respuesta |
| Documentación y DevOps | 1 | Repo organizado, README con instrucciones para replicar | README paso a paso con uv y Docker, datos sintéticos, CI en verde, prueba desde un clon limpio |
| Presentación | 0.5 | Claridad, tiempo, participación equitativa de los 7 | Guion con ~1:20 por integrante, ensayo cronometrado |
