# Guion del video (máx. 10 minutos)

Los 7 hablan ~1:20 cada uno, en el orden en que viajan los datos. El texto de cada quien está en su guía,
sección **"Tu minuto en el video"**; aquí van el orden, los tiempos y qué se ve en pantalla.

## Orden y tiempos

| # | Tiempo | Quién | Tema | Pantalla (la maneja Gabriel) |
|---|---|---|---|---|
| 1 | 0:00 a 1:20 | Gabriel | Qué construimos y la arquitectura | Diagrama (`ENTREGABLES/01_diagrama_arquitectura/arquitectura.png`); consola de MinIO con los 4 buckets y los usuarios `svc-*` |
| 2 | 1:20 a 2:40 | Ricardo | Ingesta: landing, Bronce, cuarentena, IP seudonimizada, MusicBrainz, sintéticos | MinIO `landing/spotify/`; log de la ingesta (`ya ingeridos=2` al re-correr) |
| 3 | 2:40 a 4:00 | Juan Pablo | Plata y Oro: limpieza, sesiones, skip, estrella, MERGE, reconciliación | MinIO `gold/`; reconciliación con las 4 líneas `[OK]` |
| 4 | 4:00 a 5:20 | Victor | Contratos, calidad, catálogo, linaje, privacidad y accesos | `contracts.py`; `CALIDAD_Y_LINAJE.md`; `LINAJE.md` |
| 5 | 5:20 a 6:40 | Fernando | Modelo de skip, sin fuga, contra baseline, MLflow | Notebook `ml_skip.ipynb`; MLflow (Model training, alias `champion`) |
| 6 | 6:40 a 8:00 | Emilio | Tablero sobre Oro, filtros, modelo contra realidad | Superset: tablero completo, aplicar un filtro, retención |
| 7 | 8:00 a 9:20 | Nora | Todo corre solo: Dagster, checks que bloquean, monitoreo, CI | Dagster: Lineage, Automation, una corrida en verde; GitHub Actions en verde |
| 8 | 9:20 a 9:40 | Gabriel | Cierre | Diagrama otra vez |

Total: 9:40, con 20 s de margen. Si alguien se pasa, recorta sus ejemplos, no su idea principal.

## Antes de grabar (checklist de pantallas)

- [ ] Stack arriba con los datos reales, y `orchestration wait` terminó en `Pipeline listo`.
- [ ] Pestañas abiertas en orden:
  - el diagrama;
  - MinIO <http://localhost:9001>;
  - `contracts.py`;
  - `CALIDAD_Y_LINAJE.md`;
  - el notebook;
  - MLflow <http://localhost:5000>;
  - Superset <http://localhost:8088/superset/dashboard/spotify-lakehouse/>;
  - Dagster <http://localhost:3300>;
  - GitHub Actions.
- [ ] Ya con sesión iniciada en Superset (usuario `admin`), para no mostrar la contraseña.
- [ ] Una terminal con letra grande para los comandos de Ricardo y Juan Pablo (re-correr y que no duplique).
- [ ] Nada sensible en pantalla:
  - ni el `.env`;
  - ni renglones de datos, salvo el tablero, que es lo que el dueño de los datos aprobó.

## Cómo grabar

- Una sola grabación de pantalla, con Gabriel manejando la demo. Cada quien habla en su turno; puede ser por
  videollamada grabada con la pantalla de Gabriel compartida.
- Ensayo cronometrado completo al menos una vez antes de grabar (domingo 29).
- Exportar a ≤ 10 min. Subirlo como "no listado" y poner el link en `ENTREGABLES/06_video/README.md` y en
  Blackboard junto con el link al repo.

## Cierre (Gabriel, 20 s)

> "Eso es todo: de un JSON crudo a un tablero y un modelo, sin pasos manuales, con calidad verificada en cada capa y
> probado en cada cambio. Todo está en el repo y se replica con un comando. Gracias."
