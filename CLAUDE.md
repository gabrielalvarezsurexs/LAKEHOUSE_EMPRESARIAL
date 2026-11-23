# CLAUDE.md

Reglas y apuntadores para trabajar en este repo. Solo reglas: el detalle vive en `DOCS/`.

## Qué es

Proyecto Final de Datos Masivos (UP, 15%): un **Data Lakehouse** local y de $0 sobre el historial extendido de
Spotify de un integrante. Los datos crudos se ingieren a MinIO, pasan por las capas Bronce > Plata > Oro (Delta Lake
con Spark), con gobernanza, un modelo de ML que lee de Oro y un tablero de BI. Instrucciones del profe:
[DOCS/INSTRUCCIONES_PROYECTO.pdf](DOCS/INSTRUCCIONES_PROYECTO.pdf). Equipo y roles: [DOCS/EQUIPO.md](DOCS/EQUIPO.md).

**Entrega: lunes 30 de noviembre de 2026** (presentación en clase a las 4 pm; Blackboard hasta 11:59 pm).

## Estado

Ver [DOCS/ROADMAP.md](DOCS/ROADMAP.md) (tracker y sección "Retomar aquí"). No se copia aquí.

## Antes de tocar X, lee Y

| Si vas a tocar... | Lee primero |
|---|---|
| Cualquier cosa (arranque) | [DOCS/ROADMAP.md](DOCS/ROADMAP.md) y [DOCS/ARQUITECTURA.md](DOCS/ARQUITECTURA.md) |
| Stack, versiones, Docker, imágenes | [DOCS/REFERENCIAS_TECNICAS.md](DOCS/REFERENCIAS_TECNICAS.md) |
| Ingesta, esquemas, columnas de la fuente | [DOCS/DATOS_FUENTE.md](DOCS/DATOS_FUENTE.md) |
| Qué pide el profe y cuánto vale | [DOCS/RUBRICA.md](DOCS/RUBRICA.md) |
| Un MD de estudio de un integrante | [MDS_DE_ESTUDIO/README.md](MDS_DE_ESTUDIO/README.md) (plantilla) |
| Un bug encontrado | [DOCS/HALLAZGOS.md](DOCS/HALLAZGOS.md) (se anota el mismo día) |

## Dónde va cada cosa

- `ENTREGABLES/`: **el proyecto que se entrega** (código, infraestructura, reportes, diagrama, tablero). Su
  `README.md` es el índice entregable por entregable contra la rúbrica.
- `DOCS/`: docs de trabajo (roadmap, arquitectura y decisiones, referencias, hallazgos).
- `MDS_DE_ESTUDIO/`: una guía corta por integrante; se escribe cuando su parte queda implementada.
- `.github/`: CI y `CODEOWNERS` (GitHub los exige en la raíz).
- `DATOS_SPOTIFY/`: datos reales, **gitignored**. Nunca se versionan.

## Cómo se corre

Se llena por fase (F1 en adelante). Todo comando que Gabo deba correr va completo y listo para pegar.

## Seguridad y privacidad

- **El repo es público.** Los datos reales (`DATOS_SPOTIFY/`, cualquier `data/`, Parquet o Delta) nunca entran a git.
  Antes de pedir un commit, `git status` no debe mostrar datos.
- **Salidas solo con conteos** sobre los datos reales: filas, nulos, distintos, nombres de columna. Nunca imprimir
  renglones, IPs, canciones o artistas concretos en respuestas, logs o reportes versionados.
- `ip_addr` se seudonimiza (hash con sal del `.env`) al entrar a Bronce y se elimina en Plata.
- Nunca imprimir el `.env` ni secretos. Las credenciales viven en `.env` (gitignored); en el repo solo `.env.example`.
- Llamadas a APIs externas (MusicBrainz) sin datos personales en el User-Agent ni en la URL.

## Cómo se mide un cambio

- Pruebas unitarias por módulo (`ENTREGABLES/tests/`), corren sin Docker cuando se puede.
- Pruebas de calidad por capa (checks de gobernanza) sobre las tablas reales: el reporte se regenera, no se edita.
- Corrida completa del pipeline con datos sintéticos (lo mismo que corre CI) y con los reales (la corre Gabo).
- Conteos de reconciliación entre capas (filas Bronce = filas Plata + descartadas con causa).

## Invariantes (si se rompen, degradan en silencio)

- **Flechas en un sentido:** landing > bronce > plata > oro > (ML, BI). Ninguna capa lee de una posterior.
- **Los módulos de cada rol se comunican por tablas Delta, no por imports.** Solo se importa `lakehouse.common` y
  los contratos de `lakehouse.governance.contracts`. Esto mantiene la autoría de cada integrante separable.
- ML y BI leen **solo de Oro**.
- Las escrituras son idempotentes: re-correr el pipeline con los mismos archivos no duplica filas.
- Mejor un dato vacío que uno falso: lo ausente es `null`, nunca `0` ni un default inventado.
- Un esquema se define una sola vez (contratos de gobernanza); nada de listas de columnas repetidas.
- Ninguna carpeta de código se llama `data/` (está en `.gitignore`).

## Convenciones

- Español de México con tuteo en docs, comentarios, logs y mensajes. Identificadores en inglés. Código en ASCII;
  los `.md` con acentos. Nunca el guion largo (em dash): coma, dos puntos o paréntesis.
- **Autoría visible:** cada archivo de código empieza con un docstring o comentario `Responsable: <nombre> (<rol>)`.
  La carpeta de cada rol está en [DOCS/EQUIPO.md](DOCS/EQUIPO.md) y en `.github/CODEOWNERS`.
- Comentarios cortos: solo el porqué, hasta 3 renglones. Docstring de una línea (más 3 si es contrato).
- Código limpio y modular: funciones chicas, configuración en `lakehouse.common.config`, errores con mensajes
  claros y reintentos donde hay red. Nada de rutas ni credenciales hardcodeadas.
- Los docs van en el mismo cambio que el código.
- Revisar dos veces todo hallazgo o supuesto antes de reportarlo: releyendo el código y contra los datos (conteos).
- Una pregunta no es una tarea; una auditoría no edita: se reporta y se pregunta.
- PowerShell con `$env:PYTHONIOENCODING="utf-8"`.

## Antes de pedir el commit

- [ ] ¿`git status` sin datos reales ni secretos?
- [ ] ¿Capa correcta, flechas en un sentido, sin imports entre roles?
- [ ] ¿Encabezado `Responsable:` en archivos nuevos?
- [ ] ¿Pruebas chicas en verde? ¿Gabo corrió la pesada si aplica?
- [ ] ¿ROADMAP, HALLAZGOS y docs al día?
