# Paquete para el equipo

Qué mandarle a cada integrante, en qué orden y qué tiene que practicar. Solo Gabriel levanta el entorno; los demás
**no instalan nada**. Estudian su guía (15 min), ven su parte funcionando en la demo y practican sus preguntas.

## Qué mandar (un mensaje por persona)

| Quién | Su guía | Su turno en el video | Qué ver en el repo (en GitHub, sin instalar) |
|---|---|---|---|
| Ricardo | [02_RICARDO_INGESTA.md](02_RICARDO_INGESTA.md) | 2.º (1:20 a 2:40) | `src/lakehouse/ingestion/` |
| Juan Pablo | [03_JUAN_PABLO_TRANSFORMACION.md](03_JUAN_PABLO_TRANSFORMACION.md) | 3.º (2:40 a 4:00) | `src/lakehouse/transformation/` |
| Victor | [04_VICTOR_GOBERNANZA.md](04_VICTOR_GOBERNANZA.md) | 4.º (4:00 a 5:20) | `src/lakehouse/governance/`, `ENTREGABLES/03_calidad_y_linaje/` |
| Fernando | [06_FERNANDO_ML.md](06_FERNANDO_ML.md) | 5.º (5:20 a 6:40) | `src/lakehouse/ml/`, `notebooks/ml_skip.ipynb`, `ENTREGABLES/04_modelo_ml/` |
| Emilio | [05_EMILIO_BI.md](05_EMILIO_BI.md) | 6.º (6:40 a 8:00) | `bi/`, `ENTREGABLES/05_tablero_bi/` |
| Nora | [07_NORA_DEVOPS.md](07_NORA_DEVOPS.md) | 7.º (8:00 a 9:20) | `src/lakehouse/orchestration/`, `.github/workflows/ci.yml` |

A todos, además: el [guion](00_GUION_VIDEO.md) (para saber quién va antes y después) y el
[diagrama](../ENTREGABLES/01_diagrama_arquitectura/DIAGRAMA.md).

### Mensaje modelo

> Te toca [tema], en el turno [n] del video (~1:20). Lee tu guía: [link]. Son 15 minutos. Apréndete la sección
> "Tu minuto en el video" (puedes decirlo con tus palabras) y practica las 5 preguntas de abajo. No tienes que
> instalar nada: yo manejo la pantalla. El ensayo es el domingo 29 a las [hora].

## Orden

1. **Hoy:** mandar a cada quien su guía, el guion y el diagrama.
2. **Sábado 28:** cada quien manda su minuto grabado en audio (aunque sea con el celular), para medir el tiempo y
   corregir antes del ensayo.
3. **Domingo 29:** ensayo cronometrado completo y grabación del video.
4. **Lunes 30, 4 pm:** presentación en vivo desde la laptop de Gabriel, con el video de respaldo.

## Qué tiene que poder contestar cualquiera (no solo el dueño de la parte)

- **¿Qué es un lakehouse y por qué Medallion?** Archivos baratos (MinIO) con tablas confiables (Delta). Cada capa
  sube la calidad: crudo, tabla, limpio, listo para usar.
- **¿Cómo protegen los datos personales?** La IP se vuelve un hash con sal al entrar a Bronce y desaparece en
  Plata. Cada servicio tiene un usuario de MinIO con permisos mínimos, y el repo solo tiene datos sintéticos.
- **¿Qué pasa si llega un archivo nuevo?** Un sensor de Dagster lo detecta y corre todo hasta Oro, ML y el reporte.
  Re-correr no duplica nada.
- **¿Cómo saben que los datos están bien?** Checks de calidad después de cada paso; si uno falla, lo siguiente no
  corre. Además, los conteos se reconcilian entre capas.
- **¿Cómo lo replica alguien más?** Con un clon del repo y un comando (`infra/replica.ps1`), con datos sintéticos.
  CI hace lo mismo en cada cambio.
