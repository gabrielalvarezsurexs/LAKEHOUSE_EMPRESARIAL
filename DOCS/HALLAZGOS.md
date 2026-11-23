# Hallazgos

Un renglón por causa, el mismo día que se encuentra, aunque no se arregle. La causa es hipótesis hasta verificarla.

| Id | Fecha | Dónde | Qué pasa | Hipótesis / causa | Estado |
|---|---|---|---|---|---|
| H-001 | 2026-11-22 | `skipped` (fuente) | Solo ~3% de filas en `true`, aunque hay más de 1,500 terminaciones por `fwdbtn` | Spotify llena poco el campo; no sirve solo como etiqueta | Mitigado en diseño: etiqueta derivada (DATOS_FUENTE §1.3) |
| H-002 | 2026-11-22 | Imagen de MinIO | `quay.io/minio/minio` responde 401 (también `mc`) | MinIO cerró el acceso anónimo en quay.io (sep 2026) | Resuelto: Chainguard (`cgr.dev/chainguard/minio` y `minio-client`) fijadas por digest |
| H-003 | 2026-11-22 | `offline_timestamp` (fuente) | Unidades mezcladas: 1,940 valores con 10 dígitos (segundos) y 25 con 13 (milisegundos) | Spotify cambió el formato en algún momento | Pendiente F3: Plata normaliza a segundos por número de dígitos; Bronce lo guarda tal cual |
