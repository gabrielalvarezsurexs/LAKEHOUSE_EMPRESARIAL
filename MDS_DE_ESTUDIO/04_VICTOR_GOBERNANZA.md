# Victor Gabriel Torres Guerrero: Oficial de Gobernanza y Seguridad

## Tu parte en 30 segundos

Tú defines **qué es un dato correcto y quién lo puede tocar**. Cada tabla tiene un contrato (columnas, tipos,
nulos, privacidad, dueño, clasificación y reglas). De ese contrato salen solos tres productos:

- el **catálogo**;
- los **325 checks de calidad** que corren sobre Bronce, Plata y Oro;
- el **linaje**.

Además, cada servicio entra a MinIO con su **propio usuario y permisos mínimos**: el usuario root no entra al
pipeline. Tu pieza vigila todo el flujo landing > bronce > plata > oro > ML/BI sin modificar ningún dato.

## Lo que tienes que saber (máx. 5 conceptos)

- **Contrato de datos:** la única definición de cada tabla, en `contracts.py`. Por ejemplo, `silver.plays` dice:
  - `play_id` es único;
  - `local_hour` está entre 0 y 23;
  - `platform_family` solo puede ser uno de 8 valores.

  Si alguien cambia una regla, cambian juntos el catálogo, los checks y la tabla Delta.
- **Checks por capa:**
  - **Por tabla:** que exista, esquema, privacidad y metadatos.
  - **Por fila:** no nulos, valores aceptados, rangos, reglas en SQL (por ejemplo, `ts_start <= ts_end`), llaves
    únicas e integridad referencial (todo `artist_key` del hecho existe en `dim_artist`).
  - **Entre capas:** reconciliación (bitácora = Bronce, Bronce = Plata + descartes, Plata = Oro) y frescura.

  "Error" bloquea; "aviso" no bloquea.
- **Privacidad y clasificación:**
  - **La IP:** es dato personal. Solo existe en landing (clasificación restringido). En Bronce se guarda como
    HMAC-SHA256 con sal, que es un seudónimo (confidencial). En Plata se elimina.
  - **Plata y Oro:** son "interno", sin identificadores.
  - **Verificación:** un check de privacidad revisa cada tabla real.
- **Mínimo privilegio (D17):** cinco usuarios de MinIO.
  - `svc-ingestion` es el único que ve landing.
  - `svc-transformation` lee Bronce y escribe Plata y Oro.
  - `svc-governance` solo lee.
  - `svc-ml` solo escribe sus tablas `ml_*`.
  - `svc-bi` solo lee Oro.

  Una prueba en vivo intenta 41 operaciones y confirma que MinIO permite y niega exactamente lo esperado.
- **Metadatos en la tabla:** cada tabla Delta guarda su descripción, un comentario por columna y sus
  propiedades (dueño, capa, clasificación). El metadato viaja con el dato, no solo en un documento aparte.

## Qué revisar en el repo

| Archivo | Qué hace | Qué mirar |
|---|---|---|
| [contracts.py](../src/lakehouse/governance/contracts.py) | Contratos de las 18 tablas | `SILVER_PLAYS`: `unique_keys`, `rules`, `values`, `min_value` |
| [quality.py](../src/lakehouse/governance/quality.py) | Corre los checks y la reconciliación | `table_checks` y `reconciliation_checks` |
| [access.py](../src/lakehouse/governance/access.py) | Matriz de acceso, políticas y prueba en vivo | `ACCESS` (todo sale de ahí) |
| [infra/minio/init.sh](../infra/minio/init.sh) y `policies/` | Crea usuarios y aplica políticas en MinIO | Los JSON se generan, no se escriben a mano |
| [lineage.py](../src/lakehouse/governance/lineage.py) | Grafo de linaje y linaje de columnas clave | `COLUMN_LINEAGE`: `ip_addr` > `ip_addr_hash` > se elimina |
| [CALIDAD_Y_LINAJE.md](../ENTREGABLES/03_calidad_y_linaje/CALIDAD_Y_LINAJE.md) | Reporte de la última corrida | Resumen por capa y matriz de accesos |
| [CATALOGO.md](../ENTREGABLES/03_calidad_y_linaje/CATALOGO.md) | Catálogo generado | Clasificación y quién puede leer cada tabla |

## Cómo verlo funcionando

```powershell
docker compose exec lakehouse python -m lakehouse.governance check   # checks + accesos + reporte
docker compose exec lakehouse python -m lakehouse.governance access  # solo la prueba de usuarios
uv run python -m lakehouse.governance docs                           # catálogo, linaje y políticas (local)
```

**Debe aparecer:** `Calidad: 325 checks en 17 tablas, 0 fallas (error), 0 avisos`,
`Acceso MinIO: 41 pruebas, 41 como se esperaba` y `Gobernanza OK`.

En la consola de MinIO (<http://localhost:9001>), en **Identity > Users**, se ven los cinco `svc-*` con su
política.

## Tu minuto en el video (guion)

> "Yo me encargué de gobernanza y seguridad. Todo empieza en un contrato por tabla: columnas, tipos, quién es el
> dueño, qué tan sensible es y qué reglas debe cumplir. *(pantalla: `contracts.py`, `SILVER_PLAYS`)*. De ese
> contrato salen solos el catálogo, el linaje y 325 checks que corren sobre las tres capas: nulos, valores
> válidos, rangos, llaves únicas, integridad del modelo estrella y la reconciliación entre capas. Las 15,743 filas
> de Bronce cuadran con Plata más descartes, y Plata con Oro. *(pantalla: `CALIDAD_Y_LINAJE.md`, resumen en
> verde)*. En privacidad, la IP es el único dato personal: en Bronce queda como un hash con sal y en Plata
> desaparece; un check lo verifica en cada tabla. Y en seguridad, nadie usa el usuario root: cada servicio tiene su
> usuario con permisos mínimos. BI solo puede leer Oro, y lo probamos en vivo: 41 operaciones, todas permitidas o
> negadas como esperábamos. *(pantalla: matriz de accesos)*."

## Preguntas que te pueden hacer

- **¿Qué pasa si un check falla?** Si es "error", `governance check` termina con código 1 y el reporte lo marca
  con ❌ y su conteo. En F5, Dagster lo muestra como asset check fallido. Los avisos (frescura, metadatos) no
  bloquean.
- **¿Por qué no borrar la IP desde Bronce?** Bronce guarda la fuente casi intacta para auditar. El seudónimo
  permite contar IPs distintas sin saber cuáles son, y sin la sal (que está en `.env`) no se puede revertir.
- **¿Cómo sabes que las políticas sirven?** No solo las escribimos: `access` entra con cada usuario e intenta
  listar y escribir en cada bucket. Una política demasiado abierta sale como falla (también hay una prueba
  unitaria que lo simula).
- **¿El catálogo se puede desactualizar?** No: se genera del contrato y una prueba falla si el archivo en
  `ENTREGABLES/` no coincide con lo generado.
- **¿Qué es el linaje de columnas?** De dónde sale cada columna clave. Por ejemplo, `is_skip` sale de
  `reason_end` y `ms_played`, y `date_key` de la hora local, que a su vez sale de `ts` y `conn_country`.
