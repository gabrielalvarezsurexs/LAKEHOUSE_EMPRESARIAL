"""Checks de calidad por capa, derivados de los contratos, mas reconciliacion y frescura entre capas.

Por tabla: existe, esquema, privacidad, metadatos, no vacia, no nulos, valores aceptados, rangos, reglas,
llaves unicas y referencias. Cada resultado lleva solo conteos y nombres de columna (el reporte es publico).
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from lakehouse.common.delta import TableLocator, read_optional
from lakehouse.common.log import get_logger
from lakehouse.governance.contracts import (
    ALL_TABLES,
    BRONZE_INGESTION_LOG,
    BRONZE_QUARANTINE,
    BRONZE_SPOTIFY_STREAMS,
    DELTA_TABLES,
    GOLD_DIM_TRACK,
    GOLD_FACT_PLAYS,
    GOLD_KPI_LISTENING_DAILY,
    PERSONAL,
    PSEUDONYM,
    SILVER_ARTISTS,
    SILVER_DISCARDED,
    SILVER_EPISODES,
    SILVER_PLAYS,
    TableContract,
    by_name,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

log = get_logger("lakehouse.governance.quality")

ERROR, WARN = "error", "warn"
MAX_INGESTION_AGE = timedelta(days=30)
# Operaciones que cambian datos (no metadatos) para medir frescura entre capas.
DATA_OPERATIONS = ("WRITE", "MERGE", "DELETE", "UPDATE", "STREAMING UPDATE")
PERSONAL_COLUMNS = {c.name for t in ALL_TABLES for c in t.columns if c.privacy == PERSONAL}
PSEUDONYM_COLUMNS = {c.name for t in ALL_TABLES for c in t.columns if c.privacy == PSEUDONYM}


@dataclass(frozen=True)
class CheckResult:
    """table es 'capa.tabla' (o 'a > b' en checks entre capas); failed = filas o elementos que fallan."""

    table: str
    kind: str
    check: str
    severity: str
    ok: bool
    failed: int = 0
    detail: str = ""


@dataclass
class QualityRun:
    results: list[CheckResult] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.results if not r.ok and r.severity == ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.ok and r.severity == WARN]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        """Forma JSON que lee la orquestacion (F5) para sus asset checks; solo conteos y nombres."""
        return {"row_counts": dict(self.row_counts), "results": [asdict(r) for r in self.results]}


def _result(contract: TableContract, kind: str, check: str, failed: int, detail: str = "", severity=ERROR):
    return CheckResult(contract.qualified_name, kind, check, severity, failed == 0, failed, detail)


def _type_string(dtype: str) -> str:
    from pyspark.sql.types import _parse_datatype_string

    return _parse_datatype_string(dtype).simpleString()


def schema_checks(contract: TableContract, df: DataFrame) -> list[CheckResult]:
    """Columnas y tipos contra el contrato; los nulos se revisan por filas (Bronce viejo es nullable)."""
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    missing = [c.name for c in contract.columns if c.name not in actual]
    wrong = [c.name for c in contract.columns if c.name in actual and actual[c.name] != _type_string(c.dtype)]
    extra = [n for n in actual if n not in contract.column_names]
    problems = []
    if missing:
        problems.append(f"faltan: {', '.join(missing)}")
    if wrong:
        problems.append(f"tipo distinto: {', '.join(wrong)}")
    return [
        _result(
            contract,
            "esquema",
            "columnas y tipos del contrato",
            len(missing) + len(wrong),
            "; ".join(problems),
        ),
        _result(
            contract,
            "esquema",
            "sin columnas fuera del contrato",
            len(extra),
            ", ".join(extra),
            severity=WARN,
        ),
    ]


def privacy_check(contract: TableContract, df: DataFrame) -> CheckResult:
    """Ninguna tabla Delta guarda la IP en claro; despues de Bronce tampoco su seudonimo (D8)."""
    forbidden = set(PERSONAL_COLUMNS)
    if contract.layer != "bronze":
        forbidden |= PSEUDONYM_COLUMNS
    found = sorted(n for n in df.columns if n in forbidden or (contract.layer != "bronze" and "ip_addr" in n))
    return _result(contract, "privacidad", "sin IP ni seudonimo donde no toca", len(found), ", ".join(found))


def metadata_check(spark: SparkSession, uri: str, contract: TableContract, df: DataFrame) -> CheckResult:
    """Comentarios de columna y propiedades (dueno, capa, clasificacion) iguales al contrato."""
    from delta.tables import DeltaTable

    comments = {f.name: f.metadata.get("comment") for f in df.schema.fields}
    bad_comments = [
        c.name for c in contract.columns if c.name in comments and comments[c.name] != c.description
    ]
    props = DeltaTable.forPath(spark, uri).detail().first()["properties"] or {}
    bad_props = [k for k, v in contract.table_properties().items() if props.get(k) != v]
    detail = []
    if bad_comments:
        detail.append(f"{len(bad_comments)} columnas sin comentario del contrato")
    if bad_props:
        detail.append(f"propiedades: {', '.join(bad_props)}")
    return _result(
        contract,
        "metadatos",
        "comentarios y propiedades del contrato",
        len(bad_comments) + len(bad_props),
        "; ".join(detail),
        severity=WARN,
    )


def _row_specs(contract: TableContract, columns: set[str]) -> list[tuple[str, str, Column]]:
    """(tipo, nombre, condicion de falla) de cada regla por fila que se puede evaluar."""
    from pyspark.sql import functions as F

    specs = []
    for col in contract.columns:
        if col.name not in columns:
            continue
        c = F.col(f"`{col.name}`")
        if not col.nullable:
            specs.append(("no_nulos", f"{col.name} no nulo", c.isNull()))
        if col.values:
            specs.append(("valores", f"{col.name} en {{{', '.join(col.values)}}}", ~c.isin(*col.values)))
        if col.min_value is not None or col.max_value is not None:
            low = "-inf" if col.min_value is None else f"{col.min_value:g}"
            high = "inf" if col.max_value is None else f"{col.max_value:g}"
            below = c < col.min_value if col.min_value is not None else F.lit(False)
            above = c > col.max_value if col.max_value is not None else F.lit(False)
            specs.append(("rango", f"{col.name} en [{low}, {high}]", below | above))
    for rule in contract.rules:
        specs.append(("regla", rule.name, ~F.coalesce(F.expr(rule.sql), F.lit(True))))
    return specs


def row_checks(contract: TableContract, df: DataFrame) -> tuple[int, list[CheckResult]]:
    """Una sola pasada: total de filas y cuantas fallan cada regla."""
    from pyspark.sql import functions as F

    specs = _row_specs(contract, set(df.columns))
    aggs = [F.count(F.lit(1)).alias("_total")]
    aggs += [F.count(F.when(cond, True)).alias(f"_q{i}") for i, (_, _, cond) in enumerate(specs)]
    row = df.agg(*aggs).first()
    total = row["_total"]
    results = [
        _result(
            contract,
            "volumen",
            "tiene filas",
            int(total == 0 and not contract.allow_empty),
            "tabla vacia" if total == 0 else "",
            severity=WARN,
        )
    ]
    for i, (kind, name, _) in enumerate(specs):
        failed = row[f"_q{i}"]
        results.append(_result(contract, kind, name, failed, f"{failed} de {total} filas" if failed else ""))
    return total, results


def unique_checks(contract: TableContract, df: DataFrame, total: int) -> list[CheckResult]:
    results = []
    for key in contract.unique_keys:
        if not set(key) <= set(df.columns):
            continue
        repeated = total - df.select(*key).distinct().count()
        name = f"unica por ({', '.join(key)})"
        results.append(
            _result(contract, "unicos", name, repeated, f"{repeated} filas repetidas" if repeated else "")
        )
    return results


def reference_checks(
    spark: SparkSession, locator: TableLocator, contract: TableContract, df: DataFrame
) -> list[CheckResult]:
    """Cada llave no nula existe en su dimension (integridad referencial del modelo estrella)."""
    from pyspark.sql import functions as F

    results = []
    for fk in contract.references:
        name = f"{fk.column} existe en {fk.ref}.{fk.ref_column}"
        ref = read_optional(spark, locator, by_name(fk.ref))
        if ref is None or fk.column not in df.columns:
            results.append(_result(contract, "referencia", name, 1, f"no existe {fk.ref}"))
            continue
        keys = ref.select(F.col(fk.ref_column).alias(fk.column)).distinct()
        orphans = df.where(F.col(fk.column).isNotNull()).join(keys, fk.column, "left_anti").count()
        results.append(
            _result(contract, "referencia", name, orphans, f"{orphans} filas huerfanas" if orphans else "")
        )
    return results


def table_checks(
    spark: SparkSession, locator: TableLocator, contract: TableContract
) -> tuple[int | None, list[CheckResult]]:
    """Todos los checks de una tabla; (None, [falla]) si la tabla no existe."""
    uri = locator.uri(contract)
    df = read_optional(spark, locator, contract)
    if df is None:
        return None, [_result(contract, "existe", "la tabla existe", 1, "no existe; corre su etapa")]
    results = [_result(contract, "existe", "la tabla existe", 0)]
    results += schema_checks(contract, df)
    results.append(privacy_check(contract, df))
    results.append(metadata_check(spark, uri, contract, df))
    total, rows = row_checks(contract, df)
    results += rows
    results += unique_checks(contract, df, total)
    results += reference_checks(spark, locator, contract, df)
    return total, results


def _cross(name: str, left: int, right: int, formula: str, severity: str = ERROR) -> CheckResult:
    diff = abs(left - right)
    return CheckResult(
        "entre capas", "reconciliacion", name, severity, diff == 0, diff, f"{formula}: {left} vs {right}"
    )


def reconciliation_checks(
    spark: SparkSession, locator: TableLocator, counts: dict[str, int]
) -> list[CheckResult]:
    """Cada fila tiene destino: landing > Bronce > Plata > Oro, con las mismas igualdades que F3 y mas."""
    from pyspark.sql import functions as F

    needed = (BRONZE_INGESTION_LOG, BRONZE_SPOTIFY_STREAMS, SILVER_PLAYS, SILVER_ARTISTS, GOLD_FACT_PLAYS)
    if any(counts.get(t.qualified_name) is None for t in needed):
        return [
            CheckResult(
                "entre capas", "reconciliacion", "tablas para reconciliar", ERROR, False, 1, "faltan tablas"
            )
        ]

    def n(contract):
        return counts.get(contract.qualified_name) or 0

    def read(contract):
        return read_optional(spark, locator, contract)

    latest = (
        read(BRONZE_INGESTION_LOG)
        .groupBy("file_sha256")
        .agg(
            F.max_by("rows_loaded", "ingested_at").alias("loaded"),
            F.max_by("rows_quarantined", "ingested_at").alias("quarantined"),
        )
        .agg(F.sum("loaded"), F.sum("quarantined"))
        .first()
    )
    plays = read(SILVER_PLAYS)
    fact = read(GOLD_FACT_PLAYS)
    daily = read(GOLD_KPI_LISTENING_DAILY)
    checks = [
        _cross(
            "bitacora = bronce",
            latest[0] or 0,
            n(BRONZE_SPOTIFY_STREAMS),
            "sum(rows_loaded) = spotify_streams",
        ),
        _cross(
            "bitacora = cuarentena",
            latest[1] or 0,
            n(BRONZE_QUARANTINE),
            "sum(rows_quarantined) = _quarantine",
        ),
        _cross(
            "bronce = plata + descartes",
            n(BRONZE_SPOTIFY_STREAMS),
            n(SILVER_PLAYS) + n(SILVER_EPISODES) + n(SILVER_DISCARDED),
            "spotify_streams = plays + episodes + _discarded",
        ),
        _cross(
            "artistas de plata = artistas escuchados",
            n(SILVER_ARTISTS),
            plays.where("artist_name IS NOT NULL").select("artist_name").distinct().count(),
            "artists = artist_name distintos en plays",
        ),
        _cross("plata = oro", n(SILVER_PLAYS), n(GOLD_FACT_PLAYS), "plays = fact_plays"),
        _cross(
            "dim_track = canciones de plata",
            n(GOLD_DIM_TRACK),
            plays.select("track_uri").distinct().count(),
            "dim_track = track_uri distintos en plays",
        ),
    ]
    if daily is not None:
        checks.append(
            _cross(
                "kpi diario = fact con fecha",
                fact.where("date_key IS NOT NULL").count(),
                daily.agg(F.sum("plays")).first()[0] or 0,
                "fact_plays con date_key = suma de plays diarios",
            )
        )
    return checks


def last_data_change(spark: SparkSession, uri: str) -> datetime | None:
    """Momento del ultimo commit que cambio datos (ignora cambios de metadatos y OPTIMIZE)."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    history = DeltaTable.forPath(spark, uri).history()
    return history.where(F.col("operation").isin(*DATA_OPERATIONS)).agg(F.max("timestamp")).first()[0]


def freshness_checks(spark: SparkSession, locator: TableLocator, now: datetime) -> list[CheckResult]:
    """Avisos: ingesta vieja o una capa que no se recalculo despues de cambiar la anterior."""
    from pyspark.sql import functions as F

    results = []
    log_df = read_optional(spark, locator, BRONZE_INGESTION_LOG)
    if log_df is not None:
        # Epoch en segundos: no depende de la zona horaria del proceso de Python.
        last = log_df.agg(F.max(F.unix_timestamp("ingested_at"))).first()[0]
        age = now - datetime.fromtimestamp(last, UTC) if last is not None else None
        stale = age is None or age > MAX_INGESTION_AGE
        days = "sin ingestas" if age is None else f"hace {age.days} dias (umbral {MAX_INGESTION_AGE.days})"
        results.append(
            CheckResult(
                "bronze._ingestion_log", "frescura", "ingesta reciente", WARN, not stale, int(stale), days
            )
        )
    pairs = (
        (BRONZE_SPOTIFY_STREAMS, SILVER_PLAYS, "plata al dia con bronce"),
        (SILVER_PLAYS, GOLD_KPI_LISTENING_DAILY, "oro al dia con plata"),
    )
    for upstream, downstream, name in pairs:
        if (
            read_optional(spark, locator, upstream) is None
            or read_optional(spark, locator, downstream) is None
        ):
            continue
        up = last_data_change(spark, locator.uri(upstream))
        down = last_data_change(spark, locator.uri(downstream))
        stale = up is not None and (down is None or down < up)
        detail = f"{upstream.qualified_name} cambio despues de {downstream.qualified_name}" if stale else ""
        table = f"{upstream.qualified_name} > {downstream.qualified_name}"
        results.append(CheckResult(table, "frescura", name, WARN, not stale, int(stale), detail))
    return results


def run_quality(
    spark: SparkSession,
    locator: TableLocator,
    contracts: tuple[TableContract, ...] = DELTA_TABLES,
    now: datetime | None = None,
    cross_layer: bool = True,
) -> QualityRun:
    """Corre los checks; cross_layer agrega reconciliacion y frescura. No lanza error por fallas."""
    run = QualityRun()
    for contract in contracts:
        total, results = table_checks(spark, locator, contract)
        run.row_counts[contract.qualified_name] = total
        run.results += results
    if cross_layer:
        # La reconciliacion necesita los conteos de todas las capas, no solo las pedidas.
        for contract in DELTA_TABLES:
            if contract.qualified_name not in run.row_counts:
                df = read_optional(spark, locator, contract)
                run.row_counts[contract.qualified_name] = None if df is None else df.count()
        run.results += reconciliation_checks(spark, locator, run.row_counts)
        run.results += freshness_checks(spark, locator, now or datetime.now(UTC))
    log.info(
        "Calidad: %d checks en %d tablas, %d fallas (error), %d avisos",
        len(run.results),
        len(contracts),
        len(run.errors),
        len(run.warnings),
    )
    for r in run.errors + run.warnings:
        (log.error if r.severity == ERROR else log.warning)(
            "[%s] %s: %s (%s)", r.severity.upper(), r.table, r.check, r.detail
        )
    return run
