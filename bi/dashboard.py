"""Tablero de Superset definido como codigo: datasets (bi/sql), graficas, filtros y acomodo (D26).

Genera el export de Superset (ZIP con YAML) que el contenedor importa al arrancar; asi el tablero vive en el
repo. Corre igual en local (copia para ENTREGABLES) y en el contenedor de Superset: solo usa stdlib y PyYAML.
Uso: python bi/dashboard.py --out <archivo.zip>
Responsable: Emilio Rendon Trias (Analista de Datos / BI)
"""

from __future__ import annotations

import argparse
import json
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

SQL_DIR = Path(__file__).resolve().parent / "sql"
SLUG = "spotify-lakehouse"
TITLE = "Historial de Spotify: lakehouse"
DATABASE_NAME = "Lakehouse (Oro)"
# DuckDB en memoria: cada conexion lee Oro de MinIO con delta_scan (el secreto lo pone superset_config.py).
DATABASE_URI = "duckdb:///:memory:"
ROOT = "lakehouse_bi"  # carpeta raiz dentro del ZIP (Superset la ignora)
FIXED_ZIP_TIME = (2026, 11, 1, 0, 0, 0)  # el ZIP sale igual byte a byte si el tablero no cambia

PCT = ".1%"
NUM = ",.0f"
HOURS = ",.1f"


def stable_uuid(kind: str, key: str) -> str:
    """UUID fijo por objeto: reimportar actualiza el mismo tablero en vez de duplicarlo."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"lakehouse-bi/{kind}/{key}"))


@dataclass(frozen=True)
class Metric:
    name: str
    expression: str
    label: str
    d3format: str = NUM


@dataclass(frozen=True)
class Dataset:
    """Dataset virtual de Superset: SQL de bi/sql/<name>.sql sobre Oro; columnas y tipos segun DuckDB."""

    name: str
    dttm: str  # columna de fecha que usa el filtro de fecha
    columns: dict[str, str]  # nombre -> tipo DuckDB (una prueba los compara con lo que da el SQL)
    metrics: tuple[Metric, ...]

    @property
    def uuid(self) -> str:
        return stable_uuid("dataset", self.name)

    def sql(self) -> str:
        """SQL del archivo sin el encabezado de comentarios (Superset lo envuelve como subconsulta)."""
        lines = (SQL_DIR / f"{self.name}.sql").read_text(encoding="utf-8").splitlines()
        return "\n".join(line for line in lines if not line.startswith("--")).strip()

    def metric(self, name: str) -> Metric:
        for m in self.metrics:
            if m.name == name:
                return m
        raise KeyError(f"El dataset {self.name} no tiene la metrica {name}")


SKIP_RATE = "AVG(CASE WHEN is_skip THEN 1.0 WHEN NOT is_skip THEN 0.0 END)"
HOURS_EXPR = "SUM(minutes_played) / 60.0"

DATASETS = {
    d.name: d
    for d in (
        Dataset(
            "plays",
            "date",
            {
                "date": "DATE",
                "year_month": "VARCHAR",
                "weekday_label": "VARCHAR",
                "hour_label": "VARCHAR",
                "artist_name": "VARCHAR",
                "genre": "VARCHAR",
                "track_name": "VARCHAR",
                "platform_family": "VARCHAR",
                "device_type": "VARCHAR",
                "conn_country": "VARCHAR",
                "session_id": "VARCHAR",
                "minutes_played": "DOUBLE",
                "is_skip": "BOOLEAN",
                "shuffle": "BOOLEAN",
                "reason_start": "VARCHAR",
            },
            (
                Metric("hours", HOURS_EXPR, "Horas", HOURS),
                Metric("plays", "COUNT(*)", "Reproducciones"),
                Metric("artists", "COUNT(DISTINCT artist_name)", "Artistas distintos"),
                Metric("sessions", "COUNT(DISTINCT session_id)", "Sesiones"),
                Metric("skip_rate", SKIP_RATE, "Tasa de skip", PCT),
            ),
        ),
        Dataset(
            "listening_daily",
            "date",
            {
                "date": "DATE",
                "plays": "BIGINT",
                "minutes_played": "DOUBLE",
                "distinct_tracks": "BIGINT",
                "distinct_artists": "BIGINT",
                "sessions": "BIGINT",
                "labeled_plays": "BIGINT",
                "skips": "BIGINT",
            },
            (
                Metric("hours", HOURS_EXPR, "Horas", HOURS),
                Metric("sessions", "SUM(sessions)", "Sesiones"),
                # Tasa del periodo (suma de skips entre suma de etiquetadas), no promedio de tasas diarias.
                Metric("skip_rate", "SUM(skips) / NULLIF(SUM(labeled_plays), 0)", "Tasa de skip", PCT),
            ),
        ),
        Dataset(
            "top_artists_monthly",
            "month",
            {
                "month": "DATE",
                "year_month": "VARCHAR",
                "artist_name": "VARCHAR",
                "genre": "VARCHAR",
                "plays": "BIGINT",
                "hours": "DOUBLE",
                "rank_in_month": "INTEGER",
            },
            (Metric("hours", "SUM(hours)", "Horas", HOURS),),
        ),
        Dataset(
            "genres_monthly",
            "month",
            {
                "month": "DATE",
                "year_month": "VARCHAR",
                "genre": "VARCHAR",
                "plays": "BIGINT",
                "minutes_played": "DOUBLE",
                "artists": "BIGINT",
            },
            (Metric("hours", HOURS_EXPR, "Horas", HOURS),),
        ),
        Dataset(
            "artist_retention",
            "cohort_start",
            {
                "cohort_start": "DATE",
                "cohort_month": "VARCHAR",
                "months_since": "INTEGER",
                "month_label": "VARCHAR",
                "cohort_size": "BIGINT",
                "artists_active": "BIGINT",
            },
            (
                # Suma de activos entre suma de cohortes: vale por celda y al juntar cohortes.
                Metric("retention", "SUM(artists_active) * 1.0 / SUM(cohort_size)", "Retenci\u00f3n", PCT),
            ),
        ),
        Dataset(
            "skip_predictions",
            "date",
            {
                "date": "DATE",
                "split": "VARCHAR",
                "is_skip": "BOOLEAN",
                "skip_probability": "DOUBLE",
                "baseline_probability": "DOUBLE",
                "predicted_skip": "BOOLEAN",
                "model_version": "VARCHAR",
                "platform_family": "VARCHAR",
                "genre": "VARCHAR",
            },
            (
                Metric("plays", "COUNT(*)", "Reproducciones"),
                Metric("real_skip_rate", SKIP_RATE, "Skip real", PCT),
                Metric("model_probability", "AVG(skip_probability)", "Modelo", PCT),
                Metric("baseline_probability", "AVG(baseline_probability)", "Baseline", PCT),
                Metric(
                    "accuracy",
                    "AVG(CASE WHEN predicted_skip = is_skip THEN 1.0 WHEN is_skip IS NOT NULL THEN 0.0 END)",
                    "Aciertos",
                    PCT,
                ),
            ),
        ),
    )
}


@dataclass(frozen=True)
class Chart:
    """Una grafica: lo minimo para armar sus params de Superset y su consulta de prueba."""

    key: str
    title: str
    viz: str  # big_number, line, bar, area, pie, table, raw_table, heatmap
    dataset: str
    metrics: tuple[str, ...] = ()
    groupby: tuple[str, ...] = ()
    x_axis: str | None = None
    time_grain: str | None = None  # P1W, P1M (solo si x_axis es la fecha)
    where: str | None = None  # filtro SQL fijo de la grafica
    columns: tuple[str, ...] = ()  # raw_table
    order_by: tuple[str, bool] | None = None  # raw_table: (columna, ascendente)
    row_limit: int = 10000
    fmt: str | None = None
    description: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def uuid(self) -> str:
        return stable_uuid("chart", self.key)

    def all_columns(self) -> list[str]:
        """Columnas que la grafica agrupa o muestra (para validarlas contra el dataset)."""
        cols = list(self.columns) + list(self.groupby)
        return cols + ([self.x_axis] if self.x_axis else [])


CHARTS = (
    Chart("kpi_hours", "Horas escuchadas", "big_number", "plays", ("hours",), fmt=",.0f"),
    Chart("kpi_plays", "Reproducciones", "big_number", "plays", ("plays",), fmt=NUM),
    Chart("kpi_artists", "Artistas distintos", "big_number", "plays", ("artists",), fmt=NUM),
    Chart(
        "kpi_skip",
        "Tasa de skip",
        "big_number",
        "plays",
        ("skip_rate",),
        fmt=PCT,
        description="Solo reproducciones con etiqueta (las que el usuario termin\u00f3 o salt\u00f3).",
    ),
    Chart("hours_by_month", "Horas por mes", "bar", "plays", ("hours",), x_axis="date", time_grain="P1M"),
    Chart("platforms", "Horas por plataforma", "pie", "plays", ("hours",), groupby=("platform_family",)),
    Chart(
        "top_artists",
        "Top 10 artistas",
        "table",
        "plays",
        ("hours", "plays", "skip_rate"),
        groupby=("artist_name", "genre"),
        row_limit=10,
    ),
    Chart(
        "top_genres",
        "G\u00e9neros m\u00e1s escuchados",
        "pie",
        "plays",
        ("hours",),
        groupby=("genre",),
        row_limit=10,
        description="'sin genero': artistas sin g\u00e9nero en MusicBrainz (H-004); no se inventa.",
    ),
    Chart(
        "genres_by_month",
        "G\u00e9neros por mes (top 8)",
        "area",
        "genres_monthly",
        ("hours",),
        groupby=("genre",),
        x_axis="month",
        time_grain="P1M",
    ),
    Chart(
        "artist_of_month",
        "Artista #1 de cada mes",
        "raw_table",
        "top_artists_monthly",
        where="rank_in_month = 1",
        columns=("year_month", "artist_name", "genre", "hours"),
        order_by=("year_month", False),
        row_limit=200,
    ),
    Chart(
        "habits",
        "H\u00e1bitos: reproducciones por hora y d\u00eda",
        "heatmap",
        "plays",
        ("plays",),
        groupby=("weekday_label",),
        x_axis="hour_label",
        description="Hora y d\u00eda locales (zona del pa\u00eds de conexi\u00f3n).",
    ),
    Chart(
        "retention",
        "Retenci\u00f3n de artistas por cohorte",
        "heatmap",
        "artist_retention",
        ("retention",),
        groupby=("cohort_month",),
        x_axis="month_label",
        fmt=PCT,
        description="De los artistas descubiertos en un mes, qu\u00e9 parte se escucha N meses despu\u00e9s.",
    ),
    Chart(
        "weekly_trend",
        "Tendencia semanal: horas y sesiones",
        "line",
        "listening_daily",
        ("hours", "sessions"),
        x_axis="date",
        time_grain="P1W",
    ),
    Chart(
        "model_vs_real",
        "Skip real vs modelo vs baseline (periodo de prueba)",
        "line",
        "skip_predictions",
        ("real_skip_rate", "model_probability", "baseline_probability"),
        x_axis="date",
        time_grain="P1M",
        where="split = 'test'",
        fmt=PCT,
    ),
    Chart(
        "model_by_split",
        "Modelo por conjunto",
        "table",
        "skip_predictions",
        ("plays", "real_skip_rate", "model_probability", "accuracy"),
        groupby=("split",),
        row_limit=10,
    ),
)

CHARTS_BY_KEY = {c.key: c for c in CHARTS}

INTRO = (
    "**Historial de Spotify de un integrante, servido desde la capa Oro del lakehouse** "
    "(Delta en MinIO, le\u00eddo con DuckDB).\n\nFiltros arriba: fecha, g\u00e9nero y plataforma. "
    "Las gr\u00e1ficas de un mart que no tiene la columna del filtro lo ignoran (Superset lo marca). "
    "'sin genero' = artista sin g\u00e9nero en MusicBrainz."
)

# Filas del tablero: (llave de grafica, ancho en columnas de 12) y alto por fila.
LAYOUT = (
    ((("intro", 12),), 18),
    ((("kpi_hours", 3), ("kpi_plays", 3), ("kpi_artists", 3), ("kpi_skip", 3)), 25),
    ((("hours_by_month", 8), ("platforms", 4)), 55),
    ((("top_artists", 6), ("top_genres", 6)), 55),
    ((("genres_by_month", 8), ("artist_of_month", 4)), 60),
    ((("habits", 6), ("retention", 6)), 70),
    ((("weekly_trend", 12),), 50),
    ((("model_vs_real", 8), ("model_by_split", 4)), 55),
)


def adhoc_filters(chart: Chart) -> list[dict]:
    """Filtro de fecha (lo usa el filtro nativo) mas el filtro SQL fijo de la grafica."""
    filters = [
        {
            "clause": "WHERE",
            "comparator": "No filter",
            "expressionType": "SIMPLE",
            "operator": "TEMPORAL_RANGE",
            "subject": DATASETS[chart.dataset].dttm,
        }
    ]
    if chart.where:
        filters.append({"clause": "WHERE", "expressionType": "SQL", "sqlExpression": chart.where})
    return filters


AREA_SERIES = 8  # series (generos) que muestra la grafica de area; el resto no se dibuja


def _timeseries(chart: Chart) -> dict:
    return {
        "x_axis": chart.x_axis,
        "time_grain_sqla": chart.time_grain,
        "metrics": list(chart.metrics),
        "groupby": list(chart.groupby),
        "row_limit": chart.row_limit,
        "order_desc": True,
        "show_legend": True,
        "legendOrientation": "top",
        "legendType": "scroll",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "x_axis_time_format": "smart_date",
        "x_axis_sort_asc": True,
        "truncateXAxis": True,
        "y_axis_bounds": [None, None],
        "y_axis_format": chart.fmt or "SMART_NUMBER",
        "color_scheme": "supersetColors",
        "annotation_layers": [],
        "comparison_type": "values",
        "only_total": True,
    }


def chart_params(chart: Chart) -> dict:
    """params de Superset 6.1 por tipo (mismas llaves que los ejemplos oficiales)."""
    base = {"datasource": "0__table", "adhoc_filters": adhoc_filters(chart), "extra_form_data": {}}
    if chart.viz == "big_number":
        specific = {
            "viz_type": "big_number_total",
            "metric": chart.metrics[0],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "time_format": "smart_date",
            "y_axis_format": chart.fmt or "SMART_NUMBER",
        }
    elif chart.viz in ("line", "bar"):
        specific = {"viz_type": f"echarts_timeseries_{chart.viz}", **_timeseries(chart)}
        if chart.viz == "line":
            specific.update({"seriesType": "line", "markerEnabled": True, "markerSize": 6})
        else:
            specific["orientation"] = "vertical"
    elif chart.viz == "area":
        specific = {
            "viz_type": "echarts_area",
            **_timeseries(chart),
            "stack": "Stack",
            "opacity": 0.4,
            "limit": AREA_SERIES,
            "timeseries_limit_metric": chart.metrics[0],
        }
    elif chart.viz == "pie":
        specific = {
            "viz_type": "pie",
            "metric": chart.metrics[0],
            "groupby": list(chart.groupby),
            "row_limit": chart.row_limit,
            "sort_by_metric": True,
            "show_labels": True,
            "label_type": "key_percent",
            "labels_outside": True,
            "show_labels_threshold": 5,
            "show_legend": True,
            "legendOrientation": "top",
            "legendType": "scroll",
            "donut": True,
            "innerRadius": 30,
            "outerRadius": 70,
            "number_format": chart.fmt or "SMART_NUMBER",
            "date_format": "smart_date",
            "color_scheme": "supersetColors",
        }
    elif chart.viz == "table":
        specific = {
            "viz_type": "table",
            "query_mode": "aggregate",
            "groupby": list(chart.groupby),
            "metrics": list(chart.metrics),
            "all_columns": [],
            "percent_metrics": [],
            "timeseries_limit_metric": chart.metrics[0],
            "order_desc": True,
            "row_limit": chart.row_limit,
            "server_page_length": 10,
            "show_cell_bars": True,
            "color_pn": False,
            "table_timestamp_format": "smart_date",
        }
    elif chart.viz == "raw_table":
        column, ascending = chart.order_by or (chart.columns[0], True)
        specific = {
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": list(chart.columns),
            "order_by_cols": [json.dumps([column, ascending])],
            "groupby": [],
            "percent_metrics": [],
            "row_limit": chart.row_limit,
            "server_page_length": 12,
            "show_cell_bars": False,
            "table_timestamp_format": "smart_date",
        }
    elif chart.viz == "heatmap":
        specific = {
            "viz_type": "heatmap_v2",
            "x_axis": chart.x_axis,
            "groupby": chart.groupby[0],
            "metric": chart.metrics[0],
            "row_limit": chart.row_limit,
            "linear_color_scheme": "superset_seq_1",
            "normalize_across": "heatmap",
            "legend_type": "continuous",
            "show_legend": True,
            "show_values": False,
            "show_percentage": False,
            "sort_x_axis": "alpha_asc",
            "sort_y_axis": "alpha_asc",
            "xscale_interval": -1,
            "yscale_interval": -1,
            "left_margin": "auto",
            "bottom_margin": "auto",
            "value_bounds": [None, None],
            "y_axis_format": chart.fmt or "SMART_NUMBER",
        }
    else:
        raise ValueError(f"Tipo de grafica desconocido: {chart.viz}")
    return {**base, **specific, **chart.extra}


def query_columns(chart: Chart) -> list:
    """Como las arma la UI de Superset 6: el eje de tiempo va como BASE_AXIS con su corte (mes, semana)."""
    if not chart.time_grain:
        return chart.all_columns()
    axis = {
        "timeGrain": chart.time_grain,
        "columnType": "BASE_AXIS",
        "sqlExpression": chart.x_axis,
        "label": chart.x_axis,
        "expressionType": "SQL",
    }
    return [axis if c == chart.x_axis else c for c in chart.all_columns()]


def query_context(chart: Chart) -> str:
    """Consulta guardada de la grafica: la usa bi/check.py (GET /api/v1/chart/<id>/data/) para probarla."""
    query = {
        "columns": query_columns(chart),
        "metrics": list(chart.metrics),
        "filters": [],
        "extras": {"where": chart.where or "", "having": ""},
        "row_limit": chart.row_limit,
        "orderby": [[chart.metrics[0], False]] if chart.metrics else [],
    }
    if chart.viz == "raw_table" and chart.order_by:
        query["orderby"] = [list(chart.order_by)]
    if chart.viz == "area":
        query.update(
            {
                "series_columns": list(chart.groupby),  # sin esto Superset ignora el limite
                "series_limit": AREA_SERIES,
                "series_limit_metric": chart.metrics[0],
            }
        )
    context = {
        "datasource": {"id": 0, "type": "table"},
        "force": False,
        "queries": [query],
        "form_data": chart_params(chart),
        "result_format": "json",
        "result_type": "full",
    }
    return json.dumps(context)


def test_query(chart: Chart, dataset_sql: str) -> str:
    """Equivalente SQL de la grafica (sin cortes de fecha) para probar metricas y columnas en DuckDB."""
    ds = DATASETS[chart.dataset]
    cols = chart.all_columns()
    selects = cols + [f"{ds.metric(m).expression} AS {m}" for m in chart.metrics]
    sql = f"SELECT {', '.join(selects)} FROM ({dataset_sql}) AS virtual_table"
    if chart.where:
        sql += f" WHERE {chart.where}"
    if chart.metrics and cols:
        sql += f" GROUP BY {', '.join(cols)}"
    return sql + f" LIMIT {chart.row_limit}"


def native_filters() -> list[dict]:
    """Filtros del tablero: fecha (todas las graficas), genero y plataforma (las que tienen la columna)."""

    def base(key: str, name: str, filter_type: str) -> dict:
        return {
            "id": f"NATIVE_FILTER-{key}",
            "name": name,
            "filterType": filter_type,
            "type": "NATIVE_FILTER",
            "description": "",
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "controlValues": {"enableEmptyFilter": False},
            "targets": [{}],
        }

    def select(key: str, name: str, column: str) -> dict:
        f = base(key, name, "filter_select")
        f["targets"] = [{"datasetUuid": DATASETS["plays"].uuid, "column": {"name": column}}]
        f["controlValues"].update(
            {
                "multiSelect": True,
                "searchAllOptions": False,
                "inverseSelection": False,
                "defaultToFirstItem": False,
            }
        )
        return f

    return [
        base("fecha", "Fecha", "filter_time"),
        select("genero", "G\u00e9nero", "genre"),
        select("plataforma", "Plataforma", "platform_family"),
    ]


def position() -> dict:
    """Acomodo del tablero (rejilla de 12 columnas)."""
    nodes: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TITLE}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    chart_ids = {c.key: i + 1 for i, c in enumerate(CHARTS)}
    for row_index, (items, height) in enumerate(LAYOUT):
        row_id = f"ROW-{row_index}"
        nodes["GRID_ID"]["children"].append(row_id)
        row = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for key, width in items:
            parents = ["ROOT_ID", "GRID_ID", row_id]
            if key == "intro":
                node = {
                    "type": "MARKDOWN",
                    "id": "MARKDOWN-intro",
                    "children": [],
                    "parents": parents,
                    "meta": {"width": width, "height": height, "code": INTRO},
                }
            else:
                chart = CHARTS_BY_KEY[key]
                node = {
                    "type": "CHART",
                    "id": f"CHART-{key}",
                    "children": [],
                    "parents": parents,
                    "meta": {
                        "chartId": chart_ids[key],
                        "uuid": chart.uuid,
                        "sliceName": chart.title,
                        "width": width,
                        "height": height,
                    },
                }
            row["children"].append(node["id"])
            nodes[node["id"]] = node
        nodes[row_id] = row
    return nodes


def database_yaml() -> dict:
    return {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": DATABASE_URI,
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_csv_upload": False,
        "extra": {"allows_virtual_table_explore": True},
        "uuid": stable_uuid("database", "lakehouse"),
        "version": "1.0.0",
    }


def dataset_yaml(ds: Dataset) -> dict:
    return {
        "table_name": ds.name,
        "main_dttm_col": ds.dttm,
        "description": f"Virtual sobre Oro: bi/sql/{ds.name}.sql",
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "schema": "main",
        "sql": ds.sql(),
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "uuid": ds.uuid,
        "metrics": [
            {
                "metric_name": m.name,
                "verbose_name": m.label,
                "metric_type": None,
                "expression": m.expression,
                "description": None,
                "d3format": m.d3format,
                "extra": None,
                "warning_text": None,
            }
            for m in ds.metrics
        ],
        "columns": [
            {
                "column_name": name,
                "verbose_name": None,
                "is_dttm": col_type in ("DATE", "TIMESTAMP"),
                "is_active": True,
                "type": col_type,
                "groupby": True,
                "filterable": True,
                "expression": None,
                "description": None,
                "python_date_format": None,
                "extra": None,
            }
            for name, col_type in ds.columns.items()
        ],
        "version": "1.0.0",
        "database_uuid": stable_uuid("database", "lakehouse"),
    }


def chart_yaml(chart: Chart) -> dict:
    return {
        "slice_name": chart.title,
        "description": chart.description or None,
        "certified_by": None,
        "certification_details": None,
        "viz_type": chart_params(chart)["viz_type"],
        "params": chart_params(chart),
        "query_context": query_context(chart),
        "cache_timeout": None,
        "uuid": chart.uuid,
        "version": "1.0.0",
        "dataset_uuid": DATASETS[chart.dataset].uuid,
    }


def dashboard_yaml() -> dict:
    return {
        "dashboard_title": TITLE,
        "description": None,
        "css": "",
        "slug": SLUG,
        "certified_by": None,
        "certification_details": None,
        "published": True,
        "uuid": stable_uuid("dashboard", SLUG),
        "position": position(),
        "metadata": {
            "color_scheme": "supersetColors",
            "cross_filters_enabled": True,
            "native_filter_configuration": native_filters(),
            "chart_configuration": {},
            "global_chart_configuration": {},
            "expanded_slices": {},
            "timed_refresh_immune_slices": [],
            "refresh_frequency": 0,
            "default_filters": "{}",
            "label_colors": {},
            "shared_label_colors": [],
            "color_scheme_domain": [],
        },
        "version": "1.0.0",
    }


def bundle() -> dict[str, str]:
    """Archivos del export de Superset: ruta dentro del ZIP -> YAML."""
    db_folder = "Lakehouse_Oro"

    def dump(doc: dict) -> str:
        return yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=120)

    files = {
        "metadata.yaml": dump(
            {"version": "1.0.0", "type": "Dashboard", "timestamp": "2026-11-01T00:00:00+00:00"}
        ),
        f"databases/{db_folder}.yaml": dump(database_yaml()),
    }
    for ds in DATASETS.values():
        files[f"datasets/{db_folder}/{ds.name}.yaml"] = dump(dataset_yaml(ds))
    for chart in CHARTS:
        files[f"charts/{chart.key}.yaml"] = dump(chart_yaml(chart))
    files[f"dashboards/{SLUG}.yaml"] = dump(dashboard_yaml())
    return {f"{ROOT}/{path}": text for path, text in files.items()}


def write_zip(out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, text in bundle().items():
            zf.writestr(zipfile.ZipInfo(path, FIXED_ZIP_TIME), text.encode("utf-8"))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Genera el export del tablero de Superset (ZIP)")
    parser.add_argument("--out", type=Path, required=True, help="Ruta del ZIP a escribir")
    args = parser.parse_args(argv)
    out = write_zip(args.out)
    print(f"Tablero: {len(CHARTS)} graficas, {len(DATASETS)} datasets -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
