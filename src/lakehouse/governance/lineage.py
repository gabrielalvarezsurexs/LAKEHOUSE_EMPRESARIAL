"""Linaje: de que se alimenta cada tabla (sources del contrato) y de donde sale cada columna clave.

El grafo tabla a tabla se arma solo desde los contratos; aqui viven los nodos que no son tablas Delta (fuentes
externas, la cache de MusicBrainz y los consumidores) y el linaje de columnas clave. Salida: Mermaid.
Responsable: Victor Gabriel Torres Guerrero (Oficial de Gobernanza y Seguridad)
"""

from __future__ import annotations

from dataclasses import dataclass

from lakehouse.governance.contracts import (
    ALL_TABLES,
    GOLD_ML_SKIP_FEATURES,
    GOLD_ML_SKIP_PREDICTIONS,
    GOLD_TABLES,
    ML_MODEL_NODE,
)


@dataclass(frozen=True)
class Node:
    """Nodo del grafo: group es la capa (landing, bronze, silver, gold) o ext / consumer."""

    id: str
    group: str
    label: str
    sources: tuple[str, ...] = ()


_KPIS = tuple(t.qualified_name for t in GOLD_TABLES if t.name.startswith("kpi_"))
_DIMS = tuple(t.qualified_name for t in GOLD_TABLES if t.name.startswith("dim_"))

EXTRA_NODES = (
    Node("ext.spotify", "ext", "Export de Spotify (JSON)"),
    Node("ext.musicbrainz", "ext", "API de MusicBrainz"),
    Node("ext.platform_catalog", "ext", "Catalogo fijo de plataformas"),
    # Los nombres a consultar salen de Bronce; las respuestas crudas se guardan en landing (D9).
    Node(
        "landing.musicbrainz_cache",
        "landing",
        "musicbrainz cache (JSON)",
        ("ext.musicbrainz", "bronze.spotify_streams"),
    ),
    # El modelo se entrena con las features y lo usa la inferencia por lote (F6).
    Node(ML_MODEL_NODE, "ml", "skip_model (MLflow)", (GOLD_ML_SKIP_FEATURES.qualified_name,)),
    Node(
        "consumer.bi",
        "consumer",
        "Tablero Superset (F7)",
        _KPIS + _DIMS + (GOLD_ML_SKIP_PREDICTIONS.qualified_name,),
    ),
)

GROUP_TITLES = {
    "ext": "Fuentes",
    "landing": "landing (crudo)",
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "ml": "ML (MLflow)",
    "consumer": "Consumo",
}


def nodes() -> list[Node]:
    """Todos los nodos: tablas de los contratos mas los extra, en orden del flujo."""
    table_nodes = [Node(t.qualified_name, t.layer, t.name, t.sources) for t in ALL_TABLES]
    extra = list(EXTRA_NODES)
    order = list(GROUP_TITLES)
    return sorted(table_nodes + extra, key=lambda n: order.index(n.group))


def edges() -> list[tuple[str, str]]:
    return [(src, n.id) for n in nodes() for src in n.sources]


def unknown_sources() -> list[str]:
    """Sources que no apuntan a ningun nodo (deberia estar vacio; lo revisa una prueba)."""
    ids = {n.id for n in nodes()}
    return sorted({src for src, _ in edges() if src not in ids})


def _mermaid_id(node_id: str) -> str:
    return node_id.replace(".", "_")


def mermaid() -> str:
    """Grafo tabla a tabla en Mermaid, agrupado por capa."""
    lines = ["flowchart LR"]
    for group, title in GROUP_TITLES.items():
        members = [n for n in nodes() if n.group == group]
        if not members:
            continue
        lines.append(f'  subgraph {group}["{title}"]')
        for n in members:
            shape = f'(["{n.label}"])' if group in ("ext", "consumer") else f'["{n.label}"]'
            lines.append(f"    {_mermaid_id(n.id)}{shape}")
        lines.append("  end")
    lines += [f"  {_mermaid_id(a)} --> {_mermaid_id(b)}" for a, b in edges()]
    return "\n".join(lines)


def render_lineage() -> str:
    """LINAJE.md: grafo tabla a tabla y linaje de columnas clave (acentos como escapes: codigo ASCII)."""
    lines = [
        "# Linaje",
        "",
        "> Generado desde los contratos (`sources`) y `src/lakehouse/governance/lineage.py` con",
        "> `uv run python -m lakehouse.governance docs`. No se edita a mano. En Dagster (F5) el mismo grafo",
        "> sale de los assets.",
        "",
        "## Tabla a tabla",
        "",
        "```mermaid",
        mermaid(),
        "```",
        "",
        "## Columnas clave",
        "",
        "| Columna destino | Viene de | C\u00f3mo |",
        "|---|---|---|",
    ]
    for c in COLUMN_LINEAGE:
        sources = "<br>".join(f"`{s}`" for s in c.sources)
        lines.append(f"| `{c.target}` | {sources} | {c.how} |")
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class ColumnLineage:
    target: str  # capa.tabla.columna
    sources: tuple[str, ...]
    how: str


COLUMN_LINEAGE = (
    ColumnLineage(
        "bronze.spotify_streams.ip_addr_hash",
        ("landing.spotify_streams_source.ip_addr",),
        "HMAC-SHA256 con la sal IP_HASH_SALT; la IP en claro no sale de landing (D8)",
    ),
    ColumnLineage(
        "silver.plays.(sin IP)",
        ("bronze.spotify_streams.ip_addr_hash",),
        "Se elimina: ninguna tabla de Plata u Oro tiene columnas de IP",
    ),
    ColumnLineage(
        "silver.plays.play_id",
        (
            "bronze.spotify_streams.ts",
            "bronze.spotify_streams.spotify_track_uri",
            "bronze.spotify_streams.ms_played",
        ),
        "SHA-256 de la llave de deduplicacion",
    ),
    ColumnLineage(
        "silver.plays.ts_start",
        ("bronze.spotify_streams.ts", "bronze.spotify_streams.ms_played"),
        "ts (fin) menos ms_played",
    ),
    ColumnLineage(
        "silver.plays.local_date / local_hour / local_weekday",
        ("bronze.spotify_streams.ts", "bronze.spotify_streams.conn_country"),
        "Zona IANA del pais (o de la sesion si el pais es ZZ) aplicada al inicio",
    ),
    ColumnLineage(
        "silver.plays.platform_family",
        ("bronze.spotify_streams.platform",),
        "Primer patron que coincide (transformation/catalogs.py)",
    ),
    ColumnLineage(
        "silver.plays.session_id",
        ("bronze.spotify_streams.ts", "bronze.spotify_streams.ms_played"),
        "Nueva sesion tras mas de 30 min sin escuchar",
    ),
    ColumnLineage(
        "silver.plays.is_skip",
        ("bronze.spotify_streams.reason_end", "bronze.spotify_streams.ms_played"),
        "fwdbtn/backbtn = skip; trackdone = no; endplay/playbtn segun 30 s; otro = null",
    ),
    ColumnLineage(
        "silver.plays.offline_at",
        ("bronze.spotify_streams.offline_timestamp",),
        "Epoch en segundos o milisegundos normalizado a UTC (H-003)",
    ),
    ColumnLineage(
        "silver.artists.primary_genre",
        ("bronze.musicbrainz_artists.genres", "bronze.musicbrainz_artists.status"),
        "Genero con mas votos, solo si hubo match confiable",
    ),
    ColumnLineage(
        "gold.fact_plays.date_key",
        ("silver.plays.local_date",),
        "Fecha local como AAAAMMDD (dim_date)",
    ),
    ColumnLineage(
        "gold.fact_plays.artist_key",
        ("silver.plays.artist_name",),
        "Primeros 16 hex del SHA-256 del nombre (dim_artist)",
    ),
    ColumnLineage("gold.fact_plays.track_key", ("silver.plays.track_uri",), "URI de la cancion (dim_track)"),
    ColumnLineage(
        "gold.kpi_listening_daily.skip_rate",
        ("gold.fact_plays.is_skip",),
        "skips / reproducciones etiquetadas por dia local",
    ),
    ColumnLineage(
        "gold.kpi_genres_monthly.genre",
        ("gold.dim_artist.primary_genre", "gold.fact_plays.artist_key"),
        "Genero principal del artista de cada reproduccion",
    ),
    ColumnLineage(
        "gold.kpi_artist_retention.retention_rate",
        ("gold.fact_plays.artist_key", "gold.fact_plays.date_key"),
        "Artistas de la cohorte escuchados N meses despues / tamano de la cohorte",
    ),
    ColumnLineage(
        "gold.ml_skip_features.track_prior_skip_rate",
        ("gold.fact_plays.is_skip", "gold.fact_plays.track_key", "gold.fact_plays.ts_start"),
        "Skips / etiquetadas de la misma cancion que empezaron antes (sin fuga)",
    ),
    ColumnLineage(
        "gold.ml_skip_predictions.skip_probability",
        ("gold.ml_skip_features.(todas las features)",),
        f"Modelo registrado en MLflow ({ML_MODEL_NODE}) aplicado por lote a cada fila",
    ),
)
