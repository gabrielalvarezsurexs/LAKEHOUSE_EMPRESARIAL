# Linaje

> Generado desde los contratos (`sources`) y `src/lakehouse/governance/lineage.py` con
> `uv run python -m lakehouse.governance docs`. No se edita a mano. En Dagster (F5) el mismo grafo
> sale de los assets.

## Tabla a tabla

```mermaid
flowchart LR
  subgraph ext["Fuentes"]
    ext_spotify(["Export de Spotify (JSON)"])
    ext_musicbrainz(["API de MusicBrainz"])
    ext_platform_catalog(["Catalogo fijo de plataformas"])
  end
  subgraph landing["landing (crudo)"]
    landing_spotify_streams_source["spotify_streams_source"]
    landing_musicbrainz_cache["musicbrainz cache (JSON)"]
  end
  subgraph bronze["bronze"]
    bronze_spotify_streams["spotify_streams"]
    bronze__quarantine["_quarantine"]
    bronze__ingestion_log["_ingestion_log"]
    bronze_musicbrainz_artists["musicbrainz_artists"]
  end
  subgraph silver["silver"]
    silver_plays["plays"]
    silver_episodes["episodes"]
    silver__discarded["_discarded"]
    silver_artists["artists"]
  end
  subgraph gold["gold"]
    gold_dim_date["dim_date"]
    gold_dim_artist["dim_artist"]
    gold_dim_track["dim_track"]
    gold_dim_platform["dim_platform"]
    gold_fact_plays["fact_plays"]
    gold_kpi_listening_daily["kpi_listening_daily"]
    gold_kpi_top_artists_monthly["kpi_top_artists_monthly"]
    gold_kpi_genres_monthly["kpi_genres_monthly"]
    gold_kpi_artist_retention["kpi_artist_retention"]
    gold_ml_skip_features["ml_skip_features"]
    gold_ml_skip_predictions["ml_skip_predictions"]
  end
  subgraph ml["ML (MLflow)"]
    ml_skip_model["skip_model (MLflow)"]
  end
  subgraph consumer["Consumo"]
    consumer_bi(["Tablero Superset (F7)"])
  end
  ext_spotify --> landing_spotify_streams_source
  ext_musicbrainz --> landing_musicbrainz_cache
  bronze_spotify_streams --> landing_musicbrainz_cache
  landing_spotify_streams_source --> bronze_spotify_streams
  landing_spotify_streams_source --> bronze__quarantine
  landing_spotify_streams_source --> bronze__ingestion_log
  landing_musicbrainz_cache --> bronze_musicbrainz_artists
  bronze_spotify_streams --> silver_plays
  bronze_spotify_streams --> silver_episodes
  bronze_spotify_streams --> silver__discarded
  silver_plays --> silver_artists
  bronze_musicbrainz_artists --> silver_artists
  silver_plays --> gold_dim_date
  silver_artists --> gold_dim_artist
  silver_plays --> gold_dim_track
  ext_platform_catalog --> gold_dim_platform
  silver_plays --> gold_fact_plays
  gold_fact_plays --> gold_kpi_listening_daily
  gold_dim_date --> gold_kpi_listening_daily
  gold_fact_plays --> gold_kpi_top_artists_monthly
  gold_dim_artist --> gold_kpi_top_artists_monthly
  gold_fact_plays --> gold_kpi_genres_monthly
  gold_dim_artist --> gold_kpi_genres_monthly
  gold_fact_plays --> gold_kpi_artist_retention
  gold_fact_plays --> gold_ml_skip_features
  gold_dim_artist --> gold_ml_skip_features
  gold_ml_skip_features --> gold_ml_skip_predictions
  ml_skip_model --> gold_ml_skip_predictions
  gold_ml_skip_features --> ml_skip_model
  gold_kpi_listening_daily --> consumer_bi
  gold_kpi_top_artists_monthly --> consumer_bi
  gold_kpi_genres_monthly --> consumer_bi
  gold_kpi_artist_retention --> consumer_bi
  gold_dim_date --> consumer_bi
  gold_dim_artist --> consumer_bi
  gold_dim_track --> consumer_bi
  gold_dim_platform --> consumer_bi
  gold_fact_plays --> consumer_bi
  gold_ml_skip_features --> consumer_bi
  gold_ml_skip_predictions --> consumer_bi
```

## Columnas clave

| Columna destino | Viene de | Cómo |
|---|---|---|
| `bronze.spotify_streams.ip_addr_hash` | `landing.spotify_streams_source.ip_addr` | HMAC-SHA256 con la sal IP_HASH_SALT; la IP en claro no sale de landing (D8) |
| `silver.plays.(sin IP)` | `bronze.spotify_streams.ip_addr_hash` | Se elimina: ninguna tabla de Plata u Oro tiene columnas de IP |
| `silver.plays.play_id` | `bronze.spotify_streams.ts`<br>`bronze.spotify_streams.spotify_track_uri`<br>`bronze.spotify_streams.ms_played` | SHA-256 de la llave de deduplicacion |
| `silver.plays.ts_start` | `bronze.spotify_streams.ts`<br>`bronze.spotify_streams.ms_played` | ts (fin) menos ms_played |
| `silver.plays.local_date / local_hour / local_weekday` | `bronze.spotify_streams.ts`<br>`bronze.spotify_streams.conn_country` | Zona IANA del pais (o de la sesion si el pais es ZZ) aplicada al inicio |
| `silver.plays.platform_family` | `bronze.spotify_streams.platform` | Primer patron que coincide (transformation/catalogs.py) |
| `silver.plays.session_id` | `bronze.spotify_streams.ts`<br>`bronze.spotify_streams.ms_played` | Nueva sesion tras mas de 30 min sin escuchar |
| `silver.plays.is_skip` | `bronze.spotify_streams.reason_end`<br>`bronze.spotify_streams.ms_played` | fwdbtn/backbtn = skip; trackdone = no; endplay/playbtn segun 30 s; otro = null |
| `silver.plays.offline_at` | `bronze.spotify_streams.offline_timestamp` | Epoch en segundos o milisegundos normalizado a UTC (H-003) |
| `silver.artists.primary_genre` | `bronze.musicbrainz_artists.genres`<br>`bronze.musicbrainz_artists.status` | Genero con mas votos, solo si hubo match confiable |
| `gold.fact_plays.date_key` | `silver.plays.local_date` | Fecha local como AAAAMMDD (dim_date) |
| `gold.fact_plays.artist_key` | `silver.plays.artist_name` | Primeros 16 hex del SHA-256 del nombre (dim_artist) |
| `gold.fact_plays.track_key` | `silver.plays.track_uri` | URI de la cancion (dim_track) |
| `gold.kpi_listening_daily.skip_rate` | `gold.fact_plays.is_skip` | skips / reproducciones etiquetadas por dia local |
| `gold.kpi_genres_monthly.genre` | `gold.dim_artist.primary_genre`<br>`gold.fact_plays.artist_key` | Genero principal del artista de cada reproduccion |
| `gold.kpi_artist_retention.retention_rate` | `gold.fact_plays.artist_key`<br>`gold.fact_plays.date_key` | Artistas de la cohorte escuchados N meses despues / tamano de la cohorte |
| `gold.ml_skip_features.track_prior_skip_rate` | `gold.fact_plays.is_skip`<br>`gold.fact_plays.track_key`<br>`gold.fact_plays.ts_start` | Skips / etiquetadas de la misma cancion que empezaron antes (sin fuga) |
| `gold.ml_skip_predictions.skip_probability` | `gold.ml_skip_features.(todas las features)` | Modelo registrado en MLflow (ml.skip_model) aplicado por lote a cada fila |
