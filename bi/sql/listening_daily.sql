-- Responsable: Emilio Rendon Trias (Analista de Datos / BI)
-- Mart diario de Oro (kpi_listening_daily): ya viene agregado, la tendencia no recorre fact_plays.
SELECT
    date,
    plays,
    minutes_played,
    distinct_tracks,
    distinct_artists,
    sessions,
    labeled_plays,
    skips
FROM delta_scan('s3://gold/kpi_listening_daily')
