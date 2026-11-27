-- Responsable: Emilio Rendon Trias (Analista de Datos / BI)
-- Mart de Oro con los generos por mes (kpi_genres_monthly). Genero null = artista sin genero conocido (H-004).
SELECT
    month_start AS month,
    year_month,
    coalesce(genre, 'sin genero') AS genre,
    plays,
    minutes_played,
    artists
FROM delta_scan('s3://gold/kpi_genres_monthly')
