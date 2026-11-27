-- Responsable: Emilio Rendon Trias (Analista de Datos / BI)
-- Mart de Oro con el ranking mensual de artistas (kpi_top_artists_monthly). genre se llama igual que en
-- plays para que el filtro de genero del tablero tambien aplique aqui.
SELECT
    month_start AS month,
    year_month,
    artist_name,
    coalesce(primary_genre, 'sin genero') AS genre,
    plays,
    round(minutes_played / 60.0, 1) AS hours,
    rank_in_month
FROM delta_scan('s3://gold/kpi_top_artists_monthly')
