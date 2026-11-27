-- Responsable: Emilio Rendon Trias (Analista de Datos / BI)
-- Reproducciones de musica con sus dimensiones (modelo estrella de Oro, D10). Base de casi todo el tablero y de
-- los filtros de genero y plataforma. Las fechas son locales (dim_date); sin hora local quedan en null.
SELECT
    d.date,
    d.year_month,
    printf('%d %s', d.weekday, d.weekday_name) AS weekday_label,
    lpad(CAST(f.local_hour AS VARCHAR), 2, '0') AS hour_label,
    a.artist_name,
    coalesce(a.primary_genre, 'sin genero') AS genre,
    t.track_name,
    f.platform_family,
    p.device_type,
    f.conn_country,
    f.session_id,
    f.minutes_played,
    f.is_skip,
    f.shuffle,
    f.reason_start
FROM delta_scan('s3://gold/fact_plays') AS f
LEFT JOIN delta_scan('s3://gold/dim_date') AS d ON d.date_key = f.date_key
LEFT JOIN delta_scan('s3://gold/dim_artist') AS a ON a.artist_key = f.artist_key
LEFT JOIN delta_scan('s3://gold/dim_track') AS t ON t.track_key = f.track_key
LEFT JOIN delta_scan('s3://gold/dim_platform') AS p ON p.platform_family = f.platform_family
