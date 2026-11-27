-- Responsable: Emilio Rendon Trias (Analista de Datos / BI)
-- Predicciones del modelo de skip (F6) con su contexto: plataforma y genero salen de las features, asi los
-- filtros del tablero tambien aplican. Todo es Oro (gold.ml_*).
SELECT
    d.date,
    p.split,
    p.is_skip,
    p.skip_probability,
    p.baseline_probability,
    p.predicted_skip,
    p.model_version,
    x.platform_family,
    coalesce(x.primary_genre, 'sin genero') AS genre
FROM delta_scan('s3://gold/ml_skip_predictions') AS p
LEFT JOIN delta_scan('s3://gold/ml_skip_features') AS x ON x.play_id = p.play_id
LEFT JOIN delta_scan('s3://gold/dim_date') AS d ON d.date_key = p.date_key
