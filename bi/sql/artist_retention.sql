-- Responsable: Emilio Rendon Trias (Analista de Datos / BI)
-- Mart de Oro con la retencion de artistas por cohorte (kpi_artist_retention), hasta 12 meses despues del
-- descubrimiento para que el mapa de calor se lea. month_label (M00 a M12) ordena bien como texto.
SELECT
    cohort_start,
    cohort_month,
    months_since,
    'M' || lpad(CAST(months_since AS VARCHAR), 2, '0') AS month_label,
    cohort_size,
    artists_active
FROM delta_scan('s3://gold/kpi_artist_retention')
WHERE months_since <= 12
