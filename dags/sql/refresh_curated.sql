--  Vista de insights: consumo de datos por plan y fecha  (sin _PARTITIONTIME)
CREATE OR REPLACE VIEW `self_curated_zone.vw_consumo_insights` AS

-- 1️⃣  Aggregate per día + plan
WITH base AS (
  SELECT
    DATE(fecha_evento)                         AS fecha,      -- Día calendario
    tipo_plan                                  AS plan,
    COUNT(DISTINCT id_cliente)                 AS clientes_activos,
    SUM(consumo_datos_gb)                      AS consumo_total_gb,
    AVG(consumo_datos_gb)                      AS consumo_promedio_gb,
    APPROX_QUANTILES(consumo_datos_gb, 100)[OFFSET(50)] AS mediana_gb
  FROM `self_raw_zone.customer_events_raw`
  WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY fecha, plan
)

-- 2️⃣  Add percentiles with window functions over that aggregated set
SELECT
  fecha,
  plan,
  clientes_activos,
  consumo_total_gb,
  consumo_promedio_gb,
  mediana_gb,
  PERCENTILE_CONT(consumo_total_gb, 0.90) OVER ()                     AS p90_gb_global,
  PERCENTILE_CONT(consumo_total_gb, 0.90) OVER (PARTITION BY plan)    AS p90_gb_plan,
  CURRENT_TIMESTAMP()                                                 AS ts_actualizacion
FROM base;
