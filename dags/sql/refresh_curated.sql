-- ─────────────────────────────────────────────────────────────────────────
--  Vista de insights: consumo de datos por plan y fecha
--  Dataset  : self_curated_zone
--  View name: vw_consumo_insights
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `self_curated_zone.vw_consumo_insights` AS
WITH hora_a_dia AS (
  SELECT
    -- Cambiamos la hora de evento a fecha calendario
    DATE(fecha_evento)                  AS fecha_evento,
    tipo_plan                           AS plan,
    id_cliente,
    consumo_datos_gb
  FROM `self_raw_zone.customer_events_raw`
  WHERE _PARTITIONTIME >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)   -- ventana de 90 días
)

SELECT
  fecha_evento                                         AS fecha,
  plan,
  COUNT(DISTINCT id_cliente)                           AS clientes_activos,
  SUM(consumo_datos_gb)                                AS consumo_total_gb,
  AVG(consumo_datos_gb)                                AS consumo_promedio_gb,
  APPROX_QUANTILES(consumo_datos_gb, 100)[OFFSET(50)]  AS mediana_gb,
  PERCENTILE_CONT(consumo_datos_gb, 0.90) OVER ()      AS p90_gb_global,
  PERCENTILE_CONT(consumo_datos_gb, 0.90)
      OVER (PARTITION BY plan, fecha_evento)           AS p90_gb_plan,
  CURRENT_TIMESTAMP()                                  AS ts_actualizacion
FROM hora_a_dia
GROUP BY fecha_evento, plan;
