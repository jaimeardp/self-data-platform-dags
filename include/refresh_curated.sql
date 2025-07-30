CREATE OR REPLACE TABLE self_curated_zone.customer_usage_curated AS
SELECT
  DATE(fecha_evento)            AS event_date,
  tipo_plan,
  AVG(consumo_datos_gb)         AS avg_gb,
  COUNT(DISTINCT id_cliente)    AS active_clients
FROM self_raw_zone.customer_events_raw
WHERE _PARTITIONTIME BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1,2;
