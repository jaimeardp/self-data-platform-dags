/* ─── dags/sql/merge_into_raw.sql ────────────────────────────────────────────
   Insert or update ONE hour of data from the staging external table into RAW.
   Every column is referenced explicitly to avoid schema‑drift surprises.      */

MERGE `self_raw_zone.customer_events_raw`        AS T
USING (
    SELECT
        id_cliente,
        numero_telefono,
        nombre_cliente,
        direccion,
        tipo_plan,
        consumo_datos_gb,
        estado_cuenta,
        fecha_registro,
        fecha_evento,
        tipo_evento,
        id_dispositivo,
        marca_dispositivo,
        antiguedad_cliente_meses,
        score_crediticio,
        origen_captacion,
        ingestion_ts,
        event_uuid,
        source_file
    FROM  `self_staging_zone.customer_events_ext`
    WHERE year  = '{{ data_interval_start.strftime("%Y") }}'
      AND month = '{{ data_interval_start.strftime("%m") }}'
      AND day   = '{{ data_interval_start.strftime("%d") }}'
      AND hour  = '{{ data_interval_start.strftime("%H") }}'
) AS S
ON T.event_uuid = S.event_uuid        -- idempotent key
WHEN MATCHED THEN
  /* update only mutable fields; keep event_uuid constant */
  UPDATE SET
      id_cliente               = S.id_cliente,
      numero_telefono          = S.numero_telefono,
      nombre_cliente           = S.nombre_cliente,
      direccion                = S.direccion,
      tipo_plan                = S.tipo_plan,
      consumo_datos_gb         = S.consumo_datos_gb,
      estado_cuenta            = S.estado_cuenta,
      fecha_registro           = S.fecha_registro,
      fecha_evento             = S.fecha_evento,
      tipo_evento              = S.tipo_evento,
      id_dispositivo           = S.id_dispositivo,
      marca_dispositivo        = S.marca_dispositivo,
      antiguedad_cliente_meses = S.antiguedad_cliente_meses,
      score_crediticio         = S.score_crediticio,
      origen_captacion         = S.origen_captacion,
      ingestion_ts             = S.ingestion_ts,
      source_file              = S.source_file
WHEN NOT MATCHED THEN
  INSERT (
      id_cliente,
      numero_telefono,
      nombre_cliente,
      direccion,
      tipo_plan,
      consumo_datos_gb,
      estado_cuenta,
      fecha_registro,
      fecha_evento,
      tipo_evento,
      id_dispositivo,
      marca_dispositivo,
      antiguedad_cliente_meses,
      score_crediticio,
      origen_captacion,
      ingestion_ts,
      event_uuid,
      source_file
  )
  VALUES (
      S.id_cliente,
      S.numero_telefono,
      S.nombre_cliente,
      S.direccion,
      S.tipo_plan,
      S.consumo_datos_gb,
      S.estado_cuenta,
      S.fecha_registro,
      S.fecha_evento,
      S.tipo_evento,
      S.id_dispositivo,
      S.marca_dispositivo,
      S.antiguedad_cliente_meses,
      S.score_crediticio,
      S.origen_captacion,
      S.ingestion_ts,
      S.event_uuid,
      S.source_file
  );
