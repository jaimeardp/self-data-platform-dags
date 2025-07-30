MERGE `self_raw_zone.customer_events_raw`  T
USING (
  SELECT *
  FROM  `self_staging_zone.customer_events_ext`
  WHERE year  = '{{ data_interval_start.strftime("%Y") }}'
    AND month = '{{ data_interval_start.strftime("%m") }}'
    AND day   = '{{ data_interval_start.strftime("%d") }}'
    AND hour  = '{{ data_interval_start.strftime("%H") }}'
) S
ON  T.event_uuid = S.event_uuid
WHEN NOT MATCHED THEN
  INSERT ROW;
