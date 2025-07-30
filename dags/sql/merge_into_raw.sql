MERGE self_raw_zone.customer_events_raw T
USING (
  SELECT * FROM self_staging_zone.customer_events_ext
  WHERE year  = '{{ params.p_year }}'
    AND month = '{{ params.p_month }}'
    AND day   = '{{ params.p_day }}'
    AND hour  = '{{ params.p_hour }}'
) S
ON  T.event_uuid = S.event_uuid
WHEN NOT MATCHED THEN
  INSERT ROW;
