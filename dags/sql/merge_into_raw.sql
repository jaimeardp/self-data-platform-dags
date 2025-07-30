MERGE self_raw_zone.customer_events_raw T
USING (
  SELECT * FROM self_staging_zone.customer_events_ext
  WHERE year  = @p_year
    AND month = @p_month
    AND day   = @p_day
    AND hour  = @p_hour
) S
ON  T.event_uuid = S.event_uuid
WHEN NOT MATCHED THEN
  INSERT ROW;
