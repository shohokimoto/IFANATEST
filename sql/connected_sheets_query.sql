-- Connected Sheets query for viewing reservation data
-- This query should be used in Connected Sheets with date parameters

SELECT 
  store_id,
  store_name,
  reserve_date,
  booking_date,
  start_time,
  end_time,
  course_name,
  headcount,
  channel,
  status,
  vendor,
  ingestion_ts
FROM `{PROJECT_ID}.{DATASET}.reservations_rb`
WHERE reserve_date BETWEEN @from_date AND @to_date
  AND (@store_id IS NULL OR store_id = @store_id)
  AND (@channel IS NULL OR channel = @channel)
  AND (@status IS NULL OR status = @status)
ORDER BY reserve_date DESC, store_id, start_time
LIMIT 10000;
