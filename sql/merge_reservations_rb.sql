-- MERGE operation to sync stage table data to production table
-- This handles INSERT, UPDATE, and duplicate resolution

WITH stage_deduplicated AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY record_key 
             ORDER BY ingestion_ts DESC
           ) as row_num
    FROM `{PROJECT_ID}.{DATASET}.{STAGE_TABLE}`
    WHERE run_id = '{RUN_ID}'
  )
  WHERE row_num = 1
),
merge_stats AS (
  MERGE `{PROJECT_ID}.{DATASET}.{PRODUCTION_TABLE}` AS target
  USING stage_deduplicated AS source
  ON target.record_key = source.record_key 
     AND target.vendor = source.vendor 
     AND target.store_id = source.store_id

  -- Update existing records if content has changed
  WHEN MATCHED AND target.record_hash != source.record_hash THEN
    UPDATE SET
      store_name = source.store_name,
      reserve_date = source.reserve_date,
      booking_date = source.booking_date,
      start_time = source.start_time,
      end_time = source.end_time,
      course_name = source.course_name,
      headcount = source.headcount,
      channel = source.channel,
      status = source.status,
      ingestion_ts = source.ingestion_ts,
      run_id = source.run_id,
      record_hash = source.record_hash

  -- Insert new records
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (
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
      ingestion_ts,
      run_id,
      record_key,
      record_hash
    )
    VALUES (
      source.store_id,
      source.store_name,
      source.reserve_date,
      source.booking_date,
      source.start_time,
      source.end_time,
      source.course_name,
      source.headcount,
      source.channel,
      source.status,
      source.vendor,
      source.ingestion_ts,
      source.run_id,
      source.record_key,
      source.record_hash
    )
)

-- Return merge statistics
SELECT
  @@row_count.inserted_rows,
  @@row_count.updated_rows,
  (SELECT COUNT(*) FROM stage_deduplicated) - @@row_count.inserted_rows - @@row_count.updated_rows AS unchanged_rows;
