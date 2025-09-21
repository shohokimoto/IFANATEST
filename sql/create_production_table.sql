-- Create production table for Restaurant Board reservations
-- This table is partitioned by reserve_date and clustered by store_id and channel

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.reservations_rb` (
  store_id STRING NOT NULL,
  store_name STRING,
  reserve_date DATE NOT NULL,
  booking_date DATE,
  start_time TIME,
  end_time TIME,
  course_name STRING,
  headcount INT64,
  channel STRING,
  status STRING,
  vendor STRING NOT NULL,
  ingestion_ts TIMESTAMP NOT NULL,
  run_id STRING NOT NULL,
  record_key STRING NOT NULL,
  record_hash STRING NOT NULL
)
PARTITION BY reserve_date
CLUSTER BY store_id, channel
OPTIONS (
  description = "Production table for Restaurant Board reservation data - partitioned by reserve_date"
);
