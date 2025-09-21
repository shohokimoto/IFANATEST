-- Create stage table for Restaurant Board reservations
-- This table is partitioned by ingestion_ts and has a TTL

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.stage_reservations_rb` (
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
PARTITION BY DATE(ingestion_ts)
CLUSTER BY store_id, vendor, run_id
OPTIONS (
  description = "Stage table for Restaurant Board reservation data - partitioned by ingestion date with 14-day TTL",
  partition_expiration_days = 14
);
