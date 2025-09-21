-- Complete BigQuery setup script
-- Run this script to set up all required BigQuery objects

-- 1. Create dataset
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.rb`
OPTIONS (
  description = "Restaurant Board reservation data",
  location = "asia-northeast1"
);

-- 2. Create stage table
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.rb.stage_reservations_rb` (
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

-- 3. Create production table
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.rb.reservations_rb` (
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

-- 4. Create view for daily KPIs (optional, for future use)
CREATE VIEW IF NOT EXISTS `{PROJECT_ID}.rb.vw_daily_kpi` AS
SELECT
  store_id,
  store_name,
  reserve_date,
  channel,
  status,
  COUNT(*) as reservation_count,
  SUM(headcount) as total_headcount,
  AVG(headcount) as avg_party_size,
  COUNT(CASE WHEN status = 'confirmed' THEN 1 END) as confirmed_count,
  COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_count,
  COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count
FROM `{PROJECT_ID}.rb.reservations_rb`
GROUP BY store_id, store_name, reserve_date, channel, status;
