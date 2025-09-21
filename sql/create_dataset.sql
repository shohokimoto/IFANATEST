-- Create BigQuery dataset for Restaurant Board data
-- This script should be run once to set up the dataset

CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.rb`
OPTIONS (
  description = "Restaurant Board reservation data",
  location = "asia-northeast1"
);
