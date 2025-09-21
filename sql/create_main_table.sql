-- 本番テーブル作成
-- テーブル: rb.reservations_rb
-- 用途: 正規化された予約データの本番保存

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.rb.reservations_rb` (
  store_id STRING NOT NULL OPTIONS(description="店舗ID"),
  store_name STRING OPTIONS(description="店舗名"),
  reserve_date DATE NOT NULL OPTIONS(description="予約日"),
  booking_date DATE OPTIONS(description="予約受付日"),
  start_time TIME OPTIONS(description="予約開始時間"),
  end_time TIME OPTIONS(description="予約終了時間"),
  course_name STRING OPTIONS(description="コース名"),
  headcount INT64 OPTIONS(description="人数"),
  channel STRING OPTIONS(description="経路"),
  status STRING OPTIONS(description="予約ステータス"),
  vendor STRING NOT NULL OPTIONS(description="ベンダー"),
  ingestion_ts TIMESTAMP NOT NULL OPTIONS(description="取込時刻"),
  run_id STRING NOT NULL OPTIONS(description="実行ID"),
  record_key STRING NOT NULL OPTIONS(description="レコードキー"),
  record_hash STRING NOT NULL OPTIONS(description="内容ハッシュ"),
  created_at TIMESTAMP NOT NULL OPTIONS(description="作成日時"),
  updated_at TIMESTAMP NOT NULL OPTIONS(description="更新日時")
)
PARTITION BY reserve_date
CLUSTER BY store_id, channel
OPTIONS (
  description = "レストランボード予約データの本番テーブル"
);
