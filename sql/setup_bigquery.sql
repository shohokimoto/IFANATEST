-- BigQuery初期セットアップスクリプト
-- 使用方法: bq query --use_legacy_sql=false < setup_bigquery.sql

-- プロジェクトIDを環境変数または置換文字列で設定
-- 例: sed 's/{PROJECT_ID}/your-project-id/g' setup_bigquery.sql | bq query --use_legacy_sql=false

-- 1. データセット作成
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.rb`
OPTIONS (
  description = "レストランボード予約データETL用データセット",
  location = "asia-northeast1"
);

-- 2. ステージテーブル作成
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.rb.stage_reservations_rb` (
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
  record_hash STRING NOT NULL OPTIONS(description="内容ハッシュ")
)
PARTITION BY DATE(ingestion_ts)
OPTIONS (
  description = "レストランボード予約データのステージテーブル（TTL: 30日）",
  partition_expiration_days = 30
);

-- 3. 本番テーブル作成
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

-- 4. ストアドプロシージャ作成
CREATE OR REPLACE PROCEDURE `{PROJECT_ID}.rb.merge_reservations_rb`(run STRING)
BEGIN
  DECLARE inserted_count INT64 DEFAULT 0;
  DECLARE updated_count INT64 DEFAULT 0;
  DECLARE unchanged_count INT64 DEFAULT 0;

  -- ステージテーブルの重複を排除（最新のingestion_tsを優先）
  CREATE OR REPLACE TEMP TABLE stage_deduplicated AS
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY record_key 
             ORDER BY ingestion_ts DESC
           ) as rn
    FROM `{PROJECT_ID}.rb.stage_reservations_rb`
    WHERE run_id = run
  )
  WHERE rn = 1;

  -- MERGE実行
  MERGE `{PROJECT_ID}.rb.reservations_rb` AS target
  USING stage_deduplicated AS source
  ON target.vendor = source.vendor
     AND target.store_id = source.store_id
     AND target.record_key = source.record_key
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
      record_hash = source.record_hash,
      updated_at = CURRENT_TIMESTAMP()
  WHEN MATCHED AND target.record_hash = source.record_hash THEN
    UPDATE SET
      -- 何もしない（ハッシュが同じ場合は更新しない）
      updated_at = target.updated_at
  WHEN NOT MATCHED THEN
    INSERT (
      store_id, store_name, reserve_date, booking_date,
      start_time, end_time, course_name, headcount,
      channel, status, vendor, ingestion_ts,
      run_id, record_key, record_hash,
      created_at, updated_at
    )
    VALUES (
      source.store_id, source.store_name, source.reserve_date, source.booking_date,
      source.start_time, source.end_time, source.course_name, source.headcount,
      source.channel, source.status, source.vendor, source.ingestion_ts,
      source.run_id, source.record_key, source.record_hash,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    );

  -- 処理結果を取得
  SET inserted_count = (
    SELECT COUNT(*)
    FROM stage_deduplicated s
    LEFT JOIN `{PROJECT_ID}.rb.reservations_rb` t
      ON s.vendor = t.vendor
      AND s.store_id = t.store_id
      AND s.record_key = t.record_key
    WHERE t.record_key IS NULL
  );

  SET updated_count = (
    SELECT COUNT(*)
    FROM stage_deduplicated s
    JOIN `{PROJECT_ID}.rb.reservations_rb` t
      ON s.vendor = t.vendor
      AND s.store_id = t.store_id
      AND s.record_key = t.record_key
    WHERE s.record_hash != t.record_hash
  );

  SET unchanged_count = (
    SELECT COUNT(*)
    FROM stage_deduplicated s
    JOIN `{PROJECT_ID}.rb.reservations_rb` t
      ON s.vendor = t.vendor
      AND s.store_id = t.store_id
      AND s.record_key = t.record_key
    WHERE s.record_hash = t.record_hash
  );

  -- 結果を返す
  SELECT 
    run as run_id,
    inserted_count as inserted,
    updated_count as updated,
    unchanged_count as unchanged,
    (inserted_count + updated_count + unchanged_count) as total_processed,
    CURRENT_TIMESTAMP() as processed_at
  FROM (SELECT 1);

  -- 一時テーブルを削除
  DROP TABLE stage_deduplicated;

END;

-- 5. ビュー作成
-- 日別予約サマリビュー
CREATE OR REPLACE VIEW `{PROJECT_ID}.rb.vw_daily_reservation_summary` AS
SELECT 
  reserve_date,
  store_id,
  store_name,
  COUNT(*) as total_reservations,
  SUM(headcount) as total_guests,
  COUNT(DISTINCT channel) as channel_count,
  COUNTIF(status = '確定') as confirmed_count,
  COUNTIF(status = 'キャンセル') as cancelled_count,
  COUNTIF(status = 'キャンセル') / COUNT(*) as cancellation_rate
FROM `{PROJECT_ID}.rb.reservations_rb`
WHERE reserve_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY reserve_date, store_id, store_name
ORDER BY reserve_date DESC, store_id;

-- Connected Sheets用の明細ビュー
CREATE OR REPLACE VIEW `{PROJECT_ID}.rb.vw_reservation_details` AS
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
  ingestion_ts,
  created_at,
  updated_at
FROM `{PROJECT_ID}.rb.reservations_rb`
WHERE reserve_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
ORDER BY reserve_date DESC, store_id, start_time;

-- セットアップ完了メッセージ
SELECT "BigQueryセットアップが完了しました" as message;
