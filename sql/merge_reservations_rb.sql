-- ストアドプロシージャ: ステージ→本番のMERGE処理
-- プロシージャ: rb.merge_reservations_rb
-- 用途: ステージテーブルから本番テーブルへの差分反映（UPSERT）

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
