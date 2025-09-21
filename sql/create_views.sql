-- ビュー作成
-- 用途: Connected Sheetsでの表示用

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
WHERE reserve_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)  -- 過去90日分のみ
GROUP BY reserve_date, store_id, store_name
ORDER BY reserve_date DESC, store_id;

-- 店舗別月次サマリビュー
CREATE OR REPLACE VIEW `{PROJECT_ID}.rb.vw_monthly_store_summary` AS
SELECT 
  DATE_TRUNC(reserve_date, MONTH) as month,
  store_id,
  store_name,
  COUNT(*) as total_reservations,
  SUM(headcount) as total_guests,
  AVG(headcount) as avg_guests_per_reservation,
  COUNT(DISTINCT reserve_date) as active_days,
  COUNT(DISTINCT channel) as channel_count,
  COUNTIF(status = '確定') as confirmed_count,
  COUNTIF(status = 'キャンセル') as cancelled_count,
  ROUND(COUNTIF(status = 'キャンセル') / COUNT(*) * 100, 2) as cancellation_rate_pct
FROM `{PROJECT_ID}.rb.reservations_rb`
WHERE reserve_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)  -- 過去12ヶ月分
GROUP BY DATE_TRUNC(reserve_date, MONTH), store_id, store_name
ORDER BY month DESC, store_id;

-- チャンネル別サマリビュー
CREATE OR REPLACE VIEW `{PROJECT_ID}.rb.vw_channel_summary` AS
SELECT 
  channel,
  COUNT(*) as total_reservations,
  SUM(headcount) as total_guests,
  AVG(headcount) as avg_guests_per_reservation,
  COUNT(DISTINCT store_id) as store_count,
  COUNT(DISTINCT reserve_date) as active_days,
  COUNTIF(status = '確定') as confirmed_count,
  COUNTIF(status = 'キャンセル') as cancelled_count,
  ROUND(COUNTIF(status = 'キャンセル') / COUNT(*) * 100, 2) as cancellation_rate_pct
FROM `{PROJECT_ID}.rb.reservations_rb`
WHERE reserve_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)  -- 過去30日分
GROUP BY channel
ORDER BY total_reservations DESC;

-- Connected Sheets用の明細ビュー（期間指定用）
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
WHERE reserve_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)  -- 過去1年分
ORDER BY reserve_date DESC, store_id, start_time;
