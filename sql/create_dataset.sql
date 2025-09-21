-- BigQueryデータセット作成
-- データセット: rb (restaurant board)
-- リージョン: asia-northeast1

CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.rb`
OPTIONS (
  description = "レストランボード予約データETL用データセット",
  location = "asia-northeast1"
);
