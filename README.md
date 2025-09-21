# Restaurant Board Scraper ETL

レストランボードの予約データを自動取得し、BigQueryに格納するETLシステムです。Node.jsで完結したMVP版の実装です。

## 概要

このシステムは以下の処理を自動化します：

1. **Google Sheetsから店舗マスタを取得**
2. **Puppeteerでレストランボードをスクレイピング**
3. **CSVデータの整形・正規化**
4. **Google Cloud Storageへのアップロード**
5. **BigQueryへの取り込み（ステージ→本番のMERGE）**
6. **Connected Sheetsでの表示**

## アーキテクチャ

```
[Google Sheets: Stores] → [Scraper App: Node.js + Puppeteer + ETL]
        │ 読取API                 │ CSV(共通カラム) + BQ Load/MERGE
        └───────────────┬───────────────┘
                        ▼
           [GCS: landing/tmp] → [BigQuery: stage_reservations_rb] → MERGE → [reservations_rb]
                                                                                       │
                                                                                       └→ [Connected Sheets]
```

## 技術スタック

- **Runtime**: Node.js 18+
- **スクレイピング**: Puppeteer
- **データ変換**: csv-parse, fast-csv, iconv-lite
- **クラウド**: Google Cloud Run, BigQuery, Cloud Storage
- **スケジューリング**: Cloud Scheduler
- **デプロイ**: Docker, Cloud Build

## セットアップ

### 前提条件

- Google Cloud プロジェクト
- gcloud CLI がインストール・認証済み
- BigQuery API, Cloud Run API, Cloud Storage API が有効化済み

### 1. リポジトリのクローン

```bash
git clone <repository-url>
cd IFANA_TEST
```

### 2. 環境変数の設定

```bash
cp env.example .env
```

`.env`ファイルを編集して、以下の値を設定：

```bash
PROJECT_ID=your-project-id
BQ_DATASET=rb
GCS_BUCKET=your-bucket-name
STORES_SHEET_ID=your-google-sheets-id
REGION=asia-northeast1
```

### 3. Google Sheetsの準備

店舗マスタシートを作成し、以下のヘッダーでデータを入力：

| active | store_id | store_name | rb_username | rb_password | days_back | from_date | to_date | note |
|--------|----------|------------|-------------|-------------|-----------|-----------|---------|------|
| true   | store001 | テスト店舗  | username    | password    | 7         |           |         |      |

### 4. サービスアカウントの作成

```bash
# サービスアカウント作成
gcloud iam service-accounts create rb-scraper-etl \
    --display-name="RB Scraper ETL Service Account"

# 必要な権限を付与
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:rb-scraper-etl@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:rb-scraper-etl@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:rb-scraper-etl@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:rb-scraper-etl@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/sheets.readonly"
```

### 5. デプロイ

自動デプロイスクリプトを使用：

```bash
export PROJECT_ID=your-project-id
export GCS_BUCKET=your-bucket-name
export STORES_SHEET_ID=your-sheet-id

./deploy.sh
```

または手動でデプロイ：

```bash
# BigQueryセットアップ
bq query --use_legacy_sql=false < sql/setup_bigquery.sql

# Cloud Buildでビルド・デプロイ
gcloud builds submit \
    --substitutions=_GCS_BUCKET=$GCS_BUCKET,_STORES_SHEET_ID=$STORES_SHEET_ID
```

## 運用

### スケジュール実行

Cloud Schedulerで毎日01:15 JSTに自動実行されます。

### 手動実行

```bash
# Cloud Runサービスを直接実行
gcloud run services invoke rb-scraper-etl --region=asia-northeast1
```

### ログ確認

```bash
# Cloud Loggingでログを確認
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=rb-scraper-etl" --limit=50
```

### 手動データアップロード

テスト用に手動でCSVをアップロードできます：

```bash
# GCSにアップロード
gsutil cp your-data.csv gs://$GCS_BUCKET/manual/restaurant_board/2024/01/01/

# BigQueryに取り込み
bq load --source_format=CSV --skip_leading_rows=1 \
    rb.stage_reservations_rb \
    gs://$GCS_BUCKET/manual/restaurant_board/2024/01/01/your-data.csv

# MERGEを実行
bq query --use_legacy_sql=false \
    "$(sed 's/{RUN_ID}/manual_2024010_120000/g' sql/merge_reservations_rb.sql)"
```

## データモデル

### 共通カラム定義

| 物理名 | 型 | 説明 |
|-------|---|-----|
| store_id | STRING | 店舗ID |
| store_name | STRING | 店舗名 |
| reserve_date | DATE | 予約日 |
| booking_date | DATE | 予約受付日 |
| start_time | TIME | 開始時間 |
| end_time | TIME | 終了時間 |
| course_name | STRING | コース名 |
| headcount | INT64 | 人数 |
| channel | STRING | 経路 |
| status | STRING | ステータス |
| vendor | STRING | ベンダー（固定値：restaurant_board） |
| ingestion_ts | TIMESTAMP | 取込時刻 |
| run_id | STRING | 実行ID |
| record_key | STRING | レコードキー |
| record_hash | STRING | 内容ハッシュ |

### BigQueryテーブル

- **ステージテーブル**: `rb.stage_reservations_rb`
  - パーティション: `DATE(ingestion_ts)`
  - クラスタ: `store_id, vendor, run_id`
  - TTL: 14日

- **本番テーブル**: `rb.reservations_rb`
  - パーティション: `reserve_date`
  - クラスタ: `store_id, channel`

## Connected Sheets

BigQueryデータをスプレッドシートで直接参照できます：

1. Google Sheetsで「データ」→「データコネクタ」→「BigQueryに接続」
2. プロジェクトとデータセット（rb）を選択
3. カスタムクエリで以下を使用：

```sql
SELECT store_id, store_name, reserve_date, booking_date,
       start_time, end_time, course_name, headcount, channel, status
FROM `your-project.rb.reservations_rb`
WHERE reserve_date BETWEEN @from_date AND @to_date
ORDER BY reserve_date DESC, store_id, start_time
LIMIT 10000
```

## トラブルシューティング

### よくある問題

1. **ログイン失敗**
   - 店舗マスタのユーザー名・パスワードを確認
   - 2FA有効店舗は`active=false`に設定

2. **CSV形式エラー**
   - レストランボードの画面変更の可能性
   - スクレイピングセレクタの更新が必要

3. **BigQuery権限エラー**
   - サービスアカウントの権限を確認
   - `roles/bigquery.jobUser`と`roles/bigquery.dataEditor`が必要

4. **GCS権限エラー**
   - サービスアカウントに`roles/storage.objectAdmin`権限を確認

### ログレベル設定

```bash
# デバッグログを有効化
gcloud run services update rb-scraper-etl \
    --set-env-vars LOG_LEVEL=debug \
    --region=asia-northeast1
```

### メンテナンスモード

```bash
# DRY_RUNモードで実行（実際の更新なし）
gcloud run services update rb-scraper-etl \
    --set-env-vars DRY_RUN=true \
    --region=asia-northeast1
```

## 開発

### ローカル開発

```bash
# 依存関係インストール
npm install

# 環境変数設定
cp env.example .env
# .envを編集

# ローカル実行
npm start

# デバッグモード
npm run dev
```

### テスト

```bash
# テスト実行（今後実装予定）
npm test
```

### Docker実行

```bash
# イメージビルド
docker build -t rb-scraper-etl .

# ローカル実行
docker run --env-file .env rb-scraper-etl
```

## コスト最適化

### BigQuery

- パーティション・クラスタ設計により効率的なクエリ実行
- `SELECT *`禁止、必要な列のみ選択
- 日付範囲指定必須
- ステージテーブルの自動削除（TTL設定済み）

### Cloud Run

- 最大インスタンス数を1に制限
- 必要時のみ起動（0からのスケール）

### GCS

- ライフサイクル管理で古いファイルを自動削除

## ライセンス

MIT License

## サポート

技術的な問題や要望については、GitHubのIssueを作成してください。

## 更新履歴

- v1.0: 初版リリース（レストランボード対応、Node.js完結版）
