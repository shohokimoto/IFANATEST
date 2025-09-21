# レストランボード予約データETLシステム

レストランボードから予約データを取得し、BigQueryに格納してConnected Sheetsで表示するETLシステムです。

## 概要

- **対象**: レストランボード（Restaurant Board）
- **処理**: スクレイピング → データ変換 → BigQuery格納 → Connected Sheets表示
- **実行**: Cloud Run + Cloud Scheduler（日次実行）
- **データ**: 予約データの差分反映（UPSERT）

## アーキテクチャ

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI       │    │   Node.js       │    │   React         │
│   (Python)      │◄──►│   Scraper       │    │   (Frontend)    │
│   - ETL Core    │    │   - Puppeteer   │    │   - Dashboard   │
│   - BigQuery    │    │   - Browser     │    │   - Analytics   │
│   - GCS         │    │   - Login       │    │                 │
│   - Store Master│    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## ファイル構成

```
├── backend/                      # Python FastAPI Backend
│   ├── app/
│   │   ├── main.py              # FastAPIアプリケーション
│   │   ├── config.py            # 設定管理
│   │   ├── models/schemas.py    # Pydanticモデル
│   │   ├── services/            # サービス層
│   │   │   ├── store_service.py
│   │   │   ├── bigquery_service.py
│   │   │   ├── gcs_service.py
│   │   │   ├── transformer.py
│   │   │   └── scraper_client.py
│   │   ├── utils/logger.py      # ログ管理
│   │   └── api/routes.py        # APIルート
│   ├── requirements.txt         # Python依存関係
│   ├── Dockerfile              # Python Docker設定
│   └── deploy.sh               # デプロイスクリプト
├── scraper/                     # Node.js Scraper Service
│   ├── src/
│   │   ├── index.js            # Express API
│   │   ├── config/index.js     # 設定管理
│   │   ├── utils/logger.js     # ログ管理
│   │   └── services/
│   │       ├── index.js        # サービスインデックス
│   │       └── scraper.js      # Puppeteerスクレイピング
│   ├── package.json            # Node.js依存関係
│   ├── Dockerfile              # Node.js Docker設定
│   └── env.example             # 環境変数サンプル
├── sql/                        # BigQueryスキーマ
│   ├── setup_bigquery.sql      # 初期セットアップ
│   ├── create_dataset.sql      # データセット作成
│   ├── create_stage_table.sql  # ステージテーブル
│   ├── create_main_table.sql   # 本番テーブル
│   ├── merge_reservations_rb.sql # MERGEプロシージャ
│   └── create_views.sql        # ビュー作成
├── MIGRATION_GUIDE.md          # 移行ガイド
└── DEVELOPMENT_LOG.md          # 開発ログ
```

## セットアップ手順

### 1. 前提条件

- Google Cloud Project
- 必要なAPIの有効化:
  - Cloud Run API
  - BigQuery API
  - Cloud Storage API
  - Google Sheets API
  - Cloud Scheduler API

### 2. 環境変数の設定

```bash
# 基本設定
export PROJECT_ID=your-project-id
export STORES_SHEET_ID=your-sheet-id
export GCS_BUCKET=your-gcs-bucket

# 処理設定
export DAYS_BACK=7
export FROM_DATE=  # オプション
export TO_DATE=    # オプション
```

### 3. 店舗マスタ（Google Sheets）の準備

シート名: `Stores`
ヘッダー: `active, store_id, store_name, rb_username, rb_password, days_back, from_date, to_date, note`

例:
```
active,store_id,store_name,rb_username,rb_password,days_back,from_date,to_date,note
true,store001,テスト店舗1,user001,pass001,7,,,
true,store002,テスト店舗2,user002,pass002,7,,,
```

### 4. BigQueryのセットアップ

```bash
# SQLファイル内のプロジェクトIDを置換して実行
sed "s/{PROJECT_ID}/$PROJECT_ID/g" sql/setup_bigquery.sql | \
bq query --use_legacy_sql=false
```

### 5. デプロイ

```bash
# デプロイスクリプトを実行
./deploy.sh
```

## 使用方法

### 手動実行

```bash
# 通常のETL処理
node src/index.js

# 手動テスト（CSVファイル指定）
node src/index.js manual /path/to/test.csv
```

### Cloud Runでの実行

```bash
# Cloud Runサービスを呼び出し
gcloud run services call restaurant-board-etl --region=asia-northeast1
```

### Connected Sheetsでの表示

1. Google Sheetsで「データ」→「データコネクタ」を選択
2. BigQueryに接続
3. 以下のカスタムSQLを使用:

```sql
SELECT store_id, store_name, reserve_date, booking_date,
       start_time, end_time, course_name, headcount, channel, status
FROM `{PROJECT_ID}.rb.vw_reservation_details`
WHERE reserve_date BETWEEN @from AND @to
ORDER BY reserve_date, store_id;
```

## データモデル

### 共通カラム定義

| 表示名 | 物理名 | 型 | 必須 | 説明 |
|--------|--------|----|----|------|
| 店舗_ID | store_id | STRING | ◯ | 店舗ID |
| 店舗名 | store_name | STRING | △ | 表示用名称 |
| 予約日 | reserve_date | DATE | ◯ | 来店日 |
| 予約受付日 | booking_date | DATE | △ | 受付日/登録日 |
| 予約開始時間 | start_time | TIME | △ | HH:MM:SS |
| 予約終了時間 | end_time | TIME | △ | HH:MM:SS |
| コース名 | course_name | STRING | △ | プラン名/メニュー名 |
| 人数 | headcount | INT64 | △ | 名数 |
| 経路 | channel | STRING | △ | 媒体/流入元 |
| 予約ステータス | status | STRING | △ | 例: 確定/キャンセル |
| ベンダー | vendor | STRING | ◯ | 固定値 'restaurant_board' |
| 取込時刻 | ingestion_ts | TIMESTAMP | ◯ | 取込実行のタイムスタンプ |
| 実行ID | run_id | STRING | ◯ | バッチ実行の識別子 |
| レコードキー | record_key | STRING | ◯ | 予約の恒久キー |
| 内容ハッシュ | record_hash | STRING | ◯ | 内容のMD5 |

### BigQueryテーブル

- **ステージテーブル**: `rb.stage_reservations_rb` (TTL: 30日)
- **本番テーブル**: `rb.reservations_rb` (パーティション: reserve_date, クラスタ: store_id, channel)

## 運用

### スケジュール実行

- **実行時刻**: 毎日 01:15 JST
- **処理窓**: 前日 + 過去7日（可変）
- **冪等性**: 同一run_idの再実行で重複が生じない

### エラーハンドリング

- ログイン失敗/ダウンロード失敗は店舗単位で継続
- 致命的失敗時は非ゼロ終了（再実行可能）
- 最大3回リトライ（指数バックオフ）

### 監視

- Cloud Loggingでログ確認
- 実行件数/失敗率/処理時間/ロード行数をログ出力
- 将来: アラート連携（メール/Slack等）

## トラブルシューティング

### よくある問題

1. **ログイン失敗**
   - 2FA認証が必要な店舗はMVP対象外
   - パスワード変更の確認

2. **CSVダウンロード失敗**
   - レストランボード画面の構造変更
   - セレクタの更新が必要

3. **BigQueryロード失敗**
   - スキーマ不整合
   - 権限不足

4. **Connected Sheets表示エラー**
   - カスタムSQLの日付パラメータ設定
   - 権限設定の確認

### ログ確認

```bash
# Cloud Loggingでログ確認
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=restaurant-board-etl" --limit=100 --format=json
```

## 今後の拡張予定

- **フェーズ2**: 他ベンダー追加（Ebica/TORETA/TableCheck）
- **フェーズ3**: FastAPI + Reactアプリ
- **セキュリティ**: Secret Managerへの資格情報移行
- **監視**: アラート機能の追加

## ライセンス

MIT License
