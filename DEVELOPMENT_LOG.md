# 開発ログ

## プロジェクト概要

レストランボードの予約データを自動取得し、BigQueryに格納するETLシステムの開発記録です。要件定義書v1.1に基づき、Node.jsで完結するMVP版として実装しました。

## 実装完了項目

### ✅ 基本構造
- [x] プロジェクト構造の作成
- [x] package.json の設定
- [x] Dockerfile の作成
- [x] 環境変数設定ファイル

### ✅ コア機能
- [x] メインアプリケーション (`src/index.js`)
- [x] 設定管理 (`src/config/index.js`)
- [x] ログ管理 (`src/utils/logger.js`)

### ✅ サービス層
- [x] 店舗マスタサービス (`src/services/store-service.js`)
  - Google Sheetsからの店舗データ取得
  - アクティブ店舗のフィルタリング
- [x] スクレイピングサービス (`src/services/scraper-service.js`)
  - Puppeteerによるレストランボードスクレイピング
  - ログイン処理
  - CSV ダウンロード
  - 文字コード変換 (Shift_JIS → UTF-8)
- [x] データ変換サービス (`src/services/transform-service.js`)
  - CSV パース
  - 共通カラムへの正規化
  - データ型変換
  - レコードキー・ハッシュ生成
- [x] GCS サービス (`src/services/gcs-service.js`)
  - 変換済みデータのアップロード
  - ファイル管理
- [x] BigQuery サービス (`src/services/bigquery-service.js`)
  - ステージテーブルへのロード
  - MERGE操作による差分反映

### ✅ SQL スクリプト
- [x] データセット作成 (`sql/create_dataset.sql`)
- [x] ステージテーブル作成 (`sql/create_stage_table.sql`)
- [x] 本番テーブル作成 (`sql/create_production_table.sql`)
- [x] MERGE処理 (`sql/merge_reservations_rb.sql`)
- [x] Connected Sheets用クエリ (`sql/connected_sheets_query.sql`)
- [x] 一括セットアップ (`sql/setup_bigquery.sql`)

### ✅ デプロイメント
- [x] Cloud Build設定 (`cloudbuild.yaml`)
- [x] 自動デプロイスクリプト (`deploy.sh`)
- [x] Cloud Scheduler設定

### ✅ 設定・ドキュメント
- [x] スキーマ定義 (`config/rb-schema.json`)
- [x] README.md
- [x] 環境設定ファイル
- [x] .gitignore / .dockerignore

## 技術的な実装ポイント

### データフロー
1. **Google Sheets** → 店舗マスタ取得
2. **Puppeteer** → レストランボードスクレイピング
3. **csv-parse** → CSV解析
4. **iconv-lite** → 文字コード変換
5. **Transform Service** → 共通カラムへ正規化
6. **GCS** → 変換済みデータ保存
7. **BigQuery** → ステージテーブルロード → MERGE

### 重複排除・差分反映
- `record_key`: 予約の一意キー（予約番号 or 合成キー）
- `record_hash`: 内容のMD5ハッシュ（変更検知用）
- MERGE処理で INSERT/UPDATE/スキップを自動判定

### パフォーマンス最適化
- BigQuery パーティション: `reserve_date` (本番), `ingestion_ts` (ステージ)
- クラスタ: `store_id, channel` (本番), `store_id, vendor, run_id` (ステージ)
- ステージテーブル TTL: 14日自動削除

### エラーハンドリング
- 店舗単位での処理継続
- リトライ機能（指数バックオフ）
- 詳細ログ出力

## 要件定義書との対応

| 要件 | 実装状況 | 備考 |
|------|----------|------|
| FR-1: 店舗マスタ | ✅ 完了 | Google Sheets API連携 |
| FR-2: スクレイピング | ✅ 完了 | Puppeteer + リトライ機能 |
| FR-3: 整形 | ✅ 完了 | Node.js内で完結 |
| FR-4: GCS配置 | ✅ 完了 | 階層化されたパス構造 |
| FR-5: BQ取り込み | ✅ 完了 | Load Job実装 |
| FR-6: MERGE | ✅ 完了 | 差分反映ロジック |
| FR-7: Connected Sheets | ✅ 完了 | カスタムSQL提供 |
| FR-8: 手動テスト | ✅ 完了 | 手動アップロード機能 |

## 今後の拡張予定

### フェーズ2
- [ ] 他ベンダー対応（Ebica, TORETA, TableCheck）
- [ ] Secret Manager連携
- [ ] 前計算テーブル (`fact_daily_metrics`)
- [ ] 整形層のPython分離（必要に応じて）

### フェーズ3
- [ ] FastAPI/React アプリケーション
- [ ] ロール別アクセス制御
- [ ] KPI ダッシュボード

## 運用開始時の確認項目

### セットアップ
- [ ] Google Cloud プロジェクト設定
- [ ] サービスアカウント権限
- [ ] Google Sheets 店舗マスタ作成
- [ ] 環境変数設定

### 動作確認
- [ ] 手動実行テスト
- [ ] スケジュール実行確認
- [ ] ログ出力確認
- [ ] BigQuery データ確認
- [ ] Connected Sheets 表示確認

### 監視・保守
- [ ] Cloud Logging アラート設定
- [ ] 実行失敗通知設定
- [ ] コスト監視設定

## 技術債務・改善点

### 現在の制限事項
- レストランボードのセレクタがハードコード
- 2FA対応店舗は手動除外
- エラー通知機能未実装

### 将来の改善
- セレクタの外部設定化
- より詳細なエラー分類
- パフォーマンス監視の強化
- テストカバレッジの向上

## 開発環境

- **Node.js**: 18+
- **主要ライブラリ**: puppeteer, @google-cloud/bigquery, @google-cloud/storage
- **開発ツール**: winston (ログ), dayjs (日時), crypto (ハッシュ)
- **インフラ**: Google Cloud Run, BigQuery, Cloud Storage, Cloud Scheduler

## 参考リンク

- [要件定義書 v1.1](./予約データetl／ダッシュボード（レストランボード先行）要件定義書_v_1.md)
- [BigQuery パーティション設計](https://cloud.google.com/bigquery/docs/partitioned-tables)
- [Cloud Run ベストプラクティス](https://cloud.google.com/run/docs/best-practices)
- [Puppeteer ドキュメント](https://pptr.dev/)
