# レストランボード予約データETLシステム - Python版

FastAPI + Node.js Scraper + Reactの構成による、スケーラブルな予約データETLシステムです。

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

## 特徴

### 🚀 **高性能**
- **FastAPI**: 非同期処理による高速API
- **pandas**: 高速データ処理
- **非同期I/O**: 効率的な並行処理

### 🔒 **型安全性**
- **Pydantic**: データ検証・シリアライゼーション
- **mypy**: 静的型チェック
- **構造化ログ**: 型安全なログ出力

### 📊 **データ処理**
- **BigQuery**: 大規模データ分析
- **GCS**: スケーラブルストレージ
- **pandas**: 柔軟なデータ変換

### 🛠️ **開発効率**
- **自動API文書**: Swagger UI
- **ホットリロード**: 開発時の自動再起動
- **包括的テスト**: pytest

## セットアップ

### 1. 前提条件

- Python 3.11以上
- Google Cloud Project
- Node.js 20以上（Scraper用）

### 2. 環境設定

```bash
# リポジトリをクローン
git clone https://github.com/shohokimoto/IFANATEST.git
cd IFANATEST/backend

# 仮想環境を作成
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# または
venv\Scripts\activate     # Windows

# 依存関係をインストール
pip install -r requirements.txt
```

### 3. 環境変数の設定

```bash
# 環境変数ファイルを作成
cp env.example .env

# 必要な設定を更新
PROJECT_ID=your-project-id
STORES_SHEET_ID=your-sheet-id
GCS_BUCKET=your-gcs-bucket
```

### 4. 開発サーバーの起動

```bash
# 開発サーバーを起動
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API ドキュメント

起動後、以下のURLでAPI文書を確認できます：

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 主要エンドポイント

### ヘルスチェック
```bash
GET /health
```

### ETL処理実行
```bash
POST /api/etl/run
```

### 予約データクエリ
```bash
POST /api/query
Content-Type: application/json

{
  "from_date": "2025-01-01",
  "to_date": "2025-01-31",
  "store_ids": ["store001", "store002"],
  "limit": 1000
}
```

### スクレイピング実行
```bash
POST /api/scrape
Content-Type: application/json

{
  "store_id": "store001",
  "store_name": "テスト店舗",
  "rb_username": "user001",
  "rb_password": "pass001",
  "from_date": "2025-01-01",
  "to_date": "2025-01-02",
  "run_id": "run_001"
}
```

### 店舗一覧取得
```bash
GET /api/stores
```

### システムステータス
```bash
GET /api/status
```

## 開発

### コードフォーマット
```bash
# Blackでフォーマット
black .

# isortでインポート整理
isort .
```

### リント
```bash
# flake8でリント
flake8 .
```

### テスト
```bash
# テスト実行
pytest tests/ -v

# カバレッジ付きテスト
pytest tests/ --cov=app --cov-report=html
```

## デプロイ

### ローカルでのテスト
```bash
# 開発サーバー起動
uvicorn app.main:app --reload
```

### Cloud Runへのデプロイ
```bash
# 自動デプロイスクリプト実行
./deploy.sh
```

### Dockerでの実行
```bash
# イメージビルド
docker build -t restaurant-board-etl-api .

# コンテナ実行
docker run -p 8000:8000 \
  -e PROJECT_ID=your-project-id \
  -e STORES_SHEET_ID=your-sheet-id \
  -e GCS_BUCKET=your-bucket \
  restaurant-board-etl-api
```

## 監視・ログ

### ログレベル
- `DEBUG`: 詳細なデバッグ情報
- `INFO`: 一般的な情報
- `WARNING`: 警告
- `ERROR`: エラー
- `CRITICAL`: 致命的エラー

### ログ形式
```json
{
  "timestamp": "2025-01-27T10:30:00.000Z",
  "level": "INFO",
  "logger": "app.services.bigquery_service",
  "function": "load_data_from_gcs",
  "message": "BigQueryロード完了",
  "job_id": "job_123",
  "output_rows": 1500
}
```

### メトリクス
- **レスポンス時間**: API応答時間
- **エラー率**: 4xx/5xxエラーの割合
- **スループット**: リクエスト/秒
- **メモリ使用量**: ヒープメモリ使用量

## トラブルシューティング

### よくある問題

#### 1. **インポートエラー**
```
ModuleNotFoundError: No module named 'app'
```
**解決方法**: PYTHONPATHを設定
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

#### 2. **認証エラー**
```
google.auth.exceptions.DefaultCredentialsError
```
**解決方法**: サービスアカウントキーを設定
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
```

#### 3. **メモリ不足**
```
MemoryError: Unable to allocate array
```
**解決方法**: バッチサイズを調整
```python
# settings.py
BATCH_SIZE = 1000  # デフォルト値を小さくする
```

#### 4. **タイムアウトエラー**
```
httpx.TimeoutException
```
**解決方法**: タイムアウト値を調整
```python
# config.py
SCRAPER_TIMEOUT = 600  # 10分に延長
```

## パフォーマンス最適化

### 1. **非同期処理**
```python
# 並行処理でパフォーマンス向上
async def process_stores(stores):
    tasks = [process_store(store) for store in stores]
    results = await asyncio.gather(*tasks)
    return results
```

### 2. **キャッシュ活用**
```python
# Redis キャッシュ
@cache(expire=3600)
async def get_store_data(store_id):
    return await fetch_store_data(store_id)
```

### 3. **データベース最適化**
```sql
-- パーティション・クラスタ活用
SELECT * FROM reservations_rb
WHERE reserve_date BETWEEN '2025-01-01' AND '2025-01-31'
  AND store_id = 'store001'
```

## セキュリティ

### 1. **認証・認可**
- JWT トークンベース認証
- ロールベースアクセス制御
- API キー認証

### 2. **データ保護**
- 機密情報の自動マスキング
- HTTPS 通信の強制
- 入力値検証

### 3. **監査ログ**
- 全API呼び出しのログ記録
- データ変更履歴の追跡
- セキュリティイベントの監視

## 今後の拡張予定

### フェーズ2: 他ベンダー対応
- Ebica連携
- TORETA連携
- TableCheck連携

### フェーズ3: フロントエンド
- React ダッシュボード
- リアルタイム分析
- データ可視化

### フェーズ4: 高度な分析
- 機械学習による予測
- 異常検知
- レコメンデーション

## ライセンス

MIT License

## 貢献

プルリクエストやイシューの報告を歓迎します。

## サポート

技術的な質問やサポートが必要な場合は、GitHubのIssueでお知らせください。
