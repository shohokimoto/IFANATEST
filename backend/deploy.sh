#!/bin/bash

# FastAPI Backend デプロイスクリプト
# 使用方法: ./deploy.sh [環境]

set -e

# 色付きログ関数
log_info() {
    echo -e "\033[32m[INFO]\033[0m $1"
}

log_error() {
    echo -e "\033[31m[ERROR]\033[0m $1"
}

log_warn() {
    echo -e "\033[33m[WARN]\033[0m $1"
}

# 環境設定
ENVIRONMENT=${1:-dev}
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ]; then
    log_error "Google Cloud プロジェクトが設定されていません"
    echo "gcloud config set project YOUR_PROJECT_ID を実行してください"
    exit 1
fi

log_info "FastAPI Backend デプロイ開始: 環境=$ENVIRONMENT, プロジェクト=$PROJECT_ID"

# 必要なAPIが有効化されているかチェック
log_info "必要なAPIの有効化をチェック中..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable sheets.googleapis.com

# サービスアカウントの権限設定
log_info "サービスアカウント権限の設定中..."
SERVICE_ACCOUNT="$PROJECT_ID@$PROJECT_ID.iam.gserviceaccount.com"

# BigQuery権限
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.dataEditor"

# GCS権限
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/storage.objectAdmin"

# Google Sheets権限（読み取り専用）
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/sheets.readonly"

# BigQueryのセットアップ
log_info "BigQueryのセットアップ中..."
if [ -f "sql/setup_bigquery.sql" ]; then
    # プロジェクトIDを置換してSQLを実行
    sed "s/{PROJECT_ID}/$PROJECT_ID/g" sql/setup_bigquery.sql | \
    bq query --use_legacy_sql=false
    log_info "BigQueryセットアップ完了"
else
    log_warn "BigQueryセットアップファイルが見つかりません"
fi

# GCSバケットの作成（存在しない場合）
BUCKET_NAME="${PROJECT_ID}-restaurant-board-etl"
log_info "GCSバケットの確認中: $BUCKET_NAME"
if ! gsutil ls -b gs://$BUCKET_NAME >/dev/null 2>&1; then
    log_info "GCSバケットを作成中..."
    gsutil mb -l asia-northeast1 gs://$BUCKET_NAME
    log_info "GCSバケット作成完了"
else
    log_info "GCSバケットは既に存在します"
fi

# Cloud Schedulerの設定（APIエンドポイント用）
log_info "Cloud Schedulerの設定中..."
SCHEDULER_JOB_NAME="restaurant-board-etl-api-daily"

# 既存のジョブを削除（存在する場合）
if gcloud scheduler jobs describe $SCHEDULER_JOB_NAME --location=asia-northeast1 >/dev/null 2>&1; then
    log_info "既存のスケジューラジョブを削除中..."
    gcloud scheduler jobs delete $SCHEDULER_JOB_NAME --location=asia-northeast1 --quiet
fi

# 新しいジョブを作成（APIエンドポイントを呼び出し）
log_info "新しいスケジューラジョブを作成中..."
gcloud scheduler jobs create http $SCHEDULER_JOB_NAME \
    --schedule="15 1 * * *" \
    --time-zone="Asia/Tokyo" \
    --uri="https://restaurant-board-etl-api-[hash]-uc.a.run.app/api/etl/run" \
    --http-method=POST \
    --location=asia-northeast1 \
    --oidc-service-account-email="$SERVICE_ACCOUNT" \
    || log_warn "スケジューラジョブの作成に失敗しました（手動設定が必要）"

# Dockerイメージのビルドとプッシュ
log_info "Dockerイメージのビルド中..."
cd backend
docker build -t gcr.io/$PROJECT_ID/restaurant-board-etl-api:latest .

log_info "Dockerイメージをプッシュ中..."
docker push gcr.io/$PROJECT_ID/restaurant-board-etl-api:latest

# Cloud Runにデプロイ
log_info "Cloud Runにデプロイ中..."
gcloud run deploy restaurant-board-etl-api \
    --image gcr.io/$PROJECT_ID/restaurant-board-etl-api:latest \
    --region asia-northeast1 \
    --platform managed \
    --memory 2Gi \
    --cpu 2 \
    --timeout 3600 \
    --max-instances 10 \
    --concurrency 100 \
    --service-account $SERVICE_ACCOUNT \
    --set-env-vars "PROJECT_ID=$PROJECT_ID,REGION=asia-northeast1,BQ_DATASET=rb,GCS_BUCKET=$BUCKET_NAME,LOG_LEVEL=info,API_HOST=0.0.0.0,API_PORT=8000" \
    --allow-unauthenticated

# デプロイ完了
log_info "FastAPI Backend デプロイ完了！"
echo ""
echo "次のステップ:"
echo "1. 環境変数を設定:"
echo "   export STORES_SHEET_ID=your-sheet-id"
echo "   export DAYS_BACK=7"
echo ""
echo "2. Cloud Runサービスの環境変数を更新:"
echo "   gcloud run services update restaurant-board-etl-api --region=asia-northeast1 --set-env-vars STORES_SHEET_ID=your-sheet-id"
echo ""
echo "3. API ドキュメント確認:"
echo "   https://restaurant-board-etl-api-[hash]-uc.a.run.app/docs"
echo ""
echo "4. ETL処理実行:"
echo "   curl -X POST https://restaurant-board-etl-api-[hash]-uc.a.run.app/api/etl/run"
echo ""
echo "5. 予約データクエリ:"
echo "   curl -X POST https://restaurant-board-etl-api-[hash]-uc.a.run.app/api/query \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"from_date\": \"2025-01-01\", \"to_date\": \"2025-01-31\"}'"
