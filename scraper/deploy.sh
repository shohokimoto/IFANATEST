#!/bin/bash

# Node.js Scraper Service デプロイスクリプト
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

# 共通設定ファイルから環境変数を読み込み
if [ -f "../.env" ]; then
    export $(cat ../.env | grep -v '^#' | grep -v '^$' | xargs)
    log_info "共通設定ファイルから環境変数を読み込みました"
elif [ -f ".env" ]; then
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
    log_info "ローカル設定ファイルから環境変数を読み込みました"
fi

# PROJECT_IDの確認
if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$PROJECT_ID" ]; then
        log_error "Google Cloud プロジェクトが設定されていません"
        echo "以下のいずれかを実行してください:"
        echo "1. ../.env ファイルに PROJECT_ID を設定"
        echo "2. gcloud config set project YOUR_PROJECT_ID"
        exit 1
    fi
fi

log_info "Node.js Scraper Service デプロイ開始: 環境=$ENVIRONMENT, プロジェクト=$PROJECT_ID"

# 必要なAPIが有効化されているかチェック
log_info "必要なAPIの有効化をチェック中..."
gcloud services enable cloudrun.googleapis.com cloudbuild.googleapis.com --project=$PROJECT_ID

# サービス名とリージョン設定
SERVICE_NAME="restaurant-board-scraper"
REGION=${REGION:-asia-northeast1}

log_info "サービス名: $SERVICE_NAME"
log_info "リージョン: $REGION"

# Cloud Build でイメージをビルド
log_info "Docker イメージをビルド中..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME:$ENVIRONMENT --project=$PROJECT_ID

# Cloud Run にデプロイ
log_info "Cloud Run にデプロイ中..."
gcloud run deploy $SERVICE_NAME \
    --image gcr.io/$PROJECT_ID/$SERVICE_NAME:$ENVIRONMENT \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --port 3001 \
    --memory 1Gi \
    --cpu 1 \
    --timeout 300 \
    --concurrency 10 \
    --max-instances 10 \
    --project=$PROJECT_ID

# デプロイ完了
log_info "デプロイ完了！"
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format="value(status.url)" --project=$PROJECT_ID)
log_info "サービスURL: $SERVICE_URL"

# ヘルスチェック
log_info "ヘルスチェック中..."
sleep 10
if curl -f -s "$SERVICE_URL/health" > /dev/null; then
    log_info "✅ ヘルスチェック成功"
else
    log_warn "⚠️ ヘルスチェック失敗 - サービスが起動中かもしれません"
fi

log_info "Node.js Scraper Service のデプロイが完了しました"
log_info "サービスURL: $SERVICE_URL"
