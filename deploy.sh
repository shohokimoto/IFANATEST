#!/bin/bash

# Restaurant Board Scraper ETL Deployment Script
# This script sets up the complete infrastructure and deploys the application

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-your-project-id}"
REGION="${REGION:-asia-northeast1}"
SERVICE_NAME="rb-scraper-etl"
GCS_BUCKET="${GCS_BUCKET:-${PROJECT_ID}-rb-data}"
STORES_SHEET_ID="${STORES_SHEET_ID:-your-sheet-id}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-rb-scraper-etl@${PROJECT_ID}.iam.gserviceaccount.com}"

echo "🚀 Starting deployment for Restaurant Board Scraper ETL"
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Service Name: $SERVICE_NAME"
echo "GCS Bucket: $GCS_BUCKET"

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "❌ Error: No active gcloud authentication found. Please run 'gcloud auth login'"
    exit 1
fi

# Set the project
echo "📝 Setting project to $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔌 Enabling required APIs..."
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable scheduler.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable sheets.googleapis.com

# Create GCS bucket if it doesn't exist
echo "🪣 Creating GCS bucket: $GCS_BUCKET"
if ! gsutil ls -b gs://$GCS_BUCKET > /dev/null 2>&1; then
    gsutil mb -l $REGION gs://$GCS_BUCKET
    echo "✅ Created GCS bucket: $GCS_BUCKET"
else
    echo "ℹ️  GCS bucket already exists: $GCS_BUCKET"
fi

# Create service account if it doesn't exist
echo "👤 Creating service account: $SERVICE_ACCOUNT"
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT > /dev/null 2>&1; then
    gcloud iam service-accounts create rb-scraper-etl \
        --display-name="Restaurant Board Scraper ETL Service Account" \
        --description="Service account for RB scraper ETL operations"
    echo "✅ Created service account: $SERVICE_ACCOUNT"
else
    echo "ℹ️  Service account already exists: $SERVICE_ACCOUNT"
fi

# Grant necessary permissions to service account
echo "🔐 Granting permissions to service account..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/sheets.readonly"

# Setup BigQuery dataset and tables
echo "📊 Setting up BigQuery dataset and tables..."
# Replace placeholders in SQL files
sed "s/{PROJECT_ID}/$PROJECT_ID/g" sql/setup_bigquery.sql > /tmp/setup_bigquery.sql

# Execute BigQuery setup
bq query --use_legacy_sql=false < /tmp/setup_bigquery.sql
echo "✅ BigQuery setup completed"

# Build and deploy using Cloud Build
echo "🏗️  Building and deploying application..."
gcloud builds submit \
    --substitutions=_GCS_BUCKET=$GCS_BUCKET,_STORES_SHEET_ID=$STORES_SHEET_ID,_SERVICE_ACCOUNT=$SERVICE_ACCOUNT

# Create Cloud Scheduler job
echo "⏰ Creating Cloud Scheduler job..."
SCHEDULER_JOB_NAME="rb-scraper-etl-daily"
SERVICE_URL="https://${SERVICE_NAME}-$(gcloud config get-value project | tr ':' '-' | tr '.' '-')-${REGION}.a.run.app"

# Delete existing job if it exists
if gcloud scheduler jobs describe $SCHEDULER_JOB_NAME --location=$REGION > /dev/null 2>&1; then
    gcloud scheduler jobs delete $SCHEDULER_JOB_NAME --location=$REGION --quiet
fi

# Create new scheduler job
gcloud scheduler jobs create http $SCHEDULER_JOB_NAME \
    --location=$REGION \
    --schedule="15 1 * * *" \
    --time-zone="Asia/Tokyo" \
    --uri="$SERVICE_URL" \
    --http-method=POST \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --description="Daily Restaurant Board scraping job - runs at 01:15 JST"

echo "✅ Cloud Scheduler job created: $SCHEDULER_JOB_NAME"

# Output deployment summary
echo ""
echo "🎉 Deployment completed successfully!"
echo ""
echo "📋 Deployment Summary:"
echo "  • Project ID: $PROJECT_ID"
echo "  • Cloud Run Service: $SERVICE_NAME"
echo "  • Service URL: $SERVICE_URL"
echo "  • GCS Bucket: gs://$GCS_BUCKET"
echo "  • BigQuery Dataset: $PROJECT_ID.rb"
echo "  • Cloud Scheduler Job: $SCHEDULER_JOB_NAME (runs daily at 01:15 JST)"
echo "  • Service Account: $SERVICE_ACCOUNT"
echo ""
echo "📝 Next Steps:"
echo "  1. Update your Google Sheets with store information"
echo "  2. Set the STORES_SHEET_ID in Cloud Run environment variables"
echo "  3. Test the deployment manually from Cloud Run console"
echo "  4. Monitor logs in Cloud Logging"
echo ""
echo "🔗 Useful Links:"
echo "  • Cloud Run: https://console.cloud.google.com/run/detail/$REGION/$SERVICE_NAME"
echo "  • BigQuery: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo "  • Cloud Scheduler: https://console.cloud.google.com/cloudscheduler?project=$PROJECT_ID"
echo "  • Cloud Logging: https://console.cloud.google.com/logs/query?project=$PROJECT_ID"
