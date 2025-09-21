/**
 * 設定管理モジュール
 */
require('dotenv').config();

const config = {
  // Google Cloud設定
  projectId: process.env.PROJECT_ID || 'your-project-id',
  region: process.env.REGION || 'asia-northeast1',
  bqDataset: process.env.BQ_DATASET || 'rb',
  gcsBucket: process.env.GCS_BUCKET || 'your-gcs-bucket',
  
  // Google Sheets設定
  storesSheetId: process.env.STORES_SHEET_ID || 'your-sheet-id',
  
  // 処理設定
  daysBack: parseInt(process.env.DAYS_BACK) || 7,
  fromDate: process.env.FROM_DATE || null,
  toDate: process.env.TO_DATE || null,
  
  // レストランボード設定
  rbBaseUrl: process.env.RB_BASE_URL || 'https://manage.restaurant-board.jp',
  rbLoginUrl: process.env.RB_LOGIN_URL || 'https://manage.restaurant-board.jp/login',
  
  // ログ設定
  logLevel: process.env.LOG_LEVEL || 'info',
  
  // 処理設定
  maxRetries: 3,
  retryDelay: 1000, // 1秒
  timeout: 30000, // 30秒
  
  // BigQuery設定
  bqTablePrefix: 'reservations_rb',
  stageTableName: 'stage_reservations_rb',
  mainTableName: 'reservations_rb',
  
  // GCS設定
  gcsPathPrefix: 'landing/restaurant_board',
  manualPathPrefix: 'manual/restaurant_board'
};

// 必須設定の検証
const requiredConfigs = ['projectId', 'storesSheetId', 'gcsBucket'];
for (const configKey of requiredConfigs) {
  if (!config[configKey] || config[configKey].includes('your-')) {
    throw new Error(`必須設定が未設定です: ${configKey}`);
  }
}

module.exports = config;
