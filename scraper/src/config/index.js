/**
 * スクレイピングサービス設定
 */
require('dotenv').config();

const config = {
  // サーバー設定
  port: process.env.PORT || 3001,
  host: process.env.HOST || '0.0.0.0',
  
  // レストランボード設定
  rbBaseUrl: process.env.RB_BASE_URL || 'https://manage.restaurant-board.jp',
  rbLoginUrl: process.env.RB_LOGIN_URL || 'https://manage.restaurant-board.jp/login',
  
  // ログ設定
  logLevel: process.env.LOG_LEVEL || 'info',
  
  // 処理設定
  maxRetries: parseInt(process.env.MAX_RETRIES) || 3,
  retryDelay: parseInt(process.env.RETRY_DELAY) || 1000, // 1秒
  timeout: parseInt(process.env.TIMEOUT) || 30000, // 30秒
  
  // ブラウザ設定
  headless: process.env.HEADLESS !== 'false',
  executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable',
  
  // ファイル設定
  downloadPath: process.env.DOWNLOAD_PATH || '/tmp',
  
  // セキュリティ設定
  corsOrigin: process.env.CORS_ORIGIN || '*',
  maxRequestSize: process.env.MAX_REQUEST_SIZE || '10mb'
};

// 環境変数の検証
const requiredConfigs = [];
for (const configKey of requiredConfigs) {
  if (!config[configKey]) {
    throw new Error(`必須設定が未設定です: ${configKey}`);
  }
}

module.exports = config;
