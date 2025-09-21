const path = require('path');

const config = {
  // Google Cloud Configuration
  PROJECT_ID: process.env.PROJECT_ID || 'your-project-id',
  BQ_DATASET: process.env.BQ_DATASET || 'rb',
  GCS_BUCKET: process.env.GCS_BUCKET || 'your-bucket-name',
  REGION: process.env.REGION || 'asia-northeast1',

  // Google Sheets Configuration
  STORES_SHEET_ID: process.env.STORES_SHEET_ID || '',
  STORES_SHEET_NAME: 'Stores',

  // Processing Configuration
  DAYS_BACK: parseInt(process.env.DAYS_BACK) || 7,
  FROM_DATE: process.env.FROM_DATE || '',
  TO_DATE: process.env.TO_DATE || '',
  DRY_RUN: process.env.DRY_RUN === 'true',

  // Logging Configuration
  LOG_LEVEL: process.env.LOG_LEVEL || 'info',

  // Retry Configuration
  RETRY_MAX: parseInt(process.env.RETRY_MAX) || 3,
  RETRY_BACKOFF_MS: parseInt(process.env.RETRY_BACKOFF_MS) || 1000,

  // BigQuery Configuration
  BQ_STAGE_TABLE: 'stage_reservations_rb',
  BQ_PRODUCTION_TABLE: 'reservations_rb',

  // GCS Configuration
  GCS_LANDING_PREFIX: 'landing/restaurant_board',
  GCS_MANUAL_PREFIX: 'manual/restaurant_board',

  // Scraping Configuration
  SCRAPER_TIMEOUT: 30000,
  SCRAPER_WAIT_TIMEOUT: 5000,

  // Restaurant Board Specific Configuration
  RB: {
    LOGIN_URL: 'https://restaurant-board.com/login',
    BASE_URL: 'https://restaurant-board.com',
    SELECTORS: {
      USERNAME: 'input[name="username"]',
      PASSWORD: 'input[name="password"]',
      LOGIN_BUTTON: 'button[type="submit"]',
      DATE_FROM: 'input[name="date_from"]',
      DATE_TO: 'input[name="date_to"]',
      DOWNLOAD_BUTTON: '.download-csv-btn',
      LOGOUT_BUTTON: '.logout-btn'
    }
  },

  // File paths
  TEMP_DIR: '/tmp',
  SQL_DIR: path.join(__dirname, '../../sql'),

  // Service Account (for local development)
  GOOGLE_APPLICATION_CREDENTIALS: process.env.GOOGLE_APPLICATION_CREDENTIALS
};

// Validation
if (!config.PROJECT_ID || config.PROJECT_ID === 'your-project-id') {
  console.warn('Warning: PROJECT_ID is not set properly');
}

if (!config.STORES_SHEET_ID) {
  console.warn('Warning: STORES_SHEET_ID is not set');
}

if (!config.GCS_BUCKET || config.GCS_BUCKET === 'your-bucket-name') {
  console.warn('Warning: GCS_BUCKET is not set properly');
}

module.exports = config;
