const { google } = require('googleapis');
const logger = require('../utils/logger');
const config = require('../config');

class StoreService {
  constructor() {
    this.sheets = null;
  }

  async initialize() {
    if (this.sheets) return;

    try {
      // Initialize Google Sheets API client
      const auth = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
        keyFile: config.GOOGLE_APPLICATION_CREDENTIALS
      });

      this.sheets = google.sheets({ version: 'v4', auth });
      logger.info('Google Sheets API client initialized');
    } catch (error) {
      logger.error('Failed to initialize Google Sheets API:', error);
      throw error;
    }
  }

  async getActiveStores() {
    try {
      await this.initialize();

      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: config.STORES_SHEET_ID,
        range: `${config.STORES_SHEET_NAME}!A:I`, // Adjust range as needed
      });

      const rows = response.data.values;
      if (!rows || rows.length === 0) {
        logger.warn('No data found in stores sheet');
        return [];
      }

      // Parse header row
      const headers = rows[0].map(header => header.toLowerCase().trim());
      logger.debug('Sheet headers:', headers);

      // Parse data rows
      const stores = [];
      for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        if (!row || row.length === 0) continue;

        const store = {};
        headers.forEach((header, index) => {
          store[header] = row[index] || '';
        });

        // Only include active stores
        if (store.active && store.active.toLowerCase() === 'true') {
          // Validate required fields
          if (!store.store_id || !store.rb_username || !store.rb_password) {
            logger.warn(`Skipping store at row ${i + 1}: missing required fields`);
            continue;
          }

          // Set defaults
          store.days_back = parseInt(store.days_back) || config.DAYS_BACK;
          
          stores.push(store);
        }
      }

      logger.info(`Retrieved ${stores.length} active stores from sheet`);
      return stores;

    } catch (error) {
      logger.error('Failed to get stores from sheet:', error);
      throw error;
    }
  }

  // Validate store configuration
  validateStore(store) {
    const required = ['store_id', 'store_name', 'rb_username', 'rb_password'];
    const missing = required.filter(field => !store[field]);
    
    if (missing.length > 0) {
      throw new Error(`Store ${store.store_id || 'unknown'} missing required fields: ${missing.join(', ')}`);
    }

    return true;
  }
}

module.exports = StoreService;
