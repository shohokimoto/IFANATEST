const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const iconv = require('iconv-lite');
const pRetry = require('p-retry');
const logger = require('../utils/logger');
const config = require('../config');

class ScraperService {
  constructor() {
    this.browser = null;
  }

  async initialize() {
    if (this.browser) return;

    try {
      this.browser = await puppeteer.launch({
        headless: true,
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas',
          '--no-first-run',
          '--no-zygote',
          '--disable-gpu'
        ],
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined
      });
      
      logger.info('Puppeteer browser initialized');
    } catch (error) {
      logger.error('Failed to initialize Puppeteer browser:', error);
      throw error;
    }
  }

  async close() {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
      logger.info('Puppeteer browser closed');
    }
  }

  async scrapeReservations(store, dateRange) {
    try {
      await this.initialize();

      const result = await pRetry(
        () => this.scrapeWithRetry(store, dateRange),
        {
          retries: config.RETRY_MAX,
          factor: 2,
          minTimeout: config.RETRY_BACKOFF_MS,
          onFailedAttempt: error => {
            logger.warn(`Scraping attempt ${error.attemptNumber} failed for store ${store.store_id}: ${error.message}`);
          }
        }
      );

      return result;

    } catch (error) {
      logger.error(`Failed to scrape reservations for store ${store.store_id} after all retries:`, error);
      throw error;
    }
  }

  async scrapeWithRetry(store, dateRange) {
    const page = await this.browser.newPage();
    
    try {
      // Set viewport and user agent
      await page.setViewport({ width: 1280, height: 720 });
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36');

      // Navigate to login page
      logger.debug(`Navigating to RB login page for store ${store.store_id}`);
      await page.goto(config.RB.LOGIN_URL, { 
        waitUntil: 'networkidle2',
        timeout: config.SCRAPER_TIMEOUT 
      });

      // Login
      await this.login(page, store);

      // Navigate to reservation data page and download CSV
      const csvData = await this.downloadReservationCSV(page, store, dateRange);

      return csvData;

    } catch (error) {
      logger.error(`Error during scraping for store ${store.store_id}:`, error);
      throw error;
    } finally {
      await page.close();
    }
  }

  async login(page, store) {
    try {
      logger.debug(`Logging in to RB for store ${store.store_id}`);

      // Wait for login form
      await page.waitForSelector(config.RB.SELECTORS.USERNAME, { 
        timeout: config.SCRAPER_WAIT_TIMEOUT 
      });

      // Fill in credentials
      await page.type(config.RB.SELECTORS.USERNAME, store.rb_username);
      await page.type(config.RB.SELECTORS.PASSWORD, store.rb_password);

      // Click login button
      await Promise.all([
        page.waitForNavigation({ waitUntil: 'networkidle2' }),
        page.click(config.RB.SELECTORS.LOGIN_BUTTON)
      ]);

      // Check if login was successful (adjust selector based on actual RB interface)
      const loginSuccess = await page.evaluate(() => {
        return !document.querySelector('.error-message') && 
               !document.querySelector('.login-error');
      });

      if (!loginSuccess) {
        throw new Error('Login failed - check credentials');
      }

      logger.debug(`Successfully logged in to RB for store ${store.store_id}`);

    } catch (error) {
      logger.error(`Login failed for store ${store.store_id}:`, error);
      throw error;
    }
  }

  async downloadReservationCSV(page, store, dateRange) {
    try {
      logger.debug(`Downloading reservation CSV for store ${store.store_id}, date range: ${dateRange.from} to ${dateRange.to}`);

      // Navigate to reservation data page (adjust URL based on actual RB interface)
      await page.goto(`${config.RB.BASE_URL}/reservations`, {
        waitUntil: 'networkidle2',
        timeout: config.SCRAPER_TIMEOUT
      });

      // Set date range
      await page.waitForSelector(config.RB.SELECTORS.DATE_FROM);
      await page.evaluate(() => document.querySelector(config.RB.SELECTORS.DATE_FROM).value = '');
      await page.type(config.RB.SELECTORS.DATE_FROM, dateRange.from);

      await page.waitForSelector(config.RB.SELECTORS.DATE_TO);
      await page.evaluate(() => document.querySelector(config.RB.SELECTORS.DATE_TO).value = '');
      await page.type(config.RB.SELECTORS.DATE_TO, dateRange.to);

      // Set up download handling
      const downloadPath = path.join(config.TEMP_DIR, `rb_${store.store_id}_${Date.now()}.csv`);
      
      // Configure download behavior
      await page._client.send('Page.setDownloadBehavior', {
        behavior: 'allow',
        downloadPath: config.TEMP_DIR
      });

      // Click download button
      await page.click(config.RB.SELECTORS.DOWNLOAD_BUTTON);

      // Wait for download to complete (adjust timing as needed)
      await page.waitForTimeout(5000);

      // Find the downloaded file (may have different name)
      const files = fs.readdirSync(config.TEMP_DIR);
      const csvFile = files.find(file => 
        file.endsWith('.csv') && 
        file.includes('reservation') // Adjust pattern based on actual filename
      );

      if (!csvFile) {
        throw new Error('CSV file not found after download');
      }

      const csvFilePath = path.join(config.TEMP_DIR, csvFile);

      // Read and convert file from Shift_JIS to UTF-8
      const rawData = fs.readFileSync(csvFilePath);
      const utf8Data = iconv.decode(rawData, 'shift_jis');

      // Clean up downloaded file
      fs.unlinkSync(csvFilePath);

      logger.debug(`Successfully downloaded and converted CSV for store ${store.store_id}`);
      return utf8Data;

    } catch (error) {
      logger.error(`Failed to download reservation CSV for store ${store.store_id}:`, error);
      throw error;
    }
  }

  // Graceful shutdown
  async shutdown() {
    await this.close();
  }
}

module.exports = ScraperService;
