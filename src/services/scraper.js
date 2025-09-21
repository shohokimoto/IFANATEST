/**
 * レストランボードスクレイピングサービス
 */
const puppeteer = require('puppeteer');
const fs = require('fs').promises;
const path = require('path');
const iconv = require('iconv-lite');
const logger = require('../utils/logger');
const config = require('../config');

class RestaurantBoardScraper {
  constructor() {
    this.browser = null;
    this.page = null;
  }

  /**
   * ブラウザを初期化
   */
  async initialize() {
    try {
      logger.info('ブラウザの初期化を開始');
      
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
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable'
      });

      this.page = await this.browser.newPage();
      
      // ユーザーエージェントを設定
      await this.page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36');
      
      // タイムアウト設定
      this.page.setDefaultTimeout(config.timeout);
      
      logger.info('ブラウザの初期化完了');
    } catch (error) {
      logger.error('ブラウザの初期化に失敗', { error: error.message });
      throw error;
    }
  }

  /**
   * ブラウザを終了
   */
  async cleanup() {
    try {
      if (this.page) {
        await this.page.close();
        this.page = null;
      }
      if (this.browser) {
        await this.browser.close();
        this.browser = null;
      }
      logger.info('ブラウザのクリーンアップ完了');
    } catch (error) {
      logger.error('ブラウザのクリーンアップに失敗', { error: error.message });
    }
  }

  /**
   * レストランボードから予約データをダウンロード
   * @param {Object} store 店舗情報
   * @param {string} fromDate 開始日 (YYYY-MM-DD)
   * @param {string} toDate 終了日 (YYYY-MM-DD)
   * @returns {string} ダウンロードしたCSVファイルのパス
   */
  async downloadReservationData(store, fromDate, toDate) {
    const maxRetries = config.maxRetries;
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        logger.info('スクレイピング開始', { 
          storeId: store.store_id,
          fromDate,
          toDate,
          attempt 
        });

        // ログインページに移動
        await this.page.goto(config.rbLoginUrl, { waitUntil: 'networkidle2' });
        
        // ログイン処理
        await this.performLogin(store);
        
        // 予約データページに移動
        await this.navigateToReservationPage();
        
        // 期間設定
        await this.setDateRange(fromDate, toDate);
        
        // CSVダウンロード
        const csvFilePath = await this.downloadCsv(store.store_id, fromDate, toDate);
        
        logger.info('スクレイピング完了', { 
          storeId: store.store_id,
          csvFilePath 
        });
        
        return csvFilePath;

      } catch (error) {
        lastError = error;
        logger.error('スクレイピング失敗', { 
          storeId: store.store_id,
          attempt,
          error: error.message 
        });

        if (attempt < maxRetries) {
          const delay = config.retryDelay * Math.pow(2, attempt - 1); // 指数バックオフ
          logger.info('リトライ待機中', { delay });
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    throw new Error(`スクレイピングが最大試行回数に達しました: ${lastError.message}`);
  }

  /**
   * ログイン処理
   * @param {Object} store 店舗情報
   */
  async performLogin(store) {
    try {
      // ユーザー名入力
      await this.page.waitForSelector('input[name="username"], input[name="user_id"], input[name="email"]', { timeout: 10000 });
      await this.page.type('input[name="username"], input[name="user_id"], input[name="email"]', store.rb_username);

      // パスワード入力
      await this.page.waitForSelector('input[name="password"]', { timeout: 10000 });
      await this.page.type('input[name="password"]', store.rb_password);

      // ログインボタンクリック
      await this.page.click('button[type="submit"], input[type="submit"], .login-button');
      
      // ログイン完了を待機
      await this.page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });

      // 2FAチェック（簡単な判定）
      const currentUrl = this.page.url();
      if (currentUrl.includes('2fa') || currentUrl.includes('verify') || currentUrl.includes('sms')) {
        throw new Error('2FA認証が必要な店舗です。MVP対象外とします。');
      }

      logger.info('ログイン完了', { storeId: store.store_id });

    } catch (error) {
      logger.error('ログイン失敗', { storeId: store.store_id, error: error.message });
      throw error;
    }
  }

  /**
   * 予約データページに移動
   */
  async navigateToReservationPage() {
    try {
      // 予約管理メニューを探してクリック
      const reservationSelectors = [
        'a[href*="reservation"]',
        'a[href*="booking"]',
        '.menu-reservation',
        '[data-menu="reservation"]',
        'a:contains("予約")',
        'a:contains("Reservation")'
      ];

      let found = false;
      for (const selector of reservationSelectors) {
        try {
          await this.page.waitForSelector(selector, { timeout: 5000 });
          await this.page.click(selector);
          await this.page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 });
          found = true;
          break;
        } catch (e) {
          continue;
        }
      }

      if (!found) {
        throw new Error('予約管理ページが見つかりません');
      }

      logger.info('予約管理ページに移動完了');

    } catch (error) {
      logger.error('予約管理ページへの移動に失敗', { error: error.message });
      throw error;
    }
  }

  /**
   * 期間設定
   * @param {string} fromDate 開始日
   * @param {string} toDate 終了日
   */
  async setDateRange(fromDate, toDate) {
    try {
      // 日付入力フィールドを探して設定
      const dateSelectors = [
        'input[name="from_date"]',
        'input[name="start_date"]',
        'input[name="date_from"]',
        '.date-from input',
        '#from-date'
      ];

      for (const selector of dateSelectors) {
        try {
          await this.page.waitForSelector(selector, { timeout: 3000 });
          await this.page.type(selector, fromDate);
          break;
        } catch (e) {
          continue;
        }
      }

      const toDateSelectors = [
        'input[name="to_date"]',
        'input[name="end_date"]',
        'input[name="date_to"]',
        '.date-to input',
        '#to-date'
      ];

      for (const selector of toDateSelectors) {
        try {
          await this.page.waitForSelector(selector, { timeout: 3000 });
          await this.page.type(selector, toDate);
          break;
        } catch (e) {
          continue;
        }
      }

      // 検索ボタンをクリック
      const searchSelectors = [
        'button[type="submit"]',
        '.search-button',
        '.btn-search',
        'input[type="submit"]'
      ];

      for (const selector of searchSelectors) {
        try {
          await this.page.waitForSelector(selector, { timeout: 3000 });
          await this.page.click(selector);
          await this.page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 10000 });
          break;
        } catch (e) {
          continue;
        }
      }

      logger.info('期間設定完了', { fromDate, toDate });

    } catch (error) {
      logger.error('期間設定に失敗', { error: error.message });
      throw error;
    }
  }

  /**
   * CSVダウンロード
   * @param {string} storeId 店舗ID
   * @param {string} fromDate 開始日
   * @param {string} toDate 終了日
   * @returns {string} CSVファイルパス
   */
  async downloadCsv(storeId, fromDate, toDate) {
    try {
      // CSVエクスポートボタンを探してクリック
      const exportSelectors = [
        'a[href*="csv"]',
        'a[href*="export"]',
        '.csv-export',
        '.btn-csv',
        'button:contains("CSV")',
        'a:contains("CSV")'
      ];

      let downloadStarted = false;
      for (const selector of exportSelectors) {
        try {
          await this.page.waitForSelector(selector, { timeout: 3000 });
          
          // ダウンロード開始を監視
          const client = await this.page.target().createCDPSession();
          await client.send('Page.setDownloadBehavior', {
            behavior: 'allow',
            downloadPath: '/tmp'
          });

          await this.page.click(selector);
          downloadStarted = true;
          break;
        } catch (e) {
          continue;
        }
      }

      if (!downloadStarted) {
        throw new Error('CSVエクスポートボタンが見つかりません');
      }

      // ダウンロード完了を待機
      await new Promise(resolve => setTimeout(resolve, 5000));

      // ダウンロードされたファイルを探す
      const files = await fs.readdir('/tmp');
      const csvFile = files.find(file => 
        file.includes(storeId) || 
        file.includes('reservation') || 
        file.endsWith('.csv')
      );

      if (!csvFile) {
        throw new Error('ダウンロードされたCSVファイルが見つかりません');
      }

      const originalPath = `/tmp/${csvFile}`;
      const newPath = `/tmp/rb_${storeId}_${fromDate}_${toDate}.csv`;

      // ファイルをリネーム
      await fs.rename(originalPath, newPath);

      // Shift_JISからUTF-8に変換
      await this.convertEncoding(newPath);

      logger.info('CSVダウンロード完了', { 
        storeId, 
        originalFile: csvFile,
        finalPath: newPath 
      });

      return newPath;

    } catch (error) {
      logger.error('CSVダウンロードに失敗', { error: error.message });
      throw error;
    }
  }

  /**
   * 文字エンコーディング変換 (Shift_JIS -> UTF-8)
   * @param {string} filePath ファイルパス
   */
  async convertEncoding(filePath) {
    try {
      const buffer = await fs.readFile(filePath);
      const utf8Content = iconv.decode(buffer, 'Shift_JIS');
      await fs.writeFile(filePath, utf8Content, 'utf8');
      
      logger.info('文字エンコーディング変換完了', { filePath });
    } catch (error) {
      logger.warn('文字エンコーディング変換に失敗（UTF-8として処理）', { 
        filePath, 
        error: error.message 
      });
    }
  }
}

module.exports = RestaurantBoardScraper;
