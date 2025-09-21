/**
 * 店舗マスタ（Google Sheets）読み取りサービス
 */
const { google } = require('googleapis');
const logger = require('../utils/logger');
const config = require('../config');

class StoreMasterService {
  constructor() {
    this.sheets = google.sheets({ version: 'v4' });
  }

  /**
   * Google Sheetsから店舗マスタデータを取得
   * @returns {Array} アクティブな店舗の配列
   */
  async getActiveStores() {
    try {
      logger.info('店舗マスタの取得を開始', { sheetId: config.storesSheetId });

      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: config.storesSheetId,
        range: 'Stores!A:I', // active, store_id, store_name, rb_username, rb_password, days_back, from_date, to_date, note
        auth: null // サービスアカウント認証を使用
      });

      const rows = response.data.values;
      if (!rows || rows.length < 2) {
        logger.warn('店舗マスタデータが見つかりません');
        return [];
      }

      // ヘッダー行をスキップ
      const headers = rows[0];
      const dataRows = rows.slice(1);

      const stores = dataRows
        .map((row, index) => {
          const store = {};
          headers.forEach((header, colIndex) => {
            store[header] = row[colIndex] || null;
          });
          return store;
        })
        .filter(store => {
          // active=trueの店舗のみ処理対象
          return store.active === 'true' || store.active === true;
        })
        .map(store => this.normalizeStoreData(store));

      logger.info('店舗マスタの取得完了', { 
        totalRows: rows.length - 1,
        activeStores: stores.length,
        storeIds: stores.map(s => s.store_id)
      });

      return stores;
    } catch (error) {
      logger.error('店舗マスタの取得に失敗', { error: error.message });
      throw error;
    }
  }

  /**
   * 店舗データを正規化
   * @param {Object} store 生の店舗データ
   * @returns {Object} 正規化された店舗データ
   */
  normalizeStoreData(store) {
    const normalized = {
      store_id: store.store_id?.toString().trim(),
      store_name: store.store_name?.toString().trim(),
      rb_username: store.rb_username?.toString().trim(),
      rb_password: store.rb_password?.toString().trim(),
      days_back: parseInt(store.days_back) || config.daysBack,
      from_date: store.from_date?.toString().trim() || null,
      to_date: store.to_date?.toString().trim() || null,
      note: store.note?.toString().trim() || null,
      active: true
    };

    // 必須項目の検証
    const requiredFields = ['store_id', 'rb_username', 'rb_password'];
    for (const field of requiredFields) {
      if (!normalized[field]) {
        throw new Error(`店舗 ${normalized.store_id} の必須項目が不足: ${field}`);
      }
    }

    return normalized;
  }

  /**
   * 処理期間を計算
   * @param {Object} store 店舗データ
   * @returns {Object} 開始日と終了日
   */
  calculateDateRange(store) {
    const today = new Date();
    const todayStr = today.toISOString().split('T')[0];

    let fromDate, toDate;

    if (store.from_date && store.to_date) {
      // 明示的に指定された期間を使用
      fromDate = store.from_date;
      toDate = store.to_date;
    } else {
      // デフォルト: 前日 + days_back日
      const from = new Date(today);
      from.setDate(from.getDate() - 1 - store.days_back);
      fromDate = from.toISOString().split('T')[0];
      toDate = today.toISOString().split('T')[0];
    }

    return { fromDate, toDate };
  }
}

module.exports = StoreMasterService;
