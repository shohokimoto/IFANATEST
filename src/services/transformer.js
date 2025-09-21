/**
 * データ変換・正規化サービス
 */
const fs = require('fs').promises;
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const moment = require('moment');
const crypto = require('crypto');
const logger = require('../utils/logger');

class DataTransformer {
  constructor() {
    // レストランボードの列名マッピング（実際のCSV構造に応じて調整が必要）
    this.columnMapping = {
      // 店舗情報
      '店舗ID': 'store_id',
      '店舗名': 'store_name',
      '店舗': 'store_name',
      
      // 予約日時
      '予約日': 'reserve_date',
      '来店日': 'reserve_date',
      '日付': 'reserve_date',
      '予約受付日': 'booking_date',
      '受付日': 'booking_date',
      '登録日': 'booking_date',
      '予約時間': 'start_time',
      '開始時間': 'start_time',
      '時間': 'start_time',
      '終了時間': 'end_time',
      
      // 予約内容
      'コース名': 'course_name',
      'プラン名': 'course_name',
      'メニュー名': 'course_name',
      'コース': 'course_name',
      '人数': 'headcount',
      '名数': 'headcount',
      '予約者数': 'headcount',
      
      // 経路・ステータス
      '経路': 'channel',
      '媒体': 'channel',
      '流入元': 'channel',
      '予約ステータス': 'status',
      'ステータス': 'status',
      '状態': 'status',
      
      // その他
      '予約番号': 'reservation_id',
      'ID': 'reservation_id',
      '備考': 'note',
      'メモ': 'note'
    };
  }

  /**
   * CSVファイルを読み込んで正規化
   * @param {string} csvFilePath CSVファイルパス
   * @param {Object} store 店舗情報
   * @param {string} runId 実行ID
   * @returns {string} 正規化されたCSVファイルのパス
   */
  async transformCsv(csvFilePath, store, runId) {
    try {
      logger.info('CSV変換開始', { 
        storeId: store.store_id,
        csvFilePath,
        runId 
      });

      const rawData = await this.readCsvFile(csvFilePath);
      const normalizedData = await this.normalizeData(rawData, store, runId);
      const outputPath = await this.writeNormalizedCsv(normalizedData, store.store_id, runId);

      logger.info('CSV変換完了', { 
        storeId: store.store_id,
        inputRows: rawData.length,
        outputRows: normalizedData.length,
        outputPath 
      });

      return outputPath;

    } catch (error) {
      logger.error('CSV変換に失敗', { 
        storeId: store.store_id,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * CSVファイルを読み込み
   * @param {string} filePath ファイルパス
   * @returns {Array} 生データの配列
   */
  async readCsvFile(filePath) {
    return new Promise((resolve, reject) => {
      const results = [];
      
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', () => resolve(results))
        .on('error', (error) => reject(error));
    });
  }

  /**
   * データを正規化
   * @param {Array} rawData 生データ
   * @param {Object} store 店舗情報
   * @param {string} runId 実行ID
   * @returns {Array} 正規化されたデータ
   */
  async normalizeData(rawData, store, runId) {
    const normalizedData = [];
    const ingestionTs = new Date().toISOString();

    for (const row of rawData) {
      try {
        const normalized = this.normalizeRow(row, store, runId, ingestionTs);
        if (normalized) {
          normalizedData.push(normalized);
        }
      } catch (error) {
        logger.warn('行の正規化に失敗（スキップ）', { 
          storeId: store.store_id,
          row: row,
          error: error.message 
        });
        continue;
      }
    }

    return normalizedData;
  }

  /**
   * 単一行を正規化
   * @param {Object} row 生データ行
   * @param {Object} store 店舗情報
   * @param {string} runId 実行ID
   * @param {string} ingestionTs 取込時刻
   * @returns {Object} 正規化された行データ
   */
  normalizeRow(row, store, runId, ingestionTs) {
    // 列名をマッピング
    const mappedRow = {};
    for (const [originalKey, value] of Object.entries(row)) {
      const normalizedKey = this.columnMapping[originalKey] || originalKey.toLowerCase();
      mappedRow[normalizedKey] = value;
    }

    // 必須項目の検証
    if (!mappedRow.reserve_date) {
      logger.warn('予約日が未設定（スキップ）', { storeId: store.store_id, row });
      return null;
    }

    // データ型変換・正規化
    const normalized = {
      // 店舗情報
      store_id: store.store_id,
      store_name: store.store_name || mappedRow.store_name,
      
      // 予約日（必須）
      reserve_date: this.normalizeDate(mappedRow.reserve_date),
      
      // 予約受付日（オプション）
      booking_date: mappedRow.booking_date ? this.normalizeDate(mappedRow.booking_date) : null,
      
      // 時間（オプション）
      start_time: mappedRow.start_time ? this.normalizeTime(mappedRow.start_time) : null,
      end_time: mappedRow.end_time ? this.normalizeTime(mappedRow.end_time) : null,
      
      // 予約内容（オプション）
      course_name: this.normalizeString(mappedRow.course_name),
      headcount: this.normalizeInteger(mappedRow.headcount),
      
      // 経路・ステータス（オプション）
      channel: this.normalizeString(mappedRow.channel),
      status: this.normalizeString(mappedRow.status),
      
      // 固定値・メタデータ
      vendor: 'restaurant_board',
      ingestion_ts: ingestionTs,
      run_id: runId,
      
      // レコードキーとハッシュを生成
      record_key: this.generateRecordKey(mappedRow, store.store_id),
      record_hash: null // 後で計算
    };

    // 内容ハッシュを計算
    normalized.record_hash = this.calculateRecordHash(normalized);

    return normalized;
  }

  /**
   * 日付を正規化
   * @param {string} dateStr 日付文字列
   * @returns {string} YYYY-MM-DD形式の日付
   */
  normalizeDate(dateStr) {
    if (!dateStr) return null;
    
    const cleaned = dateStr.toString().trim();
    if (!cleaned) return null;

    // 様々な日付形式をパース
    const formats = [
      'YYYY-MM-DD',
      'YYYY/MM/DD',
      'MM/DD/YYYY',
      'DD/MM/YYYY',
      'YYYY年MM月DD日',
      'MM月DD日',
      'YYYY-MM-DD HH:mm:ss',
      'YYYY/MM/DD HH:mm:ss'
    ];

    for (const format of formats) {
      const parsed = moment(cleaned, format, true);
      if (parsed.isValid()) {
        return parsed.format('YYYY-MM-DD');
      }
    }

    // パースできない場合はそのまま返す（エラーログ付き）
    logger.warn('日付の正規化に失敗', { dateStr, cleaned });
    return cleaned;
  }

  /**
   * 時間を正規化
   * @param {string} timeStr 時間文字列
   * @returns {string} HH:MM:SS形式の時間
   */
  normalizeTime(timeStr) {
    if (!timeStr) return null;
    
    const cleaned = timeStr.toString().trim();
    if (!cleaned) return null;

    // 様々な時間形式をパース
    const formats = [
      'HH:mm:ss',
      'HH:mm',
      'HH時mm分',
      'H:mm',
      'H:mm:ss'
    ];

    for (const format of formats) {
      const parsed = moment(cleaned, format, true);
      if (parsed.isValid()) {
        return parsed.format('HH:mm:ss');
      }
    }

    // パースできない場合はそのまま返す
    logger.warn('時間の正規化に失敗', { timeStr, cleaned });
    return cleaned;
  }

  /**
   * 文字列を正規化
   * @param {string} str 文字列
   * @returns {string} 正規化された文字列
   */
  normalizeString(str) {
    if (!str) return null;
    return str.toString().trim() || null;
  }

  /**
   * 整数を正規化
   * @param {string|number} value 値
   * @returns {number} 正規化された整数
   */
  normalizeInteger(value) {
    if (!value) return null;
    
    const cleaned = value.toString().replace(/[^\d]/g, '');
    const parsed = parseInt(cleaned, 10);
    
    return isNaN(parsed) ? null : parsed;
  }

  /**
   * レコードキーを生成
   * @param {Object} row 行データ
   * @param {string} storeId 店舗ID
   * @returns {string} レコードキー
   */
  generateRecordKey(row, storeId) {
    // 予約番号があればそれを使用
    if (row.reservation_id) {
      return `${storeId}|${row.reservation_id}`;
    }

    // 合成キーを生成
    const components = [
      storeId,
      this.normalizeDate(row.reserve_date),
      this.normalizeTime(row.start_time),
      this.normalizeString(row.course_name),
      this.normalizeInteger(row.headcount),
      this.normalizeString(row.channel)
    ];

    return components.filter(c => c !== null).join('|');
  }

  /**
   * レコードハッシュを計算
   * @param {Object} record レコードデータ
   * @returns {string} MD5ハッシュ
   */
  calculateRecordHash(record) {
    // ハッシュ計算から除外するフィールド
    const excludeFields = ['ingestion_ts', 'run_id', 'record_hash'];
    
    const hashData = { ...record };
    for (const field of excludeFields) {
      delete hashData[field];
    }

    const hashString = JSON.stringify(hashData, Object.keys(hashData).sort());
    return crypto.createHash('md5').update(hashString).digest('hex');
  }

  /**
   * 正規化されたCSVファイルを書き込み
   * @param {Array} data 正規化されたデータ
   * @param {string} storeId 店舗ID
   * @param {string} runId 実行ID
   * @returns {string} 出力ファイルパス
   */
  async writeNormalizedCsv(data, storeId, runId) {
    const outputPath = `/tmp/normalized_rb_${storeId}_${runId}.csv`;
    
    const csvWriter = createCsvWriter({
      path: outputPath,
      header: [
        { id: 'store_id', title: 'store_id' },
        { id: 'store_name', title: 'store_name' },
        { id: 'reserve_date', title: 'reserve_date' },
        { id: 'booking_date', title: 'booking_date' },
        { id: 'start_time', title: 'start_time' },
        { id: 'end_time', title: 'end_time' },
        { id: 'course_name', title: 'course_name' },
        { id: 'headcount', title: 'headcount' },
        { id: 'channel', title: 'channel' },
        { id: 'status', title: 'status' },
        { id: 'vendor', title: 'vendor' },
        { id: 'ingestion_ts', title: 'ingestion_ts' },
        { id: 'run_id', title: 'run_id' },
        { id: 'record_key', title: 'record_key' },
        { id: 'record_hash', title: 'record_hash' }
      ]
    });

    await csvWriter.writeRecords(data);
    return outputPath;
  }

  /**
   * 複数店舗のデータを統合
   * @param {Array} filePaths ファイルパス配列
   * @param {string} runId 実行ID
   * @returns {string} 統合ファイルパス
   */
  async mergeCsvFiles(filePaths, runId) {
    try {
      logger.info('CSV統合開始', { fileCount: filePaths.length, runId });

      const allData = [];
      
      for (const filePath of filePaths) {
        const data = await this.readCsvFile(filePath);
        allData.push(...data);
      }

      const mergedPath = `/tmp/merged_rb_${runId}.csv`;
      const csvWriter = createCsvWriter({
        path: mergedPath,
        header: [
          { id: 'store_id', title: 'store_id' },
          { id: 'store_name', title: 'store_name' },
          { id: 'reserve_date', title: 'reserve_date' },
          { id: 'booking_date', title: 'booking_date' },
          { id: 'start_time', title: 'start_time' },
          { id: 'end_time', title: 'end_time' },
          { id: 'course_name', title: 'course_name' },
          { id: 'headcount', title: 'headcount' },
          { id: 'channel', title: 'channel' },
          { id: 'status', title: 'status' },
          { id: 'vendor', title: 'vendor' },
          { id: 'ingestion_ts', title: 'ingestion_ts' },
          { id: 'run_id', title: 'run_id' },
          { id: 'record_key', title: 'record_key' },
          { id: 'record_hash', title: 'record_hash' }
        ]
      });

      await csvWriter.writeRecords(allData);

      logger.info('CSV統合完了', { 
        totalRows: allData.length,
        mergedPath 
      });

      return mergedPath;

    } catch (error) {
      logger.error('CSV統合に失敗', { error: error.message });
      throw error;
    }
  }
}

module.exports = DataTransformer;
