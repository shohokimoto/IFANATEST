/**
 * レストランボード予約データETLシステム - メインエントリーポイント
 */
const { v4: uuidv4 } = require('uuid');
const moment = require('moment');

// サービスインポート
const StoreMasterService = require('./services/storeMaster');
const RestaurantBoardScraper = require('./services/scraper');
const DataTransformer = require('./services/transformer');
const GCSService = require('./services/gcsService');
const BigQueryService = require('./services/bigqueryService');

// ユーティリティ
const logger = require('./utils/logger');
const config = require('./config');

class RestaurantBoardETL {
  constructor() {
    this.runId = uuidv4();
    this.startTime = new Date();
    
    // サービス初期化
    this.storeMaster = new StoreMasterService();
    this.scraper = new RestaurantBoardScraper();
    this.transformer = new DataTransformer();
    this.gcsService = new GCSService();
    this.bigQueryService = new BigQueryService();
    
    // 処理結果
    this.results = {
      runId: this.runId,
      startTime: this.startTime,
      stores: [],
      summary: {
        totalStores: 0,
        successfulStores: 0,
        failedStores: 0,
        totalRecords: 0,
        totalErrors: 0
      }
    };
  }

  /**
   * ETL処理のメイン実行
   */
  async run() {
    try {
      logger.info('レストランボードETL処理開始', { 
        runId: this.runId,
        startTime: this.startTime 
      });

      // 1. 店舗マスタ取得
      const stores = await this.getActiveStores();
      this.results.summary.totalStores = stores.length;

      if (stores.length === 0) {
        logger.warn('処理対象の店舗がありません');
        return this.results;
      }

      // 2. 各店舗の処理
      await this.processStores(stores);

      // 3. 統合ファイルの処理
      await this.processMergedData();

      // 4. 結果サマリ
      this.calculateSummary();

      logger.info('レストランボードETL処理完了', { 
        runId: this.runId,
        duration: moment().diff(this.startTime, 'seconds'),
        summary: this.results.summary 
      });

      return this.results;

    } catch (error) {
      logger.error('ETL処理で致命的エラー', { 
        runId: this.runId,
        error: error.message,
        stack: error.stack 
      });
      
      this.results.error = error.message;
      throw error;
    } finally {
      // クリーンアップ
      await this.cleanup();
    }
  }

  /**
   * アクティブな店舗一覧を取得
   * @returns {Array} 店舗一覧
   */
  async getActiveStores() {
    try {
      logger.info('店舗マスタ取得開始');
      const stores = await this.storeMaster.getActiveStores();
      logger.info('店舗マスタ取得完了', { storeCount: stores.length });
      return stores;
    } catch (error) {
      logger.error('店舗マスタ取得に失敗', { error: error.message });
      throw error;
    }
  }

  /**
   * 各店舗の処理を実行
   * @param {Array} stores 店舗一覧
   */
  async processStores(stores) {
    logger.info('店舗処理開始', { storeCount: stores.length });

    for (const store of stores) {
      const storeResult = {
        storeId: store.store_id,
        storeName: store.store_name,
        success: false,
        records: 0,
        errors: []
      };

      try {
        // 処理期間を計算
        const { fromDate, toDate } = this.storeMaster.calculateDateRange(store);
        storeResult.fromDate = fromDate;
        storeResult.toDate = toDate;

        logger.info('店舗処理開始', { 
          storeId: store.store_id,
          fromDate,
          toDate 
        });

        // 1. スクレイピング
        const csvPath = await this.scraper.downloadReservationData(store, fromDate, toDate);
        
        // 2. データ変換
        const normalizedPath = await this.transformer.transformCsv(csvPath, store, this.runId);
        
        // 3. GCSアップロード
        const gcsObjectName = await this.gcsService.uploadCsvFile(normalizedPath, {
          storeId: store.store_id,
          runId: this.runId
        });

        // 4. BigQueryロード
        const loadResult = await this.bigQueryService.loadDataFromGCS(gcsObjectName, this.runId);
        
        storeResult.success = true;
        storeResult.records = loadResult.outputRows;
        storeResult.gcsObjectName = gcsObjectName;
        storeResult.loadJobId = loadResult.jobId;

        this.results.summary.successfulStores++;
        this.results.summary.totalRecords += loadResult.outputRows;

        logger.info('店舗処理完了', { 
          storeId: store.store_id,
          records: loadResult.outputRows 
        });

      } catch (error) {
        storeResult.success = false;
        storeResult.errors.push(error.message);
        this.results.summary.failedStores++;
        this.results.summary.totalErrors++;

        logger.error('店舗処理失敗', { 
          storeId: store.store_id,
          error: error.message 
        });
      }

      this.results.stores.push(storeResult);
    }

    logger.info('店舗処理完了', { 
      total: stores.length,
      successful: this.results.summary.successfulStores,
      failed: this.results.summary.failedStores 
    });
  }

  /**
   * 統合データの処理
   */
  async processMergedData() {
    try {
      if (this.results.summary.successfulStores === 0) {
        logger.warn('MERGE処理をスキップ（成功した店舗なし）');
        return;
      }

      logger.info('BigQuery MERGE処理開始', { runId: this.runId });

      // ステージ→本番のMERGE実行
      const mergeResult = await this.bigQueryService.mergeStageToMain(this.runId);
      
      this.results.mergeResult = mergeResult;

      logger.info('BigQuery MERGE処理完了', { 
        runId: this.runId,
        result: mergeResult.result 
      });

    } catch (error) {
      logger.error('BigQuery MERGE処理に失敗', { 
        runId: this.runId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * 結果サマリを計算
   */
  calculateSummary() {
    this.results.endTime = new Date();
    this.results.duration = moment(this.results.endTime).diff(this.results.startTime, 'seconds');
    
    // 成功率計算
    this.results.summary.successRate = this.results.summary.totalStores > 0 
      ? Math.round((this.results.summary.successfulStores / this.results.summary.totalStores) * 100) 
      : 0;

    logger.info('処理結果サマリ', {
      runId: this.runId,
      duration: this.results.duration,
      summary: this.results.summary
    });
  }

  /**
   * リソースのクリーンアップ
   */
  async cleanup() {
    try {
      logger.info('クリーンアップ開始');
      
      // ブラウザの終了
      await this.scraper.cleanup();
      
      // 一時ファイルの削除（必要に応じて）
      // await this.cleanupTempFiles();
      
      logger.info('クリーンアップ完了');
    } catch (error) {
      logger.error('クリーンアップに失敗', { error: error.message });
    }
  }

  /**
   * 一時ファイルのクリーンアップ
   */
  async cleanupTempFiles() {
    const fs = require('fs').promises;
    
    try {
      const files = await fs.readdir('/tmp');
      const tempFiles = files.filter(file => 
        file.includes(this.runId) || 
        file.startsWith('rb_') || 
        file.startsWith('normalized_')
      );

      for (const file of tempFiles) {
        try {
          await fs.unlink(`/tmp/${file}`);
          logger.debug('一時ファイル削除', { file });
        } catch (error) {
          logger.warn('一時ファイル削除に失敗', { file, error: error.message });
        }
      }

      logger.info('一時ファイルクリーンアップ完了', { deletedFiles: tempFiles.length });
    } catch (error) {
      logger.error('一時ファイルクリーンアップに失敗', { error: error.message });
    }
  }

  /**
   * 手動テスト用のメソッド
   * @param {string} csvFilePath 手動アップロードCSVファイルパス
   */
  async runManualTest(csvFilePath) {
    try {
      logger.info('手動テスト開始', { csvFilePath, runId: this.runId });

      // 手動アップロードCSVをGCSにアップロード
      const gcsObjectName = await this.gcsService.uploadCsvFile(csvFilePath, {
        runId: this.runId,
        isManual: true
      });

      // BigQueryにロード
      const loadResult = await this.bigQueryService.loadDataFromGCS(gcsObjectName, this.runId);
      
      // MERGE実行
      const mergeResult = await this.bigQueryService.mergeStageToMain(this.runId);

      logger.info('手動テスト完了', { 
        runId: this.runId,
        loadResult,
        mergeResult 
      });

      return {
        runId: this.runId,
        loadResult,
        mergeResult,
        success: true
      };

    } catch (error) {
      logger.error('手動テストに失敗', { 
        runId: this.runId,
        error: error.message 
      });
      throw error;
    }
  }
}

/**
 * メイン実行関数
 */
async function main() {
  const etl = new RestaurantBoardETL();
  
  try {
    // コマンドライン引数をチェック
    const args = process.argv.slice(2);
    
    if (args.length > 0 && args[0] === 'manual') {
      // 手動テストモード
      if (args.length < 2) {
        throw new Error('手動テストモードではCSVファイルパスが必要です');
      }
      const csvFilePath = args[1];
      const result = await etl.runManualTest(csvFilePath);
      console.log('手動テスト結果:', JSON.stringify(result, null, 2));
    } else {
      // 通常のETL処理
      const result = await etl.run();
      console.log('ETL処理結果:', JSON.stringify(result, null, 2));
    }
    
    process.exit(0);
  } catch (error) {
    logger.error('メイン処理でエラー', { error: error.message });
    process.exit(1);
  }
}

// 直接実行時のみmain関数を呼び出し
if (require.main === module) {
  main();
}

module.exports = RestaurantBoardETL;
