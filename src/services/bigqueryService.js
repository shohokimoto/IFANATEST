/**
 * BigQuery サービス
 */
const { BigQuery } = require('@google-cloud/bigquery');
const logger = require('../utils/logger');
const config = require('../config');

class BigQueryService {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: config.projectId
    });
    this.datasetId = config.bqDataset;
    this.stageTableId = config.stageTableName;
    this.mainTableId = config.mainTableName;
  }

  /**
   * GCSからBigQueryステージテーブルにデータをロード
   * @param {string} gcsObjectName GCSオブジェクト名
   * @param {string} runId 実行ID
   * @returns {Object} ロードジョブの結果
   */
  async loadDataFromGCS(gcsObjectName, runId) {
    try {
      logger.info('BigQueryロード開始', { gcsObjectName, runId });

      const gcsUri = `gs://${config.gcsBucket}/${gcsObjectName}`;
      const dataset = this.bigquery.dataset(this.datasetId);
      const table = dataset.table(this.stageTableId);

      const jobConfig = {
        sourceFormat: 'CSV',
        writeDisposition: 'WRITE_APPEND',
        skipLeadingRows: 1,
        autodetect: false,
        schema: this.getStageTableSchema(),
        timePartitioning: {
          type: 'DAY',
          field: 'ingestion_ts'
        }
      };

      const [job] = await table.load(gcsUri, jobConfig);
      logger.info('BigQueryロードジョブ開始', { 
        jobId: job.id,
        gcsUri,
        runId 
      });

      // ジョブ完了を待機
      const [jobResult] = await job.getMetadata();
      const [jobStatus] = await job.get();

      if (jobStatus.status.state === 'DONE') {
        if (jobStatus.status.errors && jobStatus.status.errors.length > 0) {
          throw new Error(`BigQueryロードジョブ失敗: ${JSON.stringify(jobStatus.status.errors)}`);
        }

        const outputRows = jobStatus.statistics.load.outputRows;
        logger.info('BigQueryロード完了', { 
          jobId: job.id,
          outputRows,
          runId 
        });

        return {
          jobId: job.id,
          outputRows: parseInt(outputRows),
          success: true
        };
      } else {
        throw new Error(`BigQueryロードジョブが完了していません: ${jobStatus.status.state}`);
      }

    } catch (error) {
      logger.error('BigQueryロードに失敗', { 
        gcsObjectName,
        runId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * ステージテーブルから本番テーブルにMERGE実行
   * @param {string} runId 実行ID
   * @returns {Object} MERGE結果
   */
  async mergeStageToMain(runId) {
    try {
      logger.info('BigQuery MERGE開始', { runId });

      // ストアドプロシージャを実行
      const query = `
        CALL \`${config.projectId}.${this.datasetId}.merge_reservations_rb\`('${runId}')
      `;

      const options = {
        query: query,
        useLegacySql: false
      };

      const [job] = await this.bigquery.createQueryJob(options);
      logger.info('BigQuery MERGEジョブ開始', { 
        jobId: job.id,
        runId 
      });

      // ジョブ完了を待機
      const [rows] = await job.getQueryResults();

      // ストアドプロシージャの結果を解析
      const result = this.parseMergeResult(rows);

      logger.info('BigQuery MERGE完了', { 
        jobId: job.id,
        runId,
        result 
      });

      return {
        jobId: job.id,
        runId,
        result,
        success: true
      };

    } catch (error) {
      logger.error('BigQuery MERGEに失敗', { 
        runId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * ステージテーブルのスキーマ定義
   * @returns {Array} スキーマ配列
   */
  getStageTableSchema() {
    return [
      { name: 'store_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'store_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'reserve_date', type: 'DATE', mode: 'REQUIRED' },
      { name: 'booking_date', type: 'DATE', mode: 'NULLABLE' },
      { name: 'start_time', type: 'TIME', mode: 'NULLABLE' },
      { name: 'end_time', type: 'TIME', mode: 'NULLABLE' },
      { name: 'course_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'headcount', type: 'INTEGER', mode: 'NULLABLE' },
      { name: 'channel', type: 'STRING', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendor', type: 'STRING', mode: 'REQUIRED' },
      { name: 'ingestion_ts', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: 'run_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'record_key', type: 'STRING', mode: 'REQUIRED' },
      { name: 'record_hash', type: 'STRING', mode: 'REQUIRED' }
    ];
  }

  /**
   * 本番テーブルのスキーマ定義
   * @returns {Array} スキーマ配列
   */
  getMainTableSchema() {
    return [
      { name: 'store_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'store_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'reserve_date', type: 'DATE', mode: 'REQUIRED' },
      { name: 'booking_date', type: 'DATE', mode: 'NULLABLE' },
      { name: 'start_time', type: 'TIME', mode: 'NULLABLE' },
      { name: 'end_time', type: 'TIME', mode: 'NULLABLE' },
      { name: 'course_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'headcount', type: 'INTEGER', mode: 'NULLABLE' },
      { name: 'channel', type: 'STRING', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendor', type: 'STRING', mode: 'REQUIRED' },
      { name: 'ingestion_ts', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: 'run_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'record_key', type: 'STRING', mode: 'REQUIRED' },
      { name: 'record_hash', type: 'STRING', mode: 'REQUIRED' },
      { name: 'created_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: 'updated_at', type: 'TIMESTAMP', mode: 'REQUIRED' }
    ];
  }

  /**
   * データセットが存在するかチェック
   * @returns {boolean} 存在するかどうか
   */
  async datasetExists() {
    try {
      const dataset = this.bigquery.dataset(this.datasetId);
      const [exists] = await dataset.exists();
      return exists;
    } catch (error) {
      logger.error('データセット存在確認に失敗', { 
        datasetId: this.datasetId,
        error: error.message 
      });
      return false;
    }
  }

  /**
   * データセットを作成
   */
  async createDataset() {
    try {
      logger.info('データセット作成開始', { datasetId: this.datasetId });

      const [dataset] = await this.bigquery.createDataset(this.datasetId, {
        location: config.region,
        description: 'レストランボード予約データETL用データセット'
      });

      logger.info('データセット作成完了', { datasetId: this.datasetId });
      return dataset;

    } catch (error) {
      logger.error('データセット作成に失敗', { 
        datasetId: this.datasetId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * ステージテーブルが存在するかチェック
   * @returns {boolean} 存在するかどうか
   */
  async stageTableExists() {
    try {
      const dataset = this.bigquery.dataset(this.datasetId);
      const table = dataset.table(this.stageTableId);
      const [exists] = await table.exists();
      return exists;
    } catch (error) {
      logger.error('ステージテーブル存在確認に失敗', { 
        tableId: this.stageTableId,
        error: error.message 
      });
      return false;
    }
  }

  /**
   * ステージテーブルを作成
   */
  async createStageTable() {
    try {
      logger.info('ステージテーブル作成開始', { tableId: this.stageTableId });

      const dataset = this.bigquery.dataset(this.datasetId);
      const schema = this.getStageTableSchema();

      const [table] = await dataset.createTable(this.stageTableId, {
        schema: schema,
        timePartitioning: {
          type: 'DAY',
          field: 'ingestion_ts'
        },
        description: 'レストランボード予約データのステージテーブル（TTL: 30日）'
      });

      logger.info('ステージテーブル作成完了', { tableId: this.stageTableId });
      return table;

    } catch (error) {
      logger.error('ステージテーブル作成に失敗', { 
        tableId: this.stageTableId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * 本番テーブルが存在するかチェック
   * @returns {boolean} 存在するかどうか
   */
  async mainTableExists() {
    try {
      const dataset = this.bigquery.dataset(this.datasetId);
      const table = dataset.table(this.mainTableId);
      const [exists] = await table.exists();
      return exists;
    } catch (error) {
      logger.error('本番テーブル存在確認に失敗', { 
        tableId: this.mainTableId,
        error: error.message 
      });
      return false;
    }
  }

  /**
   * 本番テーブルを作成
   */
  async createMainTable() {
    try {
      logger.info('本番テーブル作成開始', { tableId: this.mainTableId });

      const dataset = this.bigquery.dataset(this.datasetId);
      const schema = this.getMainTableSchema();

      const [table] = await dataset.createTable(this.mainTableId, {
        schema: schema,
        timePartitioning: {
          type: 'DAY',
          field: 'reserve_date'
        },
        clustering: {
          fields: ['store_id', 'channel']
        },
        description: 'レストランボード予約データの本番テーブル'
      });

      logger.info('本番テーブル作成完了', { tableId: this.mainTableId });
      return table;

    } catch (error) {
      logger.error('本番テーブル作成に失敗', { 
        tableId: this.mainTableId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * テーブルの行数を取得
   * @param {string} tableId テーブルID
   * @returns {number} 行数
   */
  async getTableRowCount(tableId) {
    try {
      const query = `
        SELECT COUNT(*) as row_count
        FROM \`${config.projectId}.${this.datasetId}.${tableId}\`
      `;

      const [rows] = await this.bigquery.query(query);
      const rowCount = parseInt(rows[0].row_count);

      logger.info('テーブル行数取得完了', { tableId, rowCount });
      return rowCount;

    } catch (error) {
      logger.error('テーブル行数取得に失敗', { 
        tableId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * MERGE結果を解析
   * @param {Array} rows ストアドプロシージャの結果行
   * @returns {Object} 解析結果
   */
  parseMergeResult(rows) {
    // ストアドプロシージャの結果を解析
    // 実際の実装はストアドプロシージャの出力形式に依存
    let inserted = 0;
    let updated = 0;
    let unchanged = 0;

    for (const row of rows) {
      if (row.action === 'INSERT') inserted++;
      else if (row.action === 'UPDATE') updated++;
      else if (row.action === 'UNCHANGED') unchanged++;
    }

    return {
      inserted,
      updated,
      unchanged,
      total: inserted + updated + unchanged
    };
  }

  /**
   * 指定期間のデータをクエリ
   * @param {string} fromDate 開始日
   * @param {string} toDate 終了日
   * @param {Array} storeIds 店舗ID配列（オプション）
   * @returns {Array} クエリ結果
   */
  async queryReservations(fromDate, toDate, storeIds = null) {
    try {
      logger.info('予約データクエリ開始', { fromDate, toDate, storeIds });

      let query = `
        SELECT store_id, store_name, reserve_date, booking_date,
               start_time, end_time, course_name, headcount, channel, status
        FROM \`${config.projectId}.${this.datasetId}.${this.mainTableId}\`
        WHERE reserve_date BETWEEN '${fromDate}' AND '${toDate}'
      `;

      if (storeIds && storeIds.length > 0) {
        const storeIdList = storeIds.map(id => `'${id}'`).join(',');
        query += ` AND store_id IN (${storeIdList})`;
      }

      query += ' ORDER BY reserve_date, store_id';

      const [rows] = await this.bigquery.query(query);

      logger.info('予約データクエリ完了', { 
        fromDate,
        toDate,
        resultCount: rows.length 
      });

      return rows;

    } catch (error) {
      logger.error('予約データクエリに失敗', { 
        fromDate,
        toDate,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * ストアドプロシージャが存在するかチェック
   * @returns {boolean} 存在するかどうか
   */
  async storedProcedureExists() {
    try {
      const query = `
        SELECT routine_name
        FROM \`${config.projectId}.${this.datasetId}.INFORMATION_SCHEMA.ROUTINES\`
        WHERE routine_name = 'merge_reservations_rb'
      `;

      const [rows] = await this.bigquery.query(query);
      return rows.length > 0;

    } catch (error) {
      logger.error('ストアドプロシージャ存在確認に失敗', { 
        error: error.message 
      });
      return false;
    }
  }
}

module.exports = BigQueryService;
