const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const config = require('../config');

class BigQueryService {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: config.PROJECT_ID,
      keyFilename: config.GOOGLE_APPLICATION_CREDENTIALS
    });
    this.dataset = this.bigquery.dataset(config.BQ_DATASET);
  }

  async initializeDataset() {
    try {
      // Check if dataset exists
      const [exists] = await this.dataset.exists();
      
      if (!exists) {
        logger.info(`Creating BigQuery dataset: ${config.BQ_DATASET}`);
        await this.dataset.create({
          location: config.REGION
        });
        logger.info(`Dataset ${config.BQ_DATASET} created successfully`);
      }

      // Create tables if they don't exist
      await this.createTablesIfNotExist();

    } catch (error) {
      logger.error('Failed to initialize BigQuery dataset:', error);
      throw error;
    }
  }

  async createTablesIfNotExist() {
    try {
      // Create stage table
      await this.createStageTable();
      
      // Create production table
      await this.createProductionTable();

      logger.info('BigQuery tables initialization completed');

    } catch (error) {
      logger.error('Failed to create BigQuery tables:', error);
      throw error;
    }
  }

  async createStageTable() {
    const tableName = config.BQ_STAGE_TABLE;
    const table = this.dataset.table(tableName);
    
    const [exists] = await table.exists();
    if (exists) {
      logger.debug(`Stage table ${tableName} already exists`);
      return;
    }

    const schema = this.getCommonSchema();
    
    const options = {
      schema,
      timePartitioning: {
        type: 'DAY',
        field: 'ingestion_ts'
      },
      clustering: {
        fields: ['store_id', 'vendor', 'run_id']
      },
      // Set TTL for stage table (14-30 days as per requirements)
      expirationTime: Date.now() + (14 * 24 * 60 * 60 * 1000) // 14 days
    };

    await table.create(options);
    logger.info(`Created stage table: ${tableName}`);
  }

  async createProductionTable() {
    const tableName = config.BQ_PRODUCTION_TABLE;
    const table = this.dataset.table(tableName);
    
    const [exists] = await table.exists();
    if (exists) {
      logger.debug(`Production table ${tableName} already exists`);
      return;
    }

    const schema = this.getCommonSchema();
    
    const options = {
      schema,
      timePartitioning: {
        type: 'DAY',
        field: 'reserve_date'
      },
      clustering: {
        fields: ['store_id', 'channel']
      }
    };

    await table.create(options);
    logger.info(`Created production table: ${tableName}`);
  }

  getCommonSchema() {
    return [
      { name: 'store_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'store_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'reserve_date', type: 'DATE', mode: 'REQUIRED' },
      { name: 'booking_date', type: 'DATE', mode: 'NULLABLE' },
      { name: 'start_time', type: 'TIME', mode: 'NULLABLE' },
      { name: 'end_time', type: 'TIME', mode: 'NULLABLE' },
      { name: 'course_name', type: 'STRING', mode: 'NULLABLE' },
      { name: 'headcount', type: 'INT64', mode: 'NULLABLE' },
      { name: 'channel', type: 'STRING', mode: 'NULLABLE' },
      { name: 'status', type: 'STRING', mode: 'NULLABLE' },
      { name: 'vendor', type: 'STRING', mode: 'REQUIRED' },
      { name: 'ingestion_ts', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: 'run_id', type: 'STRING', mode: 'REQUIRED' },
      { name: 'record_key', type: 'STRING', mode: 'REQUIRED' },
      { name: 'record_hash', type: 'STRING', mode: 'REQUIRED' }
    ];
  }

  async loadToStageTable(gcsPath, runId) {
    try {
      logger.debug(`Loading data from GCS to stage table: ${gcsPath}`);

      const table = this.dataset.table(config.BQ_STAGE_TABLE);
      const gcsUri = `gs://${config.GCS_BUCKET}/${gcsPath}`;

      const metadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1, // Skip header row
        autodetect: false,
        schema: this.getCommonSchema(),
        writeDisposition: 'WRITE_APPEND',
        jobLabels: {
          run_id: runId.replace(/[^a-z0-9_-]/gi, '_').toLowerCase(),
          vendor: 'restaurant_board'
        }
      };

      const [job] = await table.load(gcsUri, metadata);
      
      // Wait for the job to complete
      await job.promise();

      const [jobResult] = await job.get();
      const stats = jobResult.statistics.load;

      logger.info(`Load job completed successfully:`, {
        job_id: job.id,
        input_files: stats.inputFiles,
        input_file_bytes: stats.inputFileBytes,
        output_rows: stats.outputRows
      });

      return {
        id: job.id,
        outputRows: parseInt(stats.outputRows),
        inputFiles: parseInt(stats.inputFiles),
        inputFileBytes: parseInt(stats.inputFileBytes)
      };

    } catch (error) {
      logger.error(`Failed to load data to stage table from ${gcsPath}:`, error);
      throw error;
    }
  }

  async mergeStageToProduction(runId) {
    try {
      logger.info(`Executing MERGE operation for run_id: ${runId}`);

      // Load MERGE SQL from file
      const mergeSQL = await this.loadMergeSQL(runId);

      const query = {
        query: mergeSQL,
        useLegacySql: false,
        jobLabels: {
          run_id: runId.replace(/[^a-z0-9_-]/gi, '_').toLowerCase(),
          operation: 'merge'
        }
      };

      const [job] = await this.bigquery.createQueryJob(query);
      await job.promise();

      const [rows] = await job.getQueryResults();
      const mergeStats = rows[0];

      logger.info('MERGE operation completed:', {
        job_id: job.id,
        inserted_rows: mergeStats.inserted_rows,
        updated_rows: mergeStats.updated_rows,
        unchanged_rows: mergeStats.unchanged_rows
      });

      return {
        jobId: job.id,
        insertedRows: parseInt(mergeStats.inserted_rows),
        updatedRows: parseInt(mergeStats.updated_rows),
        unchangedRows: parseInt(mergeStats.unchanged_rows)
      };

    } catch (error) {
      logger.error('Failed to execute MERGE operation:', error);
      throw error;
    }
  }

  async loadMergeSQL(runId) {
    try {
      const sqlPath = path.join(config.SQL_DIR, 'merge_reservations_rb.sql');
      let sql = fs.readFileSync(sqlPath, 'utf8');

      // Replace placeholders
      sql = sql.replace(/\{PROJECT_ID\}/g, config.PROJECT_ID);
      sql = sql.replace(/\{DATASET\}/g, config.BQ_DATASET);
      sql = sql.replace(/\{RUN_ID\}/g, runId);
      sql = sql.replace(/\{STAGE_TABLE\}/g, config.BQ_STAGE_TABLE);
      sql = sql.replace(/\{PRODUCTION_TABLE\}/g, config.BQ_PRODUCTION_TABLE);

      return sql;

    } catch (error) {
      logger.error('Failed to load MERGE SQL:', error);
      throw error;
    }
  }

  async queryTable(tableName, query, parameters = []) {
    try {
      const options = {
        query,
        params: parameters,
        useLegacySql: false
      };

      const [rows] = await this.bigquery.query(options);
      return rows;

    } catch (error) {
      logger.error(`Failed to query table ${tableName}:`, error);
      throw error;
    }
  }

  async getTableInfo(tableName) {
    try {
      const table = this.dataset.table(tableName);
      const [metadata] = await table.getMetadata();
      
      return {
        id: metadata.id,
        numRows: metadata.numRows,
        numBytes: metadata.numBytes,
        creationTime: metadata.creationTime,
        lastModifiedTime: metadata.lastModifiedTime,
        schema: metadata.schema.fields
      };

    } catch (error) {
      logger.error(`Failed to get table info for ${tableName}:`, error);
      throw error;
    }
  }

  // Manual load operation (for testing/backfill)
  async loadManualData(gcsPath, runId) {
    try {
      logger.info(`Loading manual data from GCS: ${gcsPath}`);

      // First load to stage table
      const loadResult = await this.loadToStageTable(gcsPath, runId);

      // Then merge to production
      const mergeResult = await this.mergeStageToProduction(runId);

      return {
        load: loadResult,
        merge: mergeResult
      };

    } catch (error) {
      logger.error('Failed to load manual data:', error);
      throw error;
    }
  }

  // Cleanup stage table (remove old partitions)
  async cleanupStageTable(daysOld = 14) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysOld);
      
      const deleteSQL = `
        DELETE FROM \`${config.PROJECT_ID}.${config.BQ_DATASET}.${config.BQ_STAGE_TABLE}\`
        WHERE DATE(ingestion_ts) < @cutoff_date
      `;

      const options = {
        query: deleteSQL,
        params: { cutoff_date: cutoffDate.toISOString().split('T')[0] },
        useLegacySql: false
      };

      const [job] = await this.bigquery.createQueryJob(options);
      await job.promise();

      logger.info(`Cleaned up stage table partitions older than ${daysOld} days`);

    } catch (error) {
      logger.error('Failed to cleanup stage table:', error);
      throw error;
    }
  }
}

module.exports = BigQueryService;
