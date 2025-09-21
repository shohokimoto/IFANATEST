const { Storage } = require('@google-cloud/storage');
const path = require('path');
const dayjs = require('dayjs');
const logger = require('../utils/logger');
const config = require('../config');
const TransformService = require('./transform-service');

class GcsService {
  constructor() {
    this.storage = new Storage({
      projectId: config.PROJECT_ID,
      keyFilename: config.GOOGLE_APPLICATION_CREDENTIALS
    });
    this.bucket = this.storage.bucket(config.GCS_BUCKET);
    this.transformService = new TransformService();
  }

  async uploadTransformedData(transformedRecords, storeId, runId) {
    try {
      logger.debug(`Uploading ${transformedRecords.length} transformed records for store ${storeId}`);

      // Generate CSV from transformed records
      const csvData = await this.transformService.generateCSV(transformedRecords);

      // Generate GCS object path
      const objectPath = this.generateObjectPath(storeId, runId);

      // Upload to GCS
      const file = this.bucket.file(objectPath);
      
      await file.save(csvData, {
        metadata: {
          contentType: 'text/csv',
          metadata: {
            store_id: storeId,
            run_id: runId,
            record_count: transformedRecords.length.toString(),
            upload_timestamp: dayjs().tz('Asia/Tokyo').format(),
            vendor: 'restaurant_board'
          }
        }
      });

      logger.info(`Successfully uploaded ${transformedRecords.length} records to GCS: ${objectPath}`);
      return objectPath;

    } catch (error) {
      logger.error(`Failed to upload data to GCS for store ${storeId}:`, error);
      throw error;
    }
  }

  generateObjectPath(storeId, runId) {
    const now = dayjs().tz('Asia/Tokyo');
    const year = now.format('YYYY');
    const month = now.format('MM');
    const day = now.format('DD');
    
    return `${config.GCS_LANDING_PREFIX}/${year}/${month}/${day}/run_${runId}/rb_${storeId}_${now.format('YYYYMMDD')}.csv`;
  }

  async uploadManualData(csvData, storeId, filename) {
    try {
      logger.debug(`Uploading manual data for store ${storeId}: ${filename}`);

      const now = dayjs().tz('Asia/Tokyo');
      const year = now.format('YYYY');
      const month = now.format('MM');
      const day = now.format('DD');
      
      const objectPath = `${config.GCS_MANUAL_PREFIX}/${year}/${month}/${day}/${filename}`;

      const file = this.bucket.file(objectPath);
      
      await file.save(csvData, {
        metadata: {
          contentType: 'text/csv',
          metadata: {
            store_id: storeId,
            upload_type: 'manual',
            upload_timestamp: now.format(),
            vendor: 'restaurant_board'
          }
        }
      });

      logger.info(`Successfully uploaded manual data to GCS: ${objectPath}`);
      return objectPath;

    } catch (error) {
      logger.error(`Failed to upload manual data to GCS:`, error);
      throw error;
    }
  }

  async listFiles(prefix, maxResults = 100) {
    try {
      const [files] = await this.bucket.getFiles({
        prefix,
        maxResults
      });

      return files.map(file => ({
        name: file.name,
        size: file.metadata.size,
        created: file.metadata.timeCreated,
        updated: file.metadata.updated,
        metadata: file.metadata.metadata || {}
      }));

    } catch (error) {
      logger.error(`Failed to list files with prefix ${prefix}:`, error);
      throw error;
    }
  }

  async downloadFile(objectPath) {
    try {
      const file = this.bucket.file(objectPath);
      const [contents] = await file.download();
      
      logger.debug(`Downloaded file from GCS: ${objectPath}`);
      return contents.toString();

    } catch (error) {
      logger.error(`Failed to download file ${objectPath}:`, error);
      throw error;
    }
  }

  async deleteFile(objectPath) {
    try {
      const file = this.bucket.file(objectPath);
      await file.delete();
      
      logger.info(`Deleted file from GCS: ${objectPath}`);

    } catch (error) {
      logger.error(`Failed to delete file ${objectPath}:`, error);
      throw error;
    }
  }

  async fileExists(objectPath) {
    try {
      const file = this.bucket.file(objectPath);
      const [exists] = await file.exists();
      return exists;

    } catch (error) {
      logger.error(`Failed to check if file exists ${objectPath}:`, error);
      return false;
    }
  }

  // Utility method to get GCS URI for BigQuery load jobs
  getGcsUri(objectPath) {
    return `gs://${config.GCS_BUCKET}/${objectPath}`;
  }

  // Clean up old files (useful for lifecycle management)
  async cleanupOldFiles(daysOld = 30) {
    try {
      const cutoffDate = dayjs().subtract(daysOld, 'days');
      
      const [files] = await this.bucket.getFiles({
        prefix: config.GCS_LANDING_PREFIX
      });

      const filesToDelete = files.filter(file => {
        const fileDate = dayjs(file.metadata.timeCreated);
        return fileDate.isBefore(cutoffDate);
      });

      logger.info(`Found ${filesToDelete.length} files older than ${daysOld} days to delete`);

      for (const file of filesToDelete) {
        try {
          await file.delete();
          logger.debug(`Deleted old file: ${file.name}`);
        } catch (error) {
          logger.warn(`Failed to delete old file ${file.name}: ${error.message}`);
        }
      }

      logger.info(`Cleanup completed: ${filesToDelete.length} old files processed`);

    } catch (error) {
      logger.error('Failed to cleanup old files:', error);
      throw error;
    }
  }
}

module.exports = GcsService;
