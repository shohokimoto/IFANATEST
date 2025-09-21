/**
 * Google Cloud Storage サービス
 */
const { Storage } = require('@google-cloud/storage');
const fs = require('fs').promises;
const path = require('path');
const moment = require('moment');
const logger = require('../utils/logger');
const config = require('../config');

class GCSService {
  constructor() {
    this.storage = new Storage({
      projectId: config.projectId
    });
    this.bucket = this.storage.bucket(config.gcsBucket);
  }

  /**
   * CSVファイルをGCSにアップロード
   * @param {string} localFilePath ローカルファイルパス
   * @param {Object} options アップロードオプション
   * @returns {string} GCSオブジェクト名
   */
  async uploadCsvFile(localFilePath, options = {}) {
    try {
      const {
        storeId,
        runId,
        isManual = false,
        customPath = null
      } = options;

      logger.info('GCSアップロード開始', { 
        localFilePath,
        storeId,
        runId,
        isManual 
      });

      // オブジェクト名を生成
      const objectName = this.generateObjectName(localFilePath, {
        storeId,
        runId,
        isManual,
        customPath
      });

      // ファイルをアップロード
      await this.bucket.upload(localFilePath, {
        destination: objectName,
        metadata: {
          contentType: 'text/csv',
          cacheControl: 'no-cache',
          metadata: {
            storeId: storeId || 'unknown',
            runId: runId || 'unknown',
            uploadTime: new Date().toISOString(),
            isManual: isManual.toString()
          }
        }
      });

      // アップロード完了を確認
      const [exists] = await this.bucket.file(objectName).exists();
      if (!exists) {
        throw new Error('アップロード後のファイル存在確認に失敗');
      }

      logger.info('GCSアップロード完了', { 
        localFilePath,
        objectName,
        storeId,
        runId 
      });

      return objectName;

    } catch (error) {
      logger.error('GCSアップロードに失敗', { 
        localFilePath,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * GCSオブジェクト名を生成
   * @param {string} localFilePath ローカルファイルパス
   * @param {Object} options オプション
   * @returns {string} GCSオブジェクト名
   */
  generateObjectName(localFilePath, options = {}) {
    const {
      storeId,
      runId,
      isManual = false,
      customPath = null
    } = options;

    const now = moment();
    const year = now.format('YYYY');
    const month = now.format('MM');
    const day = now.format('DD');
    
    // ファイル名を取得
    const fileName = path.basename(localFilePath);

    if (customPath) {
      return customPath;
    }

    if (isManual) {
      // 手動アップロード用のパス
      return `${config.manualPathPrefix}/${year}/${month}/${day}/${fileName}`;
    } else {
      // 自動処理用のパス
      if (storeId && runId) {
        return `${config.gcsPathPrefix}/${year}/${month}/${day}/run_${runId}/rb_${storeId}_${now.format('YYYYMMDD')}.csv`;
      } else {
        return `${config.gcsPathPrefix}/${year}/${month}/${day}/run_${runId}/${fileName}`;
      }
    }
  }

  /**
   * 複数ファイルを一括アップロード
   * @param {Array} files ファイル情報配列
   * @returns {Array} アップロード結果配列
   */
  async uploadMultipleFiles(files) {
    const results = [];
    
    for (const file of files) {
      try {
        const objectName = await this.uploadCsvFile(file.localPath, file.options);
        results.push({
          success: true,
          localPath: file.localPath,
          objectName,
          storeId: file.options.storeId
        });
      } catch (error) {
        results.push({
          success: false,
          localPath: file.localPath,
          error: error.message,
          storeId: file.options.storeId
        });
      }
    }

    const successCount = results.filter(r => r.success).length;
    const failureCount = results.filter(r => !r.success).length;

    logger.info('一括アップロード完了', { 
      total: files.length,
      success: successCount,
      failure: failureCount 
    });

    return results;
  }

  /**
   * GCSからファイルをダウンロード
   * @param {string} objectName GCSオブジェクト名
   * @param {string} localFilePath ローカル保存先パス
   */
  async downloadFile(objectName, localFilePath) {
    try {
      logger.info('GCSダウンロード開始', { objectName, localFilePath });

      await this.bucket.file(objectName).download({
        destination: localFilePath
      });

      // ダウンロード完了を確認
      await fs.access(localFilePath);

      logger.info('GCSダウンロード完了', { objectName, localFilePath });

    } catch (error) {
      logger.error('GCSダウンロードに失敗', { 
        objectName,
        localFilePath,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * GCSオブジェクトの存在確認
   * @param {string} objectName GCSオブジェクト名
   * @returns {boolean} 存在するかどうか
   */
  async fileExists(objectName) {
    try {
      const [exists] = await this.bucket.file(objectName).exists();
      return exists;
    } catch (error) {
      logger.error('ファイル存在確認に失敗', { objectName, error: error.message });
      return false;
    }
  }

  /**
   * GCSオブジェクトのメタデータを取得
   * @param {string} objectName GCSオブジェクト名
   * @returns {Object} メタデータ
   */
  async getFileMetadata(objectName) {
    try {
      const [metadata] = await this.bucket.file(objectName).getMetadata();
      return metadata;
    } catch (error) {
      logger.error('ファイルメタデータ取得に失敗', { 
        objectName, 
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * 指定期間のファイル一覧を取得
   * @param {string} prefix プレフィックス
   * @param {Date} startDate 開始日
   * @param {Date} endDate 終了日
   * @returns {Array} ファイル情報配列
   */
  async listFilesByDateRange(prefix, startDate, endDate) {
    try {
      logger.info('ファイル一覧取得開始', { prefix, startDate, endDate });

      const [files] = await this.bucket.getFiles({
        prefix: prefix
      });

      const filteredFiles = files.filter(file => {
        const fileDate = new Date(file.metadata.timeCreated);
        return fileDate >= startDate && fileDate <= endDate;
      });

      const fileInfos = await Promise.all(
        filteredFiles.map(async (file) => {
          const [metadata] = await file.getMetadata();
          return {
            name: file.name,
            size: metadata.size,
            created: metadata.timeCreated,
            updated: metadata.updated,
            metadata: metadata.metadata || {}
          };
        })
      );

      logger.info('ファイル一覧取得完了', { 
        prefix,
        totalFiles: files.length,
        filteredFiles: fileInfos.length 
      });

      return fileInfos;

    } catch (error) {
      logger.error('ファイル一覧取得に失敗', { 
        prefix,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * 古いファイルを削除（TTL管理）
   * @param {string} prefix プレフィックス
   * @param {number} daysToKeep 保持日数
   * @returns {number} 削除されたファイル数
   */
  async cleanupOldFiles(prefix, daysToKeep = 30) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

      logger.info('古いファイル削除開始', { prefix, daysToKeep, cutoffDate });

      const [files] = await this.bucket.getFiles({
        prefix: prefix
      });

      const filesToDelete = files.filter(file => {
        const fileDate = new Date(file.metadata.timeCreated);
        return fileDate < cutoffDate;
      });

      let deletedCount = 0;
      for (const file of filesToDelete) {
        try {
          await file.delete();
          deletedCount++;
          logger.debug('ファイル削除完了', { fileName: file.name });
        } catch (error) {
          logger.error('ファイル削除に失敗', { 
            fileName: file.name,
            error: error.message 
          });
        }
      }

      logger.info('古いファイル削除完了', { 
        prefix,
        totalFiles: files.length,
        deletedFiles: deletedCount 
      });

      return deletedCount;

    } catch (error) {
      logger.error('古いファイル削除に失敗', { 
        prefix,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * バケットの容量使用量を取得
   * @returns {Object} 容量情報
   */
  async getBucketUsage() {
    try {
      const [files] = await this.bucket.getFiles();
      
      let totalSize = 0;
      let fileCount = 0;
      
      for (const file of files) {
        const [metadata] = await file.getMetadata();
        totalSize += parseInt(metadata.size || 0);
        fileCount++;
      }

      const usage = {
        fileCount,
        totalSizeBytes: totalSize,
        totalSizeMB: Math.round(totalSize / 1024 / 1024 * 100) / 100,
        totalSizeGB: Math.round(totalSize / 1024 / 1024 / 1024 * 100) / 100
      };

      logger.info('バケット使用量取得完了', usage);
      return usage;

    } catch (error) {
      logger.error('バケット使用量取得に失敗', { error: error.message });
      throw error;
    }
  }
}

module.exports = GCSService;
