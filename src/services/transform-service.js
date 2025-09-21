const csv = require('csv-parse');
const { stringify } = require('csv-stringify');
const crypto = require('crypto');
const dayjs = require('dayjs');
const logger = require('../utils/logger');
const config = require('../config');

class TransformService {
  constructor() {
    // Load schema mapping for Restaurant Board
    this.rbSchema = this.loadRBSchema();
  }

  loadRBSchema() {
    // Restaurant Board CSV column mapping
    // This should be externalized to config/schema files for easy maintenance
    return {
      // Map RB CSV columns to our common schema
      columnMapping: {
        '予約番号': 'reservation_id',
        '店舗名': 'store_name_rb',
        '予約日': 'reserve_date_raw',
        '受付日': 'booking_date_raw', 
        '開始時間': 'start_time_raw',
        '終了時間': 'end_time_raw',
        'コース名': 'course_name',
        '人数': 'headcount_raw',
        '経路': 'channel_raw',
        'ステータス': 'status_raw'
        // Add more mappings based on actual RB CSV format
      },
      
      // Data type conversions
      typeConversions: {
        'reserve_date_raw': 'date',
        'booking_date_raw': 'date',
        'start_time_raw': 'time',
        'end_time_raw': 'time',
        'headcount_raw': 'integer'
      },

      // Channel mapping for standardization
      channelMapping: {
        'ホットペッパー': 'hotpepper',
        '食べログ': 'tabelog',
        '電話': 'phone',
        '店頭': 'walk_in',
        'ネット': 'web'
        // Add more channel mappings as needed
      },

      // Status mapping
      statusMapping: {
        '確定': 'confirmed',
        'キャンセル': 'cancelled',
        '仮予約': 'tentative',
        '来店済み': 'completed'
        // Add more status mappings as needed
      }
    };
  }

  async transformReservations(csvData, store, runId) {
    try {
      logger.debug(`Starting transformation for store ${store.store_id}`);

      // Parse CSV data
      const records = await this.parseCSV(csvData);
      logger.debug(`Parsed ${records.length} raw records from CSV`);

      if (records.length === 0) {
        logger.warn(`No records found in CSV for store ${store.store_id}`);
        return [];
      }

      // Transform each record
      const transformedRecords = [];
      const ingestionTs = dayjs().tz('Asia/Tokyo').format();

      for (let i = 0; i < records.length; i++) {
        try {
          const rawRecord = records[i];
          const transformedRecord = await this.transformRecord(
            rawRecord, 
            store, 
            runId, 
            ingestionTs
          );

          if (transformedRecord) {
            transformedRecords.push(transformedRecord);
          }
        } catch (error) {
          logger.warn(`Failed to transform record ${i + 1} for store ${store.store_id}: ${error.message}`);
          // Continue processing other records
        }
      }

      logger.info(`Successfully transformed ${transformedRecords.length} out of ${records.length} records for store ${store.store_id}`);
      return transformedRecords;

    } catch (error) {
      logger.error(`Failed to transform reservations for store ${store.store_id}:`, error);
      throw error;
    }
  }

  async parseCSV(csvData) {
    return new Promise((resolve, reject) => {
      const records = [];
      
      csv.parse(csvData, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        encoding: 'utf8'
      })
      .on('data', (record) => {
        records.push(record);
      })
      .on('error', (error) => {
        reject(error);
      })
      .on('end', () => {
        resolve(records);
      });
    });
  }

  async transformRecord(rawRecord, store, runId, ingestionTs) {
    try {
      // Map raw columns to common schema
      const mappedRecord = this.mapColumns(rawRecord);

      // Convert data types
      const convertedRecord = this.convertDataTypes(mappedRecord);

      // Generate record key
      const recordKey = this.generateRecordKey(convertedRecord, store);

      // Build final transformed record
      const transformedRecord = {
        // Core identification
        store_id: store.store_id,
        store_name: store.store_name,
        vendor: 'restaurant_board',
        
        // Reservation details
        reserve_date: convertedRecord.reserve_date,
        booking_date: convertedRecord.booking_date,
        start_time: convertedRecord.start_time,
        end_time: convertedRecord.end_time,
        course_name: convertedRecord.course_name || null,
        headcount: convertedRecord.headcount,
        channel: this.standardizeChannel(convertedRecord.channel_raw),
        status: this.standardizeStatus(convertedRecord.status_raw),

        // Metadata
        ingestion_ts: ingestionTs,
        run_id: runId,
        record_key: recordKey,
        record_hash: null // Will be calculated after record is complete
      };

      // Calculate content hash (excluding metadata fields)
      transformedRecord.record_hash = this.calculateRecordHash(transformedRecord);

      return transformedRecord;

    } catch (error) {
      logger.debug(`Error transforming record: ${error.message}`, { rawRecord });
      throw error;
    }
  }

  mapColumns(rawRecord) {
    const mappedRecord = {};
    
    // Map each column according to schema
    Object.entries(this.rbSchema.columnMapping).forEach(([rbColumn, commonColumn]) => {
      if (rawRecord.hasOwnProperty(rbColumn)) {
        mappedRecord[commonColumn] = rawRecord[rbColumn];
      }
    });

    return mappedRecord;
  }

  convertDataTypes(record) {
    const converted = { ...record };

    Object.entries(this.rbSchema.typeConversions).forEach(([field, type]) => {
      if (record[field]) {
        try {
          switch (type) {
            case 'date':
              converted[field.replace('_raw', '')] = this.convertToDate(record[field]);
              break;
            case 'time':
              converted[field.replace('_raw', '')] = this.convertToTime(record[field]);
              break;
            case 'integer':
              converted[field.replace('_raw', '')] = this.convertToInteger(record[field]);
              break;
          }
        } catch (error) {
          logger.debug(`Failed to convert ${field} value '${record[field]}' to ${type}: ${error.message}`);
          // Set to null if conversion fails
          converted[field.replace('_raw', '')] = null;
        }
      }
    });

    return converted;
  }

  convertToDate(dateStr) {
    if (!dateStr) return null;
    
    // Handle various date formats that might come from RB
    const formats = [
      'YYYY-MM-DD',
      'YYYY/MM/DD',
      'MM/DD/YYYY',
      'DD/MM/YYYY'
    ];

    for (const format of formats) {
      const parsed = dayjs(dateStr, format);
      if (parsed.isValid()) {
        return parsed.format('YYYY-MM-DD');
      }
    }

    throw new Error(`Invalid date format: ${dateStr}`);
  }

  convertToTime(timeStr) {
    if (!timeStr) return null;

    // Handle various time formats
    const timeRegex = /^(\d{1,2}):(\d{2})(?::(\d{2}))?/;
    const match = timeStr.match(timeRegex);
    
    if (match) {
      const hours = match[1].padStart(2, '0');
      const minutes = match[2];
      const seconds = match[3] || '00';
      return `${hours}:${minutes}:${seconds}`;
    }

    throw new Error(`Invalid time format: ${timeStr}`);
  }

  convertToInteger(numStr) {
    if (!numStr) return null;
    
    const num = parseInt(numStr, 10);
    if (isNaN(num)) {
      throw new Error(`Invalid integer: ${numStr}`);
    }
    
    return num;
  }

  standardizeChannel(rawChannel) {
    if (!rawChannel) return null;
    
    // Use mapping if available, otherwise return cleaned raw value
    return this.rbSchema.channelMapping[rawChannel] || 
           rawChannel.toLowerCase().replace(/\s+/g, '_');
  }

  standardizeStatus(rawStatus) {
    if (!rawStatus) return null;
    
    // Use mapping if available, otherwise return cleaned raw value
    return this.rbSchema.statusMapping[rawStatus] || 
           rawStatus.toLowerCase().replace(/\s+/g, '_');
  }

  generateRecordKey(record, store) {
    // Use reservation ID if available
    if (record.reservation_id) {
      return `${store.store_id}_${record.reservation_id}`;
    }

    // Generate composite key
    const keyParts = [
      store.store_id,
      record.reserve_date || 'no_date',
      record.start_time || 'no_time',
      record.course_name || 'no_course',
      record.headcount || '0',
      record.channel_raw || 'no_channel'
    ];

    return keyParts.join('|');
  }

  calculateRecordHash(record) {
    // Create hash from content fields (excluding metadata)
    const contentFields = {
      store_id: record.store_id,
      reserve_date: record.reserve_date,
      booking_date: record.booking_date,
      start_time: record.start_time,
      end_time: record.end_time,
      course_name: record.course_name,
      headcount: record.headcount,
      channel: record.channel,
      status: record.status
    };

    const contentString = JSON.stringify(contentFields, Object.keys(contentFields).sort());
    return crypto.createHash('md5').update(contentString).digest('hex');
  }

  async generateCSV(records) {
    return new Promise((resolve, reject) => {
      stringify(records, {
        header: true,
        quoted: true
      }, (error, output) => {
        if (error) {
          reject(error);
        } else {
          resolve(output);
        }
      });
    });
  }
}

module.exports = TransformService;
