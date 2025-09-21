const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const logger = require('./utils/logger');
const config = require('./config');
const StoreService = require('./services/store-service');
const ScraperService = require('./services/scraper-service');
const TransformService = require('./services/transform-service');
const GcsService = require('./services/gcs-service');
const BigQueryService = require('./services/bigquery-service');

// Configure dayjs for JST
dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.tz.setDefault('Asia/Tokyo');

class RBScraperETL {
  constructor() {
    this.runId = `rb_${dayjs().format('YYYYMMDD_HHmmss')}`;
    this.storeService = new StoreService();
    this.scraperService = new ScraperService();
    this.transformService = new TransformService();
    this.gcsService = new GcsService();
    this.bigQueryService = new BigQueryService();
  }

  async run() {
    try {
      logger.info(`Starting RB Scraper ETL process - Run ID: ${this.runId}`);
      
      // 1. Get store master data from Google Sheets
      const stores = await this.storeService.getActiveStores();
      logger.info(`Found ${stores.length} active stores to process`);

      if (stores.length === 0) {
        logger.warn('No active stores found, exiting');
        return;
      }

      const results = {
        success: [],
        failed: [],
        totalRecords: 0
      };

      // 2. Process each store
      for (const store of stores) {
        try {
          logger.info(`Processing store: ${store.store_id} (${store.store_name})`);
          
          // Calculate date range
          const dateRange = this.calculateDateRange(store);
          logger.info(`Date range for ${store.store_id}: ${dateRange.from} to ${dateRange.to}`);

          // 3. Scrape reservation data
          const csvData = await this.scraperService.scrapeReservations(store, dateRange);
          
          if (!csvData || csvData.length === 0) {
            logger.warn(`No data scraped for store ${store.store_id}`);
            continue;
          }

          // 4. Transform and normalize data
          const transformedData = await this.transformService.transformReservations(
            csvData, 
            store, 
            this.runId
          );

          if (transformedData.length === 0) {
            logger.warn(`No valid records after transformation for store ${store.store_id}`);
            continue;
          }

          // 5. Upload to GCS
          const gcsPath = await this.gcsService.uploadTransformedData(
            transformedData, 
            store.store_id, 
            this.runId
          );

          // 6. Load to BigQuery stage table
          const loadJobResult = await this.bigQueryService.loadToStageTable(gcsPath, this.runId);

          results.success.push({
            store_id: store.store_id,
            store_name: store.store_name,
            records: transformedData.length,
            gcs_path: gcsPath,
            load_job_id: loadJobResult.id
          });

          results.totalRecords += transformedData.length;
          logger.info(`Successfully processed store ${store.store_id}: ${transformedData.length} records`);

        } catch (error) {
          logger.error(`Failed to process store ${store.store_id}:`, error);
          results.failed.push({
            store_id: store.store_id,
            store_name: store.store_name,
            error: error.message
          });
        }
      }

      // 7. Execute MERGE operation if we have successful loads
      if (results.success.length > 0) {
        logger.info('Executing MERGE operation to production table');
        await this.bigQueryService.mergeStageToProduction(this.runId);
        logger.info('MERGE operation completed successfully');
      }

      // 8. Log final results
      this.logResults(results);

      // Exit with error if any stores failed
      if (results.failed.length > 0) {
        process.exit(1);
      }

    } catch (error) {
      logger.error('Fatal error in RB Scraper ETL process:', error);
      process.exit(1);
    }
  }

  calculateDateRange(store) {
    const today = dayjs().tz('Asia/Tokyo');
    
    // Use explicit date range if provided
    if (store.from_date && store.to_date) {
      return {
        from: dayjs(store.from_date).format('YYYY-MM-DD'),
        to: dayjs(store.to_date).format('YYYY-MM-DD')
      };
    }

    // Use days_back configuration (default: 7 days back + yesterday)
    const daysBack = parseInt(store.days_back) || config.DAYS_BACK || 7;
    const fromDate = today.subtract(daysBack + 1, 'day');
    const toDate = today.subtract(1, 'day'); // Yesterday

    return {
      from: fromDate.format('YYYY-MM-DD'),
      to: toDate.format('YYYY-MM-DD')
    };
  }

  logResults(results) {
    logger.info('=== ETL Process Summary ===');
    logger.info(`Run ID: ${this.runId}`);
    logger.info(`Successful stores: ${results.success.length}`);
    logger.info(`Failed stores: ${results.failed.length}`);
    logger.info(`Total records processed: ${results.totalRecords}`);

    if (results.success.length > 0) {
      logger.info('Successful stores:');
      results.success.forEach(result => {
        logger.info(`  - ${result.store_id} (${result.store_name}): ${result.records} records`);
      });
    }

    if (results.failed.length > 0) {
      logger.error('Failed stores:');
      results.failed.forEach(result => {
        logger.error(`  - ${result.store_id} (${result.store_name}): ${result.error}`);
      });
    }
  }
}

// Main execution
async function main() {
  try {
    const etl = new RBScraperETL();
    await etl.run();
    logger.info('RB Scraper ETL process completed successfully');
  } catch (error) {
    logger.error('RB Scraper ETL process failed:', error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  logger.info('Received SIGINT, shutting down gracefully');
  process.exit(0);
});

process.on('SIGTERM', () => {
  logger.info('Received SIGTERM, shutting down gracefully');
  process.exit(0);
});

// Run if this file is executed directly
if (require.main === module) {
  main();
}

module.exports = RBScraperETL;
