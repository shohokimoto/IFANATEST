/**
 * レストランボードスクレイピングサービス - Express API
 */
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const RestaurantBoardScraper = require('./services/scraper');
const logger = require('./utils/logger');
const config = require('./config');

const app = express();
const port = process.env.PORT || 3001;

// ミドルウェア設定
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// ログミドルウェア
app.use((req, res, next) => {
  logger.info('API Request', {
    method: req.method,
    url: req.url,
    ip: req.ip,
    userAgent: req.get('User-Agent')
  });
  next();
});

// ヘルスチェックエンドポイント
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: 'restaurant-board-scraper',
    version: '1.0.0'
  });
});

// ステータスエンドポイント
app.get('/status', async (req, res) => {
  try {
    const scraper = new RestaurantBoardScraper();
    await scraper.initialize();
    await scraper.cleanup();
    
    res.json({
      status: 'ready',
      timestamp: new Date().toISOString(),
      browser: 'available'
    });
  } catch (error) {
    logger.error('Status check failed', { error: error.message });
    res.status(500).json({
      status: 'error',
      timestamp: new Date().toISOString(),
      error: error.message
    });
  }
});

// スクレイピングエンドポイント
app.post('/api/scrape', async (req, res) => {
  const { store_id, store_name, rb_username, rb_password, from_date, to_date, run_id } = req.body;
  
  // バリデーション
  if (!store_id || !rb_username || !rb_password || !from_date || !to_date || !run_id) {
    return res.status(400).json({
      success: false,
      error: '必須パラメータが不足しています'
    });
  }
  
  const scraper = new RestaurantBoardScraper();
  
  try {
    logger.info('Scraping request received', {
      store_id,
      from_date,
      to_date,
      run_id
    });
    
    // スクレイピング実行
    const csvPath = await scraper.downloadReservationData(
      { store_id, store_name, rb_username, rb_password },
      from_date,
      to_date
    );
    
    // レコード数をカウント（簡易版）
    const fs = require('fs');
    const csvContent = fs.readFileSync(csvPath, 'utf8');
    const lines = csvContent.split('\n').filter(line => line.trim());
    const recordsCount = Math.max(0, lines.length - 1); // ヘッダー行を除く
    
    res.json({
      success: true,
      csv_path: csvPath,
      records_count: recordsCount
    });
    
    logger.info('Scraping completed', {
      store_id,
      csv_path: csvPath,
      records_count: recordsCount
    });
    
  } catch (error) {
    logger.error('Scraping failed', {
      store_id,
      error: error.message,
      run_id
    });
    
    res.status(500).json({
      success: false,
      error: error.message
    });
  } finally {
    // クリーンアップ
    try {
      await scraper.cleanup();
    } catch (cleanupError) {
      logger.error('Cleanup failed', { error: cleanupError.message });
    }
  }
});

// テストエンドポイント
app.post('/api/test', async (req, res) => {
  try {
    const scraper = new RestaurantBoardScraper();
    await scraper.initialize();
    
    res.json({
      success: true,
      message: 'Browser initialized successfully',
      timestamp: new Date().toISOString()
    });
    
    await scraper.cleanup();
  } catch (error) {
    logger.error('Test failed', { error: error.message });
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// エラーハンドリングミドルウェア
app.use((error, req, res, next) => {
  logger.error('Unhandled error', {
    error: error.message,
    stack: error.stack,
    url: req.url,
    method: req.method
  });
  
  res.status(500).json({
    success: false,
    error: 'Internal server error'
  });
});

// 404ハンドラー
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found'
  });
});

// サーバー起動
app.listen(port, '0.0.0.0', () => {
  logger.info(`Scraper service started on port ${port}`, {
    port,
    environment: process.env.NODE_ENV || 'development'
  });
});

// グレースフルシャットダウン
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  process.exit(0);
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down gracefully');
  process.exit(0);
});

module.exports = app;
