/**
 * ログ管理ユーティリティ
 */
const winston = require('winston');
const config = require('../config');

const logger = winston.createLogger({
  level: config.logLevel,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { service: 'restaurant-board-etl' },
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// パスワードなどの機密情報をマスクする関数
function maskSensitiveData(obj) {
  if (typeof obj !== 'object' || obj === null) {
    return obj;
  }
  
  const masked = { ...obj };
  const sensitiveKeys = ['password', 'rb_password', 'token', 'secret'];
  
  for (const key of sensitiveKeys) {
    if (masked[key]) {
      masked[key] = '***masked***';
    }
  }
  
  return masked;
}

// ログ出力時の機密情報マスキング
const originalLog = logger.log.bind(logger);
logger.log = function(level, message, meta) {
  if (meta) {
    meta = maskSensitiveData(meta);
  }
  return originalLog(level, message, meta);
};

module.exports = logger;
