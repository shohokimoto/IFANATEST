const winston = require('winston');
const config = require('../config');

// Create logger with Cloud Logging compatible format
const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { service: 'rb-scraper-etl' },
  transports: [
    // Console transport for Cloud Run
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize({ all: true }),
        winston.format.simple()
      )
    })
  ]
});

// Add file transport for local development
if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.File({
    filename: 'logs/error.log',
    level: 'error',
    format: winston.format.json()
  }));
  
  logger.add(new winston.transports.File({
    filename: 'logs/combined.log',
    format: winston.format.json()
  }));
}

module.exports = logger;
