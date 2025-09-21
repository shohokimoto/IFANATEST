"""
設定管理モジュール - Python版
"""
import os
from typing import Optional, List
from pydantic import BaseSettings, validator
from pydantic_settings import BaseSettings as PydanticBaseSettings


class Settings(PydanticBaseSettings):
    """アプリケーション設定"""
    
    # アプリケーション設定
    app_name: str = "Restaurant Board ETL API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Google Cloud設定
    project_id: str
    region: str = "asia-northeast1"
    bq_dataset: str = "rb"
    gcs_bucket: str
    
    # Google Sheets設定
    stores_sheet_id: str
    
    # 処理設定
    days_back: int = 7
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    
    # レストランボード設定
    rb_base_url: str = "https://manage.restaurant-board.jp"
    rb_login_url: str = "https://manage.restaurant-board.jp/login"
    
    # Node.js Scraper設定
    scraper_service_url: str = "http://localhost:3001"
    scraper_timeout: int = 300  # 5分
    
    # ログ設定
    log_level: str = "INFO"
    log_format: str = "json"
    
    # 処理設定
    max_retries: int = 3
    retry_delay: int = 1000  # 1秒
    timeout: int = 30000  # 30秒
    
    # BigQuery設定
    bq_table_prefix: str = "reservations_rb"
    stage_table_name: str = "stage_reservations_rb"
    main_table_name: str = "reservations_rb"
    
    # GCS設定
    gcs_path_prefix: str = "landing/restaurant_board"
    manual_path_prefix: str = "manual/restaurant_board"
    
    # API設定
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 1
    
    # セキュリティ設定
    secret_key: str = "your-secret-key-change-in-production"
    access_token_expire_minutes: int = 30
    
    # データベース設定（将来の拡張用）
    database_url: Optional[str] = None
    
    # Redis設定（Celery用）
    redis_url: str = "redis://localhost:6379"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        
    @validator('project_id', 'stores_sheet_id', 'gcs_bucket')
    def validate_required_settings(cls, v):
        if not v or v in ['your-project-id', 'your-sheet-id', 'your-gcs-bucket']:
            raise ValueError(f"必須設定が未設定です: {v}")
        return v
    
    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"ログレベルは {valid_levels} のいずれかである必要があります")
        return v.upper()


# グローバル設定インスタンス
settings = Settings()


def get_settings() -> Settings:
    """設定インスタンスを取得"""
    return settings
