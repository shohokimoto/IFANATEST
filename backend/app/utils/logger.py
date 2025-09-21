"""
ログ管理ユーティリティ - Python版
"""
import logging
import sys
from typing import Any, Dict, Optional
from loguru import logger
import json
from functools import wraps
from app.config import settings


class SensitiveDataMasker:
    """機密情報をマスクするクラス"""
    
    def __init__(self):
        self.sensitive_keys = {
            'password', 'rb_password', 'token', 'secret', 
            'api_key', 'access_token', 'refresh_token'
        }
    
    def mask_data(self, data: Any) -> Any:
        """データ内の機密情報をマスク"""
        if isinstance(data, dict):
            masked_data = {}
            for key, value in data.items():
                if any(sensitive_key in key.lower() for sensitive_key in self.sensitive_keys):
                    masked_data[key] = "***masked***"
                else:
                    masked_data[key] = self.mask_data(value)
            return masked_data
        elif isinstance(data, list):
            return [self.mask_data(item) for item in data]
        else:
            return data


class RestaurantBoardLogger:
    """レストランボードETL用ロガー"""
    
    def __init__(self):
        self.masker = SensitiveDataMasker()
        self._setup_logger()
    
    def _setup_logger(self):
        """ロガーの初期設定"""
        # 既存のハンドラーを削除
        logger.remove()
        
        # コンソール出力設定
        if settings.log_format == "json":
            # JSON形式でログ出力
            logger.add(
                sys.stdout,
                format=self._json_formatter,
                level=settings.log_level,
                serialize=False
            )
        else:
            # 人間が読みやすい形式でログ出力
            logger.add(
                sys.stdout,
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                       "<level>{level: <8}</level> | "
                       "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                       "<level>{message}</level>",
                level=settings.log_level,
                colorize=True
            )
        
        # ファイル出力設定（本番環境用）
        if not settings.debug:
            logger.add(
                "logs/app.log",
                rotation="100 MB",
                retention="30 days",
                compression="zip",
                level=settings.log_level,
                format=self._json_formatter
            )
    
    def _json_formatter(self, record: Dict[str, Any]) -> str:
        """JSON形式のログフォーマッター"""
        # 機密情報をマスク
        masked_record = self.masker.mask_data(record)
        
        # ログレコードを整形
        log_data = {
            "timestamp": masked_record["time"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": masked_record["level"].name,
            "logger": masked_record["name"],
            "function": masked_record["function"],
            "line": masked_record["line"],
            "message": masked_record["message"],
            "service": "restaurant-board-etl-api"
        }
        
        # 追加フィールドがあれば追加
        if "extra" in masked_record and masked_record["extra"]:
            log_data.update(masked_record["extra"])
        
        return json.dumps(log_data, ensure_ascii=False)
    
    def info(self, message: str, **kwargs):
        """情報ログ"""
        logger.bind(**kwargs).info(message)
    
    def warning(self, message: str, **kwargs):
        """警告ログ"""
        logger.bind(**kwargs).warning(message)
    
    def error(self, message: str, **kwargs):
        """エラーログ"""
        logger.bind(**kwargs).error(message)
    
    def debug(self, message: str, **kwargs):
        """デバッグログ"""
        logger.bind(**kwargs).debug(message)
    
    def critical(self, message: str, **kwargs):
        """クリティカルログ"""
        logger.bind(**kwargs).critical(message)


# グローバルロガーインスタンス
app_logger = RestaurantBoardLogger()


def log_function_call(func):
    """関数呼び出しをログに記録するデコレータ"""
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        func_name = f"{func.__module__}.{func.__name__}"
        app_logger.info(f"関数開始: {func_name}", function=func_name, args_count=len(args))
        
        try:
            result = await func(*args, **kwargs)
            app_logger.info(f"関数完了: {func_name}", function=func_name, success=True)
            return result
        except Exception as e:
            app_logger.error(
                f"関数エラー: {func_name}", 
                function=func_name, 
                error=str(e), 
                error_type=type(e).__name__
            )
            raise
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        func_name = f"{func.__module__}.{func.__name__}"
        app_logger.info(f"関数開始: {func_name}", function=func_name, args_count=len(args))
        
        try:
            result = func(*args, **kwargs)
            app_logger.info(f"関数完了: {func_name}", function=func_name, success=True)
            return result
        except Exception as e:
            app_logger.error(
                f"関数エラー: {func_name}", 
                function=func_name, 
                error=str(e), 
                error_type=type(e).__name__
            )
            raise
    
    # 非同期関数かどうかを判定
    if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:  # CO_COROUTINE
        return async_wrapper
    else:
        return sync_wrapper


def get_logger() -> RestaurantBoardLogger:
    """ロガーインスタンスを取得"""
    return app_logger
