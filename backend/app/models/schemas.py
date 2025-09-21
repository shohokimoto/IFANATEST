"""
Pydantic スキーマ定義
"""
from datetime import datetime, date, time
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class VendorEnum(str, Enum):
    """ベンダー列挙型"""
    RESTAURANT_BOARD = "restaurant_board"
    EBICA = "ebica"
    TORETA = "toreta"
    TABLECHECK = "tablecheck"


class StatusEnum(str, Enum):
    """予約ステータス列挙型"""
    CONFIRMED = "確定"
    CANCELLED = "キャンセル"
    PENDING = "保留"
    COMPLETED = "完了"


class StoreBase(BaseModel):
    """店舗ベースモデル"""
    store_id: str = Field(..., description="店舗ID")
    store_name: Optional[str] = Field(None, description="店舗名")
    rb_username: str = Field(..., description="レストランボードユーザー名")
    rb_password: str = Field(..., description="レストランボードパスワード")
    days_back: int = Field(7, description="遡り日数")
    from_date: Optional[str] = Field(None, description="開始日")
    to_date: Optional[str] = Field(None, description="終了日")
    note: Optional[str] = Field(None, description="備考")
    active: bool = Field(True, description="アクティブフラグ")


class StoreCreate(StoreBase):
    """店舗作成モデル"""
    pass


class StoreResponse(StoreBase):
    """店舗応答モデル"""
    class Config:
        from_attributes = True


class ReservationBase(BaseModel):
    """予約データベースモデル"""
    store_id: str = Field(..., description="店舗ID")
    store_name: Optional[str] = Field(None, description="店舗名")
    reserve_date: date = Field(..., description="予約日")
    booking_date: Optional[date] = Field(None, description="予約受付日")
    start_time: Optional[time] = Field(None, description="予約開始時間")
    end_time: Optional[time] = Field(None, description="予約終了時間")
    course_name: Optional[str] = Field(None, description="コース名")
    headcount: Optional[int] = Field(None, description="人数")
    channel: Optional[str] = Field(None, description="経路")
    status: Optional[str] = Field(None, description="予約ステータス")
    vendor: VendorEnum = Field(VendorEnum.RESTAURANT_BOARD, description="ベンダー")
    record_key: str = Field(..., description="レコードキー")
    record_hash: str = Field(..., description="内容ハッシュ")


class ReservationCreate(ReservationBase):
    """予約データ作成モデル"""
    pass


class ReservationResponse(ReservationBase):
    """予約データ応答モデル"""
    ingestion_ts: datetime = Field(..., description="取込時刻")
    run_id: str = Field(..., description="実行ID")
    created_at: datetime = Field(..., description="作成日時")
    updated_at: datetime = Field(..., description="更新日時")
    
    class Config:
        from_attributes = True


class ETLRunBase(BaseModel):
    """ETL実行ベースモデル"""
    run_id: str = Field(..., description="実行ID")
    start_time: datetime = Field(..., description="開始時刻")
    stores: List[Dict[str, Any]] = Field(default_factory=list, description="店舗処理結果")


class ETLRunCreate(ETLRunBase):
    """ETL実行作成モデル"""
    pass


class ETLRunResponse(ETLRunBase):
    """ETL実行応答モデル"""
    end_time: Optional[datetime] = Field(None, description="終了時刻")
    duration: Optional[int] = Field(None, description="実行時間（秒）")
    summary: Dict[str, Any] = Field(default_factory=dict, description="実行サマリ")
    success: bool = Field(False, description="成功フラグ")
    error: Optional[str] = Field(None, description="エラーメッセージ")
    
    class Config:
        from_attributes = True


class ScrapingRequest(BaseModel):
    """スクレイピングリクエストモデル"""
    store_id: str = Field(..., description="店舗ID")
    store_name: Optional[str] = Field(None, description="店舗名")
    rb_username: str = Field(..., description="レストランボードユーザー名")
    rb_password: str = Field(..., description="レストランボードパスワード")
    from_date: str = Field(..., description="開始日 (YYYY-MM-DD)")
    to_date: str = Field(..., description="終了日 (YYYY-MM-DD)")
    run_id: str = Field(..., description="実行ID")


class ScrapingResponse(BaseModel):
    """スクレイピング応答モデル"""
    success: bool = Field(..., description="成功フラグ")
    csv_path: Optional[str] = Field(None, description="CSVファイルパス")
    error: Optional[str] = Field(None, description="エラーメッセージ")
    records_count: Optional[int] = Field(None, description="レコード数")


class DataTransformRequest(BaseModel):
    """データ変換リクエストモデル"""
    csv_path: str = Field(..., description="CSVファイルパス")
    store_id: str = Field(..., description="店舗ID")
    store_name: Optional[str] = Field(None, description="店舗名")
    run_id: str = Field(..., description="実行ID")


class DataTransformResponse(BaseModel):
    """データ変換応答モデル"""
    success: bool = Field(..., description="成功フラグ")
    normalized_csv_path: Optional[str] = Field(None, description="正規化CSVファイルパス")
    records_count: int = Field(0, description="レコード数")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class GCSUploadRequest(BaseModel):
    """GCSアップロードリクエストモデル"""
    local_file_path: str = Field(..., description="ローカルファイルパス")
    store_id: str = Field(..., description="店舗ID")
    run_id: str = Field(..., description="実行ID")
    is_manual: bool = Field(False, description="手動アップロードフラグ")


class GCSUploadResponse(BaseModel):
    """GCSアップロード応答モデル"""
    success: bool = Field(..., description="成功フラグ")
    object_name: Optional[str] = Field(None, description="GCSオブジェクト名")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class BigQueryLoadRequest(BaseModel):
    """BigQueryロードリクエストモデル"""
    gcs_object_name: str = Field(..., description="GCSオブジェクト名")
    run_id: str = Field(..., description="実行ID")


class BigQueryLoadResponse(BaseModel):
    """BigQueryロード応答モデル"""
    success: bool = Field(..., description="成功フラグ")
    job_id: Optional[str] = Field(None, description="ジョブID")
    output_rows: int = Field(0, description="出力行数")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class BigQueryMergeRequest(BaseModel):
    """BigQuery MERGEリクエストモデル"""
    run_id: str = Field(..., description="実行ID")


class BigQueryMergeResponse(BaseModel):
    """BigQuery MERGE応答モデル"""
    success: bool = Field(..., description="成功フラグ")
    job_id: Optional[str] = Field(None, description="ジョブID")
    result: Optional[Dict[str, int]] = Field(None, description="MERGE結果")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class QueryRequest(BaseModel):
    """クエリリクエストモデル"""
    from_date: str = Field(..., description="開始日 (YYYY-MM-DD)")
    to_date: str = Field(..., description="終了日 (YYYY-MM-DD)")
    store_ids: Optional[List[str]] = Field(None, description="店舗IDリスト")
    limit: Optional[int] = Field(1000, description="取得件数制限")


class QueryResponse(BaseModel):
    """クエリ応答モデル"""
    success: bool = Field(..., description="成功フラグ")
    data: List[Dict[str, Any]] = Field(default_factory=list, description="クエリ結果")
    total_count: int = Field(0, description="総件数")
    error: Optional[str] = Field(None, description="エラーメッセージ")


class HealthCheckResponse(BaseModel):
    """ヘルスチェック応答モデル"""
    status: str = Field(..., description="ステータス")
    timestamp: datetime = Field(..., description="チェック時刻")
    version: str = Field(..., description="アプリケーションバージョン")
    services: Dict[str, str] = Field(default_factory=dict, description="サービスステータス")


class ErrorResponse(BaseModel):
    """エラー応答モデル"""
    error: str = Field(..., description="エラーメッセージ")
    detail: Optional[str] = Field(None, description="詳細情報")
    timestamp: datetime = Field(default_factory=datetime.now, description="エラー発生時刻")
