"""
API ルート定義
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import List

from app.models.schemas import (
    StoreResponse, ETLRunResponse, QueryRequest, QueryResponse,
    ScrapingRequest, ScrapingResponse, HealthCheckResponse
)
from app.services.store_service import StoreMasterService
from app.services.bigquery_service import BigQueryService
from app.services.gcs_service import GCSService
from app.services.transformer import DataTransformer
from app.services.scraper_client import ScraperClient
from app.utils.logger import get_logger

logger = get_logger()

# ルーターの作成
router = APIRouter()


# 依存性注入
def get_store_service() -> StoreMasterService:
    return StoreMasterService()

def get_bq_service() -> BigQueryService:
    return BigQueryService()

def get_gcs_service() -> GCSService:
    return GCSService()

def get_transformer() -> DataTransformer:
    return DataTransformer()

def get_scraper_client() -> ScraperClient:
    return ScraperClient()


@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """ヘルスチェック"""
    return HealthCheckResponse(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0",
        services={}
    )


@router.get("/stores", response_model=List[StoreResponse])
async def get_stores(store_svc: StoreMasterService = Depends(get_store_service)):
    """店舗一覧を取得"""
    stores = await store_svc.get_active_stores()
    return stores


@router.get("/stores/{store_id}")
async def get_store(store_id: str, store_svc: StoreMasterService = Depends(get_store_service)):
    """店舗詳細を取得"""
    store = await store_svc.get_store_by_id(store_id)
    if not store:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    return store


@router.post("/etl/run", response_model=ETLRunResponse)
async def run_etl(background_tasks: BackgroundTasks):
    """ETL処理を実行"""
    # バックグラウンドタスクとして実装
    pass


@router.post("/query", response_model=QueryResponse)
async def query_reservations(
    request: QueryRequest,
    bq_svc: BigQueryService = Depends(get_bq_service)
):
    """予約データをクエリ"""
    try:
        data = await bq_svc.query_reservations(
            request.from_date,
            request.to_date,
            request.store_ids,
            request.limit
        )
        
        return QueryResponse(
            success=True,
            data=data,
            total_count=len(data)
        )
        
    except Exception as e:
        logger.error("クエリエラー", error=str(e))
        return QueryResponse(
            success=False,
            error=str(e)
        )


@router.post("/scrape", response_model=ScrapingResponse)
async def scrape_data(
    request: ScrapingRequest,
    scraper_svc: ScraperClient = Depends(get_scraper_client)
):
    """スクレイピング実行"""
    store = {
        'store_id': request.store_id,
        'store_name': request.store_name,
        'rb_username': request.rb_username,
        'rb_password': request.rb_password
    }
    
    result = await scraper_svc.retry_scraping(
        store, request.from_date, request.to_date, request.run_id
    )
    
    return result
