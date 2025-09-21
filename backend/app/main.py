"""
FastAPI メインアプリケーション
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uvicorn
from datetime import datetime
import uuid

from app.config import settings
from app.utils.logger import get_logger
from app.models.schemas import (
    HealthCheckResponse, ErrorResponse, ETLRunResponse, 
    QueryRequest, QueryResponse, ScrapingRequest, ScrapingResponse
)
from app.services.store_service import StoreMasterService
from app.services.bigquery_service import BigQueryService
from app.services.gcs_service import GCSService
from app.services.transformer import DataTransformer
from app.services.scraper_client import ScraperClient

logger = get_logger()

# グローバルサービスインスタンス
store_service = None
bq_service = None
gcs_service = None
transformer = None
scraper_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """アプリケーションのライフサイクル管理"""
    global store_service, bq_service, gcs_service, transformer, scraper_client
    
    # 起動時の初期化
    logger.info("FastAPIアプリケーション起動中...")
    
    try:
        # サービスインスタンスの初期化
        store_service = StoreMasterService()
        bq_service = BigQueryService()
        gcs_service = GCSService()
        transformer = DataTransformer()
        scraper_client = ScraperClient()
        
        # 依存サービスの初期化確認
        await bq_service.ensure_dataset_exists()
        await bq_service.ensure_tables_exist()
        await gcs_service.ensure_bucket_exists()
        
        logger.info("FastAPIアプリケーション起動完了")
        
    except Exception as e:
        logger.error("アプリケーション起動エラー", error=str(e))
        raise
    
    yield
    
    # シャットダウン時の処理
    logger.info("FastAPIアプリケーションシャットダウン中...")


# FastAPIアプリケーションの作成
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="レストランボード予約データETLシステム API",
    lifespan=lifespan
)

# CORS設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 本番環境では適切に設定
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 依存性注入
def get_store_service() -> StoreMasterService:
    return store_service

def get_bq_service() -> BigQueryService:
    return bq_service

def get_gcs_service() -> GCSService:
    return gcs_service

def get_transformer() -> DataTransformer:
    return transformer

def get_scraper_client() -> ScraperClient:
    return scraper_client


@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """ヘルスチェックエンドポイント"""
    try:
        services = {}
        
        # 各サービスのヘルスチェック
        try:
            scraper_health = await scraper_client.health_check()
            services['scraper'] = 'healthy' if scraper_health else 'unhealthy'
        except:
            services['scraper'] = 'unhealthy'
        
        services['bigquery'] = 'healthy'  # 初期化時に確認済み
        services['gcs'] = 'healthy'  # 初期化時に確認済み
        services['store_master'] = 'healthy'  # 初期化時に確認済み
        
        return HealthCheckResponse(
            status="healthy",
            timestamp=datetime.now(),
            version=settings.app_version,
            services=services
        )
        
    except Exception as e:
        logger.error("ヘルスチェックエラー", error=str(e))
        return HealthCheckResponse(
            status="unhealthy",
            timestamp=datetime.now(),
            version=settings.app_version,
            services={}
        )


@app.get("/")
async def root():
    """ルートエンドポイント"""
    return {
        "message": "レストランボード予約データETLシステム API",
        "version": settings.app_version,
        "docs": "/docs"
    }


@app.post("/api/etl/run", response_model=ETLRunResponse)
async def run_etl(
    background_tasks: BackgroundTasks,
    store_svc: StoreMasterService = Depends(get_store_service),
    bq_svc: BigQueryService = Depends(get_bq_service),
    gcs_svc: GCSService = Depends(get_gcs_service),
    transform_svc: DataTransformer = Depends(get_transformer),
    scraper_svc: ScraperClient = Depends(get_scraper_client)
):
    """ETL処理を実行"""
    run_id = str(uuid.uuid4())
    start_time = datetime.now()
    
    try:
        logger.info("ETL処理開始", run_id=run_id, start_time=start_time)
        
        # バックグラウンドでETL処理を実行
        background_tasks.add_task(
            execute_etl_process,
            run_id, start_time, store_svc, bq_svc, gcs_svc, transform_svc, scraper_svc
        )
        
        return ETLRunResponse(
            run_id=run_id,
            start_time=start_time,
            stores=[],
            summary={
                "totalStores": 0,
                "successfulStores": 0,
                "failedStores": 0,
                "totalRecords": 0,
                "totalErrors": 0
            },
            success=True
        )
        
    except Exception as e:
        logger.error("ETL処理開始エラー", error=str(e), run_id=run_id)
        raise HTTPException(status_code=500, detail=str(e))


async def execute_etl_process(
    run_id: str,
    start_time: datetime,
    store_svc: StoreMasterService,
    bq_svc: BigQueryService,
    gcs_svc: GCSService,
    transform_svc: DataTransformer,
    scraper_svc: ScraperClient
):
    """ETL処理の実行情報"""
    try:
        # 1. 店舗マスタ取得
        stores = await store_svc.get_active_stores()
        
        if not stores:
            logger.warning("処理対象の店舗がありません", run_id=run_id)
            return
        
        # 2. 各店舗の処理
        results = []
        successful_stores = 0
        failed_stores = 0
        total_records = 0
        
        for store in stores:
            try:
                # 処理期間を計算
                date_range = store_svc.calculate_date_range(store)
                
                # スクレイピング
                scraping_result = await scraper_svc.retry_scraping(
                    store, date_range['from_date'], date_range['to_date'], run_id
                )
                
                if not scraping_result.success:
                    failed_stores += 1
                    results.append({
                        'store_id': store['store_id'],
                        'success': False,
                        'error': scraping_result.error
                    })
                    continue
                
                # データ変換
                normalized_path = await transform_svc.transform_csv(
                    scraping_result.csv_path, store, run_id
                )
                
                # GCSアップロード
                gcs_result = await gcs_svc.upload_csv_file(
                    normalized_path, {
                        'store_id': store['store_id'],
                        'run_id': run_id
                    }
                )
                
                # BigQueryロード
                load_result = await bq_svc.load_data_from_gcs(gcs_result, run_id)
                
                successful_stores += 1
                total_records += load_result['output_rows']
                
                results.append({
                    'store_id': store['store_id'],
                    'success': True,
                    'records': load_result['output_rows']
                })
                
            except Exception as e:
                failed_stores += 1
                logger.error("店舗処理エラー", error=str(e), store_id=store['store_id'])
                results.append({
                    'store_id': store['store_id'],
                    'success': False,
                    'error': str(e)
                })
        
        # 3. MERGE処理
        if successful_stores > 0:
            merge_result = await bq_svc.merge_stage_to_main(run_id)
            logger.info("MERGE処理完了", run_id=run_id, result=merge_result['result'])
        
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())
        
        logger.info(
            "ETL処理完了",
            run_id=run_id,
            duration=duration,
            successful_stores=successful_stores,
            failed_stores=failed_stores,
            total_records=total_records
        )
        
    except Exception as e:
        logger.error("ETL処理で致命的エラー", error=str(e), run_id=run_id)


@app.post("/api/query", response_model=QueryResponse)
async def query_reservations(
    request: QueryRequest,
    bq_svc: BigQueryService = Depends(get_bq_service)
):
    """予約データをクエリ"""
    try:
        logger.info("予約データクエリ開始", **request.dict())
        
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
        logger.error("予約データクエリエラー", error=str(e))
        return QueryResponse(
            success=False,
            error=str(e)
        )


@app.post("/api/scrape", response_model=ScrapingResponse)
async def scrape_data(
    request: ScrapingRequest,
    scraper_svc: ScraperClient = Depends(get_scraper_client)
):
    """スクレイピング実行（テスト用）"""
    try:
        logger.info("スクレイピング実行", store_id=request.store_id)
        
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
        
    except Exception as e:
        logger.error("スクレイピング実行エラー", error=str(e))
        return ScrapingResponse(
            success=False,
            error=str(e)
        )


@app.get("/api/stores")
async def get_stores(store_svc: StoreMasterService = Depends(get_store_service)):
    """店舗一覧を取得"""
    try:
        stores = await store_svc.get_active_stores()
        
        # パスワードをマスク
        for store in stores:
            if 'rb_password' in store:
                store['rb_password'] = '***masked***'
        
        return {"stores": stores, "count": len(stores)}
        
    except Exception as e:
        logger.error("店舗一覧取得エラー", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def get_status(
    bq_svc: BigQueryService = Depends(get_bq_service),
    gcs_svc: GCSService = Depends(get_gcs_service),
    scraper_svc: ScraperClient = Depends(get_scraper_client)
):
    """システムステータス取得"""
    try:
        status = {
            "timestamp": datetime.now(),
            "version": settings.app_version,
            "services": {}
        }
        
        # BigQueryステータス
        try:
            row_count = await bq_svc.get_table_row_count('reservations_rb')
            status["services"]["bigquery"] = {
                "status": "healthy",
                "main_table_rows": row_count
            }
        except Exception as e:
            status["services"]["bigquery"] = {
                "status": "error",
                "error": str(e)
            }
        
        # GCSステータス
        try:
            usage = await gcs_svc.get_bucket_usage()
            status["services"]["gcs"] = {
                "status": "healthy",
                "usage": usage
            }
        except Exception as e:
            status["services"]["gcs"] = {
                "status": "error",
                "error": str(e)
            }
        
        # Scraperステータス
        scraper_status = await scraper_svc.get_scraper_status()
        status["services"]["scraper"] = scraper_status
        
        return status
        
    except Exception as e:
        logger.error("ステータス取得エラー", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """グローバル例外ハンドラー"""
    logger.error("未処理の例外", error=str(exc), path=request.url.path)
    
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="内部サーバーエラー",
            detail=str(exc)
        ).dict()
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        reload=settings.debug
    )
