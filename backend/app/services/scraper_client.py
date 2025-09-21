"""
Node.js Scraper通信クライアント - Python版
"""
from typing import Dict, Any, Optional
import httpx
import asyncio
from app.config import settings
from app.utils.logger import get_logger
from app.models.schemas import ScrapingRequest, ScrapingResponse

logger = get_logger()


class ScraperClient:
    """Node.js Scraperとの通信クライアント"""
    
    def __init__(self):
        self.base_url = settings.scraper_service_url
        self.timeout = settings.scraper_timeout
        
    async def scrape_reservation_data(self, store: Dict[str, Any], from_date: str, to_date: str, run_id: str) -> ScrapingResponse:
        """レストランボードから予約データをスクレイピング"""
        try:
            logger.info(
                "スクレイピング開始",
                store_id=store['store_id'],
                from_date=from_date,
                to_date=to_date,
                run_id=run_id
            )
            
            # リクエストデータを構築
            request_data = ScrapingRequest(
                store_id=store['store_id'],
                store_name=store.get('store_name'),
                rb_username=store['rb_username'],
                rb_password=store['rb_password'],
                from_date=from_date,
                to_date=to_date,
                run_id=run_id
            )
            
            # HTTPリクエストを送信
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/api/scrape",
                    json=request_data.dict()
                )
                
                if response.status_code == 200:
                    result = response.json()
                    scraping_response = ScrapingResponse(**result)
                    
                    logger.info(
                        "スクレイピング完了",
                        store_id=store['store_id'],
                        success=scraping_response.success,
                        records_count=scraping_response.records_count
                    )
                    
                    return scraping_response
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    logger.error("スクレイピング失敗", error=error_msg, store_id=store['store_id'])
                    
                    return ScrapingResponse(
                        success=False,
                        error=error_msg
                    )
                    
        except httpx.TimeoutException:
            error_msg = f"スクレイピングタイムアウト（{self.timeout}秒）"
            logger.error("スクレイピングタイムアウト", error=error_msg, store_id=store['store_id'])
            
            return ScrapingResponse(
                success=False,
                error=error_msg
            )
            
        except Exception as e:
            error_msg = str(e)
            logger.error("スクレイピングエラー", error=error_msg, store_id=store['store_id'])
            
            return ScrapingResponse(
                success=False,
                error=error_msg
            )
    
    async def health_check(self) -> bool:
        """Scraperサービスのヘルスチェック"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(f"{self.base_url}/health")
                return response.status_code == 200
                
        except Exception as e:
            logger.error("Scraperサービスヘルスチェック失敗", error=str(e))
            return False
    
    async def get_scraper_status(self) -> Dict[str, Any]:
        """Scraperサービスのステータス取得"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(f"{self.base_url}/status")
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return {
                        'status': 'error',
                        'message': f"HTTP {response.status_code}: {response.text}"
                    }
                    
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }
    
    async def test_scraping(self, test_store: Dict[str, Any]) -> ScrapingResponse:
        """テスト用スクレイピング"""
        try:
            logger.info("テストスクレイピング開始", store_id=test_store['store_id'])
            
            # テスト用の期間設定（過去1日）
            from datetime import datetime, timedelta
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            return await self.scrape_reservation_data(
                test_store,
                yesterday.strftime('%Y-%m-%d'),
                today.strftime('%Y-%m-%d'),
                'test_run'
            )
            
        except Exception as e:
            logger.error("テストスクレイピングエラー", error=str(e))
            return ScrapingResponse(
                success=False,
                error=str(e)
            )
    
    async def retry_scraping(self, store: Dict[str, Any], from_date: str, to_date: str, run_id: str, max_retries: int = 3) -> ScrapingResponse:
        """リトライ機能付きスクレイピング"""
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "スクレイピング試行",
                    store_id=store['store_id'],
                    attempt=attempt,
                    max_retries=max_retries
                )
                
                result = await self.scrape_reservation_data(store, from_date, to_date, run_id)
                
                if result.success:
                    return result
                
                last_error = result.error
                
                if attempt < max_retries:
                    # 指数バックオフで待機
                    delay = settings.retry_delay * (2 ** (attempt - 1))
                    logger.info("リトライ待機中", delay=delay)
                    await asyncio.sleep(delay / 1000)  # ミリ秒を秒に変換
                    
            except Exception as e:
                last_error = str(e)
                if attempt < max_retries:
                    delay = settings.retry_delay * (2 ** (attempt - 1))
                    await asyncio.sleep(delay / 1000)
        
        # すべてのリトライが失敗
        error_msg = f"スクレイピングが最大試行回数に達しました: {last_error}"
        logger.error("スクレイピング最終失敗", error=error_msg, store_id=store['store_id'])
        
        return ScrapingResponse(
            success=False,
            error=error_msg
        )
    
    async def batch_scrape(self, stores: list, from_date: str, to_date: str, run_id: str) -> list:
        """複数店舗の一括スクレイピング"""
        results = []
        
        # 並行処理でスクレイピング実行
        tasks = []
        for store in stores:
            task = self.retry_scraping(store, from_date, to_date, run_id)
            tasks.append(task)
        
        # すべてのタスクを並行実行
        scraping_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 結果を整理
        for i, result in enumerate(scraping_results):
            store_id = stores[i]['store_id']
            
            if isinstance(result, Exception):
                results.append({
                    'store_id': store_id,
                    'success': False,
                    'error': str(result),
                    'csv_path': None,
                    'records_count': 0
                })
            else:
                results.append({
                    'store_id': store_id,
                    'success': result.success,
                    'error': result.error,
                    'csv_path': result.csv_path,
                    'records_count': result.records_count or 0
                })
        
        success_count = sum(1 for r in results if r['success'])
        failure_count = len(results) - success_count
        
        logger.info(
            "一括スクレイピング完了",
            total=len(stores),
            success=success_count,
            failure=failure_count
        )
        
        return results
