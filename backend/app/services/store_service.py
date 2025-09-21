"""
店舗マスタサービス - Python版
"""
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
from googleapiclient.discovery import build
from google.auth import default
from app.config import settings
from app.utils.logger import get_logger
from app.models.schemas import StoreBase

logger = get_logger()


class StoreMasterService:
    """店舗マスタ管理サービス"""
    
    def __init__(self):
        self.sheets_service = None
        self._initialize_sheets_service()
    
    def _initialize_sheets_service(self):
        """Google Sheets APIサービスの初期化"""
        try:
            credentials, _ = default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
            self.sheets_service = build('sheets', 'v4', credentials=credentials)
            logger.info("Google Sheets APIサービスの初期化完了")
        except Exception as e:
            logger.error("Google Sheets APIサービスの初期化に失敗", error=str(e))
            raise
    
    async def get_active_stores(self) -> List[Dict[str, Any]]:
        """アクティブな店舗一覧を取得"""
        try:
            logger.info("店舗マスタの取得を開始", sheet_id=settings.stores_sheet_id)
            
            # Google Sheetsからデータを取得
            request = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=settings.stores_sheet_id,
                range='Stores!A:I'  # active, store_id, store_name, rb_username, rb_password, days_back, from_date, to_date, note
            )
            response = request.execute()
            
            values = response.get('values', [])
            if not values or len(values) < 2:
                logger.warning("店舗マスタデータが見つかりません")
                return []
            
            # ヘッダー行をスキップ
            headers = values[0]
            data_rows = values[1:]
            
            # データフレームに変換
            df = pd.DataFrame(data_rows, columns=headers)
            
            # アクティブな店舗のみフィルタリング
            active_stores = df[df['active'].astype(str).str.lower() == 'true']
            
            # 辞書リストに変換
            stores = []
            for _, row in active_stores.iterrows():
                store = self._normalize_store_data(row.to_dict())
                stores.append(store)
            
            logger.info(
                "店舗マスタの取得完了",
                total_rows=len(values) - 1,
                active_stores=len(stores),
                store_ids=[s['store_id'] for s in stores]
            )
            
            return stores
            
        except Exception as e:
            logger.error("店舗マスタの取得に失敗", error=str(e))
            raise
    
    def _normalize_store_data(self, store_data: Dict[str, Any]) -> Dict[str, Any]:
        """店舗データを正規化"""
        normalized = {
            'store_id': str(store_data.get('store_id', '')).strip(),
            'store_name': str(store_data.get('store_name', '')).strip() if store_data.get('store_name') else None,
            'rb_username': str(store_data.get('rb_username', '')).strip(),
            'rb_password': str(store_data.get('rb_password', '')).strip(),
            'days_back': int(store_data.get('days_back', settings.days_back)),
            'from_date': str(store_data.get('from_date', '')).strip() if store_data.get('from_date') else None,
            'to_date': str(store_data.get('to_date', '')).strip() if store_data.get('to_date') else None,
            'note': str(store_data.get('note', '')).strip() if store_data.get('note') else None,
            'active': True
        }
        
        # 必須項目の検証
        required_fields = ['store_id', 'rb_username', 'rb_password']
        for field in required_fields:
            if not normalized[field]:
                raise ValueError(f"店舗 {normalized['store_id']} の必須項目が不足: {field}")
        
        return normalized
    
    def calculate_date_range(self, store: Dict[str, Any]) -> Dict[str, str]:
        """処理期間を計算"""
        today = datetime.now().date()
        today_str = today.strftime('%Y-%m-%d')
        
        if store.get('from_date') and store.get('to_date'):
            # 明示的に指定された期間を使用
            from_date = store['from_date']
            to_date = store['to_date']
        else:
            # デフォルト: 前日 + days_back日
            from_date_obj = today - timedelta(days=1 + store.get('days_back', settings.days_back))
            from_date = from_date_obj.strftime('%Y-%m-%d')
            to_date = today_str
        
        return {
            'from_date': from_date,
            'to_date': to_date
        }
    
    async def get_store_by_id(self, store_id: str) -> Optional[Dict[str, Any]]:
        """店舗IDで店舗情報を取得"""
        stores = await self.get_active_stores()
        for store in stores:
            if store['store_id'] == store_id:
                return store
        return None
    
    async def validate_store_data(self, store_data: Dict[str, Any]) -> bool:
        """店舗データの妥当性を検証"""
        try:
            # 必須項目のチェック
            required_fields = ['store_id', 'rb_username', 'rb_password']
            for field in required_fields:
                if not store_data.get(field):
                    logger.warning(f"必須項目が不足: {field}", store_id=store_data.get('store_id'))
                    return False
            
            # 日付形式のチェック
            if store_data.get('from_date'):
                try:
                    datetime.strptime(store_data['from_date'], '%Y-%m-%d')
                except ValueError:
                    logger.warning("開始日の形式が不正", from_date=store_data['from_date'])
                    return False
            
            if store_data.get('to_date'):
                try:
                    datetime.strptime(store_data['to_date'], '%Y-%m-%d')
                except ValueError:
                    logger.warning("終了日の形式が不正", to_date=store_data['to_date'])
                    return False
            
            # days_backのチェック
            if store_data.get('days_back'):
                try:
                    days_back = int(store_data['days_back'])
                    if days_back < 0 or days_back > 365:
                        logger.warning("遡り日数が範囲外", days_back=days_back)
                        return False
                except ValueError:
                    logger.warning("遡り日数の形式が不正", days_back=store_data['days_back'])
                    return False
            
            return True
            
        except Exception as e:
            logger.error("店舗データ検証エラー", error=str(e))
            return False
