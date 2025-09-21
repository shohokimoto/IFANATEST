"""
GCSサービス - Python版
"""
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger()


class GCSService:
    """Google Cloud Storage操作サービス"""
    
    def __init__(self):
        self.client = storage.Client(project=settings.project_id)
        self.bucket = self.client.bucket(settings.gcs_bucket)
    
    async def upload_csv_file(self, local_file_path: str, options: Dict[str, Any]) -> str:
        """CSVファイルをGCSにアップロード"""
        try:
            store_id = options.get('store_id')
            run_id = options.get('run_id')
            is_manual = options.get('is_manual', False)
            custom_path = options.get('custom_path')
            
            logger.info(
                "GCSアップロード開始",
                local_file_path=local_file_path,
                store_id=store_id,
                run_id=run_id,
                is_manual=is_manual
            )
            
            # オブジェクト名を生成
            object_name = self._generate_object_name(local_file_path, options)
            
            # ファイルをアップロード
            blob = self.bucket.blob(object_name)
            
            # メタデータを設定
            blob.metadata = {
                'store_id': store_id or 'unknown',
                'run_id': run_id or 'unknown',
                'upload_time': datetime.now().isoformat(),
                'is_manual': str(is_manual)
            }
            
            # ファイルをアップロード
            blob.upload_from_filename(local_file_path, content_type='text/csv')
            
            # アップロード完了を確認
            if not blob.exists():
                raise Exception("アップロード後のファイル存在確認に失敗")
            
            logger.info(
                "GCSアップロード完了",
                local_file_path=local_file_path,
                object_name=object_name,
                store_id=store_id,
                run_id=run_id
            )
            
            return object_name
            
        except Exception as e:
            logger.error("GCSアップロードに失敗", error=str(e), local_file_path=local_file_path)
            raise
    
    def _generate_object_name(self, local_file_path: str, options: Dict[str, Any]) -> str:
        """GCSオブジェクト名を生成"""
        store_id = options.get('store_id')
        run_id = options.get('run_id')
        is_manual = options.get('is_manual', False)
        custom_path = options.get('custom_path')
        
        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        
        # ファイル名を取得
        file_name = os.path.basename(local_file_path)
        
        if custom_path:
            return custom_path
        
        if is_manual:
            # 手動アップロード用のパス
            return f"{settings.manual_path_prefix}/{year}/{month}/{day}/{file_name}"
        else:
            # 自動処理用のパス
            if store_id and run_id:
                return f"{settings.gcs_path_prefix}/{year}/{month}/{day}/run_{run_id}/rb_{store_id}_{now.strftime('%Y%m%d')}.csv"
            else:
                return f"{settings.gcs_path_prefix}/{year}/{month}/{day}/run_{run_id}/{file_name}"
    
    async def upload_multiple_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """複数ファイルを一括アップロード"""
        results = []
        
        for file_info in files:
            try:
                object_name = await self.upload_csv_file(file_info['local_path'], file_info['options'])
                results.append({
                    'success': True,
                    'local_path': file_info['local_path'],
                    'object_name': object_name,
                    'store_id': file_info['options'].get('store_id')
                })
            except Exception as error:
                results.append({
                    'success': False,
                    'local_path': file_info['local_path'],
                    'error': str(error),
                    'store_id': file_info['options'].get('store_id')
                })
        
        success_count = sum(1 for r in results if r['success'])
        failure_count = sum(1 for r in results if not r['success'])
        
        logger.info(
            "一括アップロード完了",
            total=len(files),
            success=success_count,
            failure=failure_count
        )
        
        return results
    
    async def download_file(self, object_name: str, local_file_path: str):
        """GCSからファイルをダウンロード"""
        try:
            logger.info("GCSダウンロード開始", object_name=object_name, local_file_path=local_file_path)
            
            blob = self.bucket.blob(object_name)
            blob.download_to_filename(local_file_path)
            
            # ダウンロード完了を確認
            if not os.path.exists(local_file_path):
                raise Exception("ダウンロード後のファイル存在確認に失敗")
            
            logger.info("GCSダウンロード完了", object_name=object_name, local_file_path=local_file_path)
            
        except Exception as e:
            logger.error("GCSダウンロードに失敗", error=str(e), object_name=object_name, local_file_path=local_file_path)
            raise
    
    async def file_exists(self, object_name: str) -> bool:
        """GCSオブジェクトの存在確認"""
        try:
            blob = self.bucket.blob(object_name)
            return blob.exists()
        except Exception as e:
            logger.error("ファイル存在確認に失敗", error=str(e), object_name=object_name)
            return False
    
    async def get_file_metadata(self, object_name: str) -> Dict[str, Any]:
        """GCSオブジェクトのメタデータを取得"""
        try:
            blob = self.bucket.blob(object_name)
            blob.reload()
            
            metadata = {
                'name': blob.name,
                'size': blob.size,
                'created': blob.time_created,
                'updated': blob.updated,
                'content_type': blob.content_type,
                'metadata': blob.metadata or {}
            }
            
            return metadata
            
        except Exception as e:
            logger.error("ファイルメタデータ取得に失敗", error=str(e), object_name=object_name)
            raise
    
    async def list_files_by_date_range(self, prefix: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """指定期間のファイル一覧を取得"""
        try:
            logger.info("ファイル一覧取得開始", prefix=prefix, start_date=start_date, end_date=end_date)
            
            blobs = self.client.list_blobs(settings.gcs_bucket, prefix=prefix)
            
            filtered_files = []
            for blob in blobs:
                # ファイルの作成日時でフィルタリング
                if start_date <= blob.time_created <= end_date:
                    metadata = {
                        'name': blob.name,
                        'size': blob.size,
                        'created': blob.time_created,
                        'updated': blob.updated,
                        'metadata': blob.metadata or {}
                    }
                    filtered_files.append(metadata)
            
            logger.info(
                "ファイル一覧取得完了",
                prefix=prefix,
                filtered_files=len(filtered_files)
            )
            
            return filtered_files
            
        except Exception as e:
            logger.error("ファイル一覧取得に失敗", error=str(e), prefix=prefix)
            raise
    
    async def cleanup_old_files(self, prefix: str, days_to_keep: int = 30) -> int:
        """古いファイルを削除（TTL管理）"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            logger.info("古いファイル削除開始", prefix=prefix, days_to_keep=days_to_keep, cutoff_date=cutoff_date)
            
            blobs = self.client.list_blobs(settings.gcs_bucket, prefix=prefix)
            
            files_to_delete = []
            for blob in blobs:
                if blob.time_created < cutoff_date:
                    files_to_delete.append(blob)
            
            deleted_count = 0
            for blob in files_to_delete:
                try:
                    blob.delete()
                    deleted_count += 1
                    logger.debug("ファイル削除完了", file_name=blob.name)
                except Exception as e:
                    logger.error("ファイル削除に失敗", error=str(e), file_name=blob.name)
            
            logger.info(
                "古いファイル削除完了",
                prefix=prefix,
                deleted_files=deleted_count
            )
            
            return deleted_count
            
        except Exception as e:
            logger.error("古いファイル削除に失敗", error=str(e), prefix=prefix)
            raise
    
    async def get_bucket_usage(self) -> Dict[str, Any]:
        """バケットの容量使用量を取得"""
        try:
            blobs = self.client.list_blobs(settings.gcs_bucket)
            
            total_size = 0
            file_count = 0
            
            for blob in blobs:
                total_size += blob.size or 0
                file_count += 1
            
            usage = {
                'file_count': file_count,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / 1024 / 1024, 2),
                'total_size_gb': round(total_size / 1024 / 1024 / 1024, 2)
            }
            
            logger.info("バケット使用量取得完了", **usage)
            return usage
            
        except Exception as e:
            logger.error("バケット使用量取得に失敗", error=str(e))
            raise
    
    async def ensure_bucket_exists(self) -> bool:
        """バケットが存在するかチェック・作成"""
        try:
            try:
                self.bucket.reload()
                logger.info("GCSバケットは既に存在します", bucket_name=settings.gcs_bucket)
                return True
            except NotFound:
                # バケットを作成
                self.bucket.location = settings.region
                self.bucket.create()
                logger.info("GCSバケットを作成しました", bucket_name=settings.gcs_bucket)
                return True
                
        except Exception as e:
            logger.error("GCSバケット確認・作成に失敗", error=str(e), bucket_name=settings.gcs_bucket)
            return False
