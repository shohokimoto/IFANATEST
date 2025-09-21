"""
BigQueryサービス - Python版
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger()


class BigQueryService:
    """BigQuery操作サービス"""
    
    def __init__(self):
        self.client = bigquery.Client(project=settings.project_id)
        self.dataset_id = settings.bq_dataset
        self.stage_table_id = settings.stage_table_name
        self.main_table_id = settings.main_table_name
    
    async def load_data_from_gcs(self, gcs_object_name: str, run_id: str) -> Dict[str, Any]:
        """GCSからBigQueryステージテーブルにデータをロード"""
        try:
            logger.info("BigQueryロード開始", gcs_object_name=gcs_object_name, run_id=run_id)
            
            # GCS URIを構築
            gcs_uri = f"gs://{settings.gcs_bucket}/{gcs_object_name}"
            
            # データセットとテーブルの参照を取得
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(self.stage_table_id)
            
            # ジョブ設定
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                skip_leading_rows=1,
                autodetect=False,
                schema=self._get_stage_table_schema(),
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="ingestion_ts"
                )
            )
            
            # ロードジョブを実行
            load_job = self.client.load_table_from_uri(
                gcs_uri, table_ref, job_config=job_config
            )
            
            logger.info("BigQueryロードジョブ開始", job_id=load_job.job_id, gcs_uri=gcs_uri)
            
            # ジョブ完了を待機
            load_job.result()  # 同期で待機
            
            if load_job.state == 'DONE':
                if load_job.errors:
                    raise Exception(f"BigQueryロードジョブ失敗: {load_job.errors}")
                
                output_rows = load_job.output_rows
                logger.info(
                    "BigQueryロード完了",
                    job_id=load_job.job_id,
                    output_rows=output_rows,
                    run_id=run_id
                )
                
                return {
                    'job_id': load_job.job_id,
                    'output_rows': output_rows,
                    'success': True
                }
            else:
                raise Exception(f"BigQueryロードジョブが完了していません: {load_job.state}")
                
        except Exception as e:
            logger.error("BigQueryロードに失敗", error=str(e), gcs_object_name=gcs_object_name, run_id=run_id)
            raise
    
    async def merge_stage_to_main(self, run_id: str) -> Dict[str, Any]:
        """ステージテーブルから本番テーブルにMERGE実行"""
        try:
            logger.info("BigQuery MERGE開始", run_id=run_id)
            
            # ストアドプロシージャを実行するクエリ
            query = f"""
                CALL `{settings.project_id}.{self.dataset_id}.merge_reservations_rb`('{run_id}')
            """
            
            # クエリジョブを実行
            query_job = self.client.query(query)
            logger.info("BigQuery MERGEジョブ開始", job_id=query_job.job_id, run_id=run_id)
            
            # 結果を取得
            results = list(query_job.result())
            
            # 結果を解析
            result = self._parse_merge_result(results)
            
            logger.info(
                "BigQuery MERGE完了",
                job_id=query_job.job_id,
                run_id=run_id,
                result=result
            )
            
            return {
                'job_id': query_job.job_id,
                'run_id': run_id,
                'result': result,
                'success': True
            }
            
        except Exception as e:
            logger.error("BigQuery MERGEに失敗", error=str(e), run_id=run_id)
            raise
    
    def _get_stage_table_schema(self) -> List[bigquery.SchemaField]:
        """ステージテーブルのスキーマ定義"""
        return [
            bigquery.SchemaField("store_id", "STRING", mode="REQUIRED", description="店舗ID"),
            bigquery.SchemaField("store_name", "STRING", mode="NULLABLE", description="店舗名"),
            bigquery.SchemaField("reserve_date", "DATE", mode="REQUIRED", description="予約日"),
            bigquery.SchemaField("booking_date", "DATE", mode="NULLABLE", description="予約受付日"),
            bigquery.SchemaField("start_time", "TIME", mode="NULLABLE", description="予約開始時間"),
            bigquery.SchemaField("end_time", "TIME", mode="NULLABLE", description="予約終了時間"),
            bigquery.SchemaField("course_name", "STRING", mode="NULLABLE", description="コース名"),
            bigquery.SchemaField("headcount", "INTEGER", mode="NULLABLE", description="人数"),
            bigquery.SchemaField("channel", "STRING", mode="NULLABLE", description="経路"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE", description="予約ステータス"),
            bigquery.SchemaField("vendor", "STRING", mode="REQUIRED", description="ベンダー"),
            bigquery.SchemaField("ingestion_ts", "TIMESTAMP", mode="REQUIRED", description="取込時刻"),
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED", description="実行ID"),
            bigquery.SchemaField("record_key", "STRING", mode="REQUIRED", description="レコードキー"),
            bigquery.SchemaField("record_hash", "STRING", mode="REQUIRED", description="内容ハッシュ"),
        ]
    
    def _get_main_table_schema(self) -> List[bigquery.SchemaField]:
        """本番テーブルのスキーマ定義"""
        stage_schema = self._get_stage_table_schema()
        main_schema = stage_schema.copy()
        
        # 追加フィールド
        main_schema.extend([
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED", description="作成日時"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED", description="更新日時"),
        ])
        
        return main_schema
    
    def _parse_merge_result(self, results: List[Any]) -> Dict[str, int]:
        """MERGE結果を解析"""
        # ストアドプロシージャの結果を解析
        inserted = 0
        updated = 0
        unchanged = 0
        
        for row in results:
            # 結果の形式に応じて解析
            if hasattr(row, 'inserted'):
                inserted = row.inserted
            if hasattr(row, 'updated'):
                updated = row.updated
            if hasattr(row, 'unchanged'):
                unchanged = row.unchanged
        
        return {
            'inserted': inserted,
            'updated': updated,
            'unchanged': unchanged,
            'total': inserted + updated + unchanged
        }
    
    async def ensure_dataset_exists(self) -> bool:
        """データセットが存在するかチェック・作成"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            
            try:
                self.client.get_dataset(dataset_ref)
                logger.info("データセットは既に存在します", dataset_id=self.dataset_id)
                return True
            except NotFound:
                # データセットを作成
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = settings.region
                dataset.description = "レストランボード予約データETL用データセット"
                
                dataset = self.client.create_dataset(dataset, timeout=30)
                logger.info("データセットを作成しました", dataset_id=self.dataset_id)
                return True
                
        except Exception as e:
            logger.error("データセット確認・作成に失敗", error=str(e), dataset_id=self.dataset_id)
            return False
    
    async def ensure_tables_exist(self) -> bool:
        """テーブルが存在するかチェック・作成"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            
            # ステージテーブルの確認・作成
            stage_table_ref = dataset_ref.table(self.stage_table_id)
            try:
                self.client.get_table(stage_table_ref)
                logger.info("ステージテーブルは既に存在します", table_id=self.stage_table_id)
            except NotFound:
                stage_table = bigquery.Table(stage_table_ref, schema=self._get_stage_table_schema())
                stage_table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="ingestion_ts"
                )
                stage_table.description = "レストランボード予約データのステージテーブル（TTL: 30日）"
                
                self.client.create_table(stage_table)
                logger.info("ステージテーブルを作成しました", table_id=self.stage_table_id)
            
            # 本番テーブルの確認・作成
            main_table_ref = dataset_ref.table(self.main_table_id)
            try:
                self.client.get_table(main_table_ref)
                logger.info("本番テーブルは既に存在します", table_id=self.main_table_id)
            except NotFound:
                main_table = bigquery.Table(main_table_ref, schema=self._get_main_table_schema())
                main_table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="reserve_date"
                )
                main_table.clustering_fields = ["store_id", "channel"]
                main_table.description = "レストランボード予約データの本番テーブル"
                
                self.client.create_table(main_table)
                logger.info("本番テーブルを作成しました", table_id=self.main_table_id)
            
            return True
            
        except Exception as e:
            logger.error("テーブル確認・作成に失敗", error=str(e))
            return False
    
    async def get_table_row_count(self, table_id: str) -> int:
        """テーブルの行数を取得"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(table_id)
            query = f"""
                SELECT COUNT(*) as row_count
                FROM `{settings.project_id}.{self.dataset_id}.{table_id}`
            """
            
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            row_count = results[0].row_count if results else 0
            logger.info("テーブル行数取得完了", table_id=table_id, row_count=row_count)
            
            return row_count
            
        except Exception as e:
            logger.error("テーブル行数取得に失敗", error=str(e), table_id=table_id)
            raise
    
    async def query_reservations(self, from_date: str, to_date: str, store_ids: Optional[List[str]] = None, limit: int = 1000) -> List[Dict[str, Any]]:
        """予約データをクエリ"""
        try:
            logger.info("予約データクエリ開始", from_date=from_date, to_date=to_date, store_ids=store_ids)
            
            query = f"""
                SELECT store_id, store_name, reserve_date, booking_date,
                       start_time, end_time, course_name, headcount, channel, status
                FROM `{settings.project_id}.{self.dataset_id}.{self.main_table_id}`
                WHERE reserve_date BETWEEN '{from_date}' AND '{to_date}'
            """
            
            if store_ids:
                store_id_list = "', '".join(store_ids)
                query += f" AND store_id IN ('{store_id_list}')"
            
            query += f" ORDER BY reserve_date, store_id LIMIT {limit}"
            
            query_job = self.client.query(query)
            results = list(query_job.result())
            
            # 結果を辞書リストに変換
            data = []
            for row in results:
                data.append(dict(row))
            
            logger.info("予約データクエリ完了", from_date=from_date, to_date=to_date, result_count=len(data))
            
            return data
            
        except Exception as e:
            logger.error("予約データクエリに失敗", error=str(e), from_date=from_date, to_date=to_date)
            raise
