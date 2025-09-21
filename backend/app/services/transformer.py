"""
データ変換サービス - Python版
"""
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, date, time
import hashlib
import json
import os
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger()


class DataTransformer:
    """データ変換・正規化サービス"""
    
    def __init__(self):
        # レストランボードの列名マッピング
        self.column_mapping = {
            # 店舗情報
            '店舗ID': 'store_id',
            '店舗名': 'store_name',
            '店舗': 'store_name',
            
            # 予約日時
            '予約日': 'reserve_date',
            '来店日': 'reserve_date',
            '日付': 'reserve_date',
            '予約受付日': 'booking_date',
            '受付日': 'booking_date',
            '登録日': 'booking_date',
            '予約時間': 'start_time',
            '開始時間': 'start_time',
            '時間': 'start_time',
            '終了時間': 'end_time',
            
            # 予約内容
            'コース名': 'course_name',
            'プラン名': 'course_name',
            'メニュー名': 'course_name',
            'コース': 'course_name',
            '人数': 'headcount',
            '名数': 'headcount',
            '予約者数': 'headcount',
            
            # 経路・ステータス
            '経路': 'channel',
            '媒体': 'channel',
            '流入元': 'channel',
            '予約ステータス': 'status',
            'ステータス': 'status',
            '状態': 'status',
            
            # その他
            '予約番号': 'reservation_id',
            'ID': 'reservation_id',
            '備考': 'note',
            'メモ': 'note'
        }
    
    async def transform_csv(self, csv_file_path: str, store: Dict[str, Any], run_id: str) -> str:
        """CSVファイルを読み込んで正規化"""
        try:
            logger.info(
                "CSV変換開始",
                store_id=store['store_id'],
                csv_file_path=csv_file_path,
                run_id=run_id
            )
            
            # CSVファイルを読み込み
            raw_data = await self._read_csv_file(csv_file_path)
            
            # データを正規化
            normalized_data = await self._normalize_data(raw_data, store, run_id)
            
            # 正規化されたCSVファイルを書き込み
            output_path = await self._write_normalized_csv(normalized_data, store['store_id'], run_id)
            
            logger.info(
                "CSV変換完了",
                store_id=store['store_id'],
                input_rows=len(raw_data),
                output_rows=len(normalized_data),
                output_path=output_path
            )
            
            return output_path
            
        except Exception as e:
            logger.error("CSV変換に失敗", error=str(e), store_id=store['store_id'])
            raise
    
    async def _read_csv_file(self, file_path: str) -> pd.DataFrame:
        """CSVファイルを読み込み"""
        try:
            # 文字エンコーディングを自動検出
            encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info("CSVファイル読み込み成功", file_path=file_path, encoding=encoding)
                    return df
                except UnicodeDecodeError:
                    continue
            
            # すべてのエンコーディングで失敗した場合
            raise Exception("CSVファイルのエンコーディングを検出できませんでした")
            
        except Exception as e:
            logger.error("CSVファイル読み込みに失敗", error=str(e), file_path=file_path)
            raise
    
    async def _normalize_data(self, raw_data: pd.DataFrame, store: Dict[str, Any], run_id: str) -> List[Dict[str, Any]]:
        """データを正規化"""
        normalized_data = []
        ingestion_ts = datetime.now()
        
        for index, row in raw_data.iterrows():
            try:
                normalized = self._normalize_row(row.to_dict(), store, run_id, ingestion_ts)
                if normalized:
                    normalized_data.append(normalized)
            except Exception as e:
                logger.warning(
                    "行の正規化に失敗（スキップ）",
                    store_id=store['store_id'],
                    row_index=index,
                    error=str(e)
                )
                continue
        
        return normalized_data
    
    def _normalize_row(self, row: Dict[str, Any], store: Dict[str, Any], run_id: str, ingestion_ts: datetime) -> Optional[Dict[str, Any]]:
        """単一行を正規化"""
        # 列名をマッピング
        mapped_row = {}
        for original_key, value in row.items():
            normalized_key = self.column_mapping.get(original_key, original_key.lower())
            mapped_row[normalized_key] = value
        
        # 必須項目の検証
        if not mapped_row.get('reserve_date'):
            logger.warning("予約日が未設定（スキップ）", store_id=store['store_id'])
            return None
        
        # データ型変換・正規化
        normalized = {
            # 店舗情報
            'store_id': store['store_id'],
            'store_name': store.get('store_name') or self._normalize_string(mapped_row.get('store_name')),
            
            # 予約日（必須）
            'reserve_date': self._normalize_date(mapped_row.get('reserve_date')),
            
            # 予約受付日（オプション）
            'booking_date': self._normalize_date(mapped_row.get('booking_date')),
            
            # 時間（オプション）
            'start_time': self._normalize_time(mapped_row.get('start_time')),
            'end_time': self._normalize_time(mapped_row.get('end_time')),
            
            # 予約内容（オプション）
            'course_name': self._normalize_string(mapped_row.get('course_name')),
            'headcount': self._normalize_integer(mapped_row.get('headcount')),
            
            # 経路・ステータス（オプション）
            'channel': self._normalize_string(mapped_row.get('channel')),
            'status': self._normalize_string(mapped_row.get('status')),
            
            # 固定値・メタデータ
            'vendor': 'restaurant_board',
            'ingestion_ts': ingestion_ts.isoformat(),
            'run_id': run_id,
            
            # レコードキーとハッシュを生成
            'record_key': self._generate_record_key(mapped_row, store['store_id']),
            'record_hash': None  # 後で計算
        }
        
        # 内容ハッシュを計算
        normalized['record_hash'] = self._calculate_record_hash(normalized)
        
        return normalized
    
    def _normalize_date(self, date_str: Any) -> Optional[str]:
        """日付を正規化"""
        if pd.isna(date_str) or not date_str:
            return None
        
        date_str = str(date_str).strip()
        if not date_str:
            return None
        
        # 様々な日付形式をパース
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y年%m月%d日',
            '%m月%d日',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S'
        ]
        
        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, date_format)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # パースできない場合はそのまま返す（エラーログ付き）
        logger.warning("日付の正規化に失敗", date_str=date_str)
        return date_str
    
    def _normalize_time(self, time_str: Any) -> Optional[str]:
        """時間を正規化"""
        if pd.isna(time_str) or not time_str:
            return None
        
        time_str = str(time_str).strip()
        if not time_str:
            return None
        
        # 様々な時間形式をパース
        time_formats = [
            '%H:%M:%S',
            '%H:%M',
            '%H時%M分',
            '%H:%M:%S.%f',
            '%H:%M:%S.%fZ'
        ]
        
        for time_format in time_formats:
            try:
                parsed_time = datetime.strptime(time_str, time_format)
                return parsed_time.strftime('%H:%M:%S')
            except ValueError:
                continue
        
        # パースできない場合はそのまま返す
        logger.warning("時間の正規化に失敗", time_str=time_str)
        return time_str
    
    def _normalize_string(self, value: Any) -> Optional[str]:
        """文字列を正規化"""
        if pd.isna(value) or not value:
            return None
        
        normalized = str(value).strip()
        return normalized if normalized else None
    
    def _normalize_integer(self, value: Any) -> Optional[int]:
        """整数を正規化"""
        if pd.isna(value) or not value:
            return None
        
        try:
            # 文字列から数字以外を除去
            cleaned = ''.join(filter(str.isdigit, str(value)))
            if cleaned:
                return int(cleaned)
            return None
        except (ValueError, TypeError):
            return None
    
    def _generate_record_key(self, row: Dict[str, Any], store_id: str) -> str:
        """レコードキーを生成"""
        # 予約番号があればそれを使用
        if row.get('reservation_id'):
            return f"{store_id}|{row['reservation_id']}"
        
        # 合成キーを生成
        components = [
            store_id,
            self._normalize_date(row.get('reserve_date')),
            self._normalize_time(row.get('start_time')),
            self._normalize_string(row.get('course_name')),
            str(self._normalize_integer(row.get('headcount')) or ''),
            self._normalize_string(row.get('channel'))
        ]
        
        return '|'.join([c for c in components if c is not None])
    
    def _calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """レコードハッシュを計算"""
        # ハッシュ計算から除外するフィールド
        exclude_fields = {'ingestion_ts', 'run_id', 'record_hash'}
        
        hash_data = {k: v for k, v in record.items() if k not in exclude_fields}
        
        # ソートしてJSON文字列に変換
        hash_string = json.dumps(hash_data, sort_keys=True, ensure_ascii=False)
        
        # MD5ハッシュを計算
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
    
    async def _write_normalized_csv(self, data: List[Dict[str, Any]], store_id: str, run_id: str) -> str:
        """正規化されたCSVファイルを書き込み"""
        output_path = f"/tmp/normalized_rb_{store_id}_{run_id}.csv"
        
        if not data:
            # 空のCSVファイルを作成
            df = pd.DataFrame()
            df.to_csv(output_path, index=False)
            return output_path
        
        # DataFrameに変換
        df = pd.DataFrame(data)
        
        # 列の順序を指定
        columns = [
            'store_id', 'store_name', 'reserve_date', 'booking_date',
            'start_time', 'end_time', 'course_name', 'headcount',
            'channel', 'status', 'vendor', 'ingestion_ts',
            'run_id', 'record_key', 'record_hash'
        ]
        
        # 指定された列のみを選択（存在しない列はNaNで埋める）
        df = df.reindex(columns=columns, fill_value='')
        
        # CSVファイルに書き込み
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        return output_path
    
    async def merge_csv_files(self, file_paths: List[str], run_id: str) -> str:
        """複数店舗のデータを統合"""
        try:
            logger.info("CSV統合開始", file_count=len(file_paths), run_id=run_id)
            
            all_data = []
            
            for file_path in file_paths:
                data = await self._read_csv_file(file_path)
                all_data.append(data)
            
            # すべてのデータを結合
            if all_data:
                merged_df = pd.concat(all_data, ignore_index=True)
            else:
                merged_df = pd.DataFrame()
            
            merged_path = f"/tmp/merged_rb_{run_id}.csv"
            merged_df.to_csv(merged_path, index=False, encoding='utf-8')
            
            logger.info(
                "CSV統合完了",
                total_rows=len(merged_df),
                merged_path=merged_path
            )
            
            return merged_path
            
        except Exception as e:
            logger.error("CSV統合に失敗", error=str(e))
            raise
