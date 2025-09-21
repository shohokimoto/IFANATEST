# 予約データETL／ダッシュボード（レストランボード先行）要件定義書 v1
最終更新: 2025-09-21 (JST)
作成: ChatGPT（ユーザー要件ヒアリング反映）

---

## 0. 本ドキュメントの目的
- レストランボード（以下RB）を対象に、**Cloud Runでの定期スクレイピング → GCS → BigQuery（ステージ→MERGE） → Connected Sheets表示**までの**MVP要件**を定義する。
- 将来、Ebica／TORETA／TableCheck／（名称確認中: レスザイコ／デウs）へ水平展開し、FastAPI+Reactのアプリ表示へ拡張する前提の**段階的スコープ**を明確化する。

---

## 1. スコープ
### 1.1 In Scope（MVP / フェーズ1: RBのみ）
- Cloud Run にデプロイする**Dockerコンテナ**での定期実行（Cloud Schedulerで1日1回）
- Google スプレッドシート（平置き）からの**店舗マスタ**（ID/ユーザー名/パスワード等）の取得
- Puppeteer による RB 管理画面の**ログイン・期間指定・CSVダウンロード**
- 文字コード/列名/日付・時間/数値の**正規化** → **共通カラム**に整形
- 整形CSVを **GCS** へ配置
- **BigQuery ステージ表**へのロード（Load Job）
- **ステージ→本番（MERGE）** による差分反映（重複排除／遅延更新反映）
- **Connected Sheets** による期間指定表示（まずは明細表示）
- 手動テスト用の**CSV手動アップロード→Load→MERGE**運用

### 1.2 Out of Scope（MVP外／フェーズ2以降）
- Secret Manager への資格情報移行（MVPはシート平置き）
- FastAPI + React でのアプリ表示（MVPは Connected Sheets）
- 各ベンダーの公式API連携（入手可能な場合の将来検討）
- 2FA/画像認証の自動突破（対象店舗はMVPでは除外/手動）

---

## 2. 用語
- **RB**: レストランボード（Restaurant Board）
- **ETL**: Extract/Transform/Load
- **GCS**: Google Cloud Storage
- **ステージ表**: BigQueryで取り込み直後に置く一時的な表（差分判定の入力）
- **MERGE**: ステージ→本番への差分反映（UPSERT）
- **Connected Sheets**: スプレッドシートからBigQueryに接続してデータを直接参照する機能

---

## 3. 役割／ユーザー
- **運用者（内部）**: スケジュール実行、失敗対応、手動バックフィル、シートの店舗追加・更新
- **閲覧者（内部）**: Connected Sheets上のデータ確認（期間指定など）
- **将来（アプリ）**: master/sales ロールでのアプリログイン閲覧（フェーズ2）

---

## 4. 全体アーキテクチャ（MVP）
```
[Google Sheets: Stores]  →  [Cloud Run: Docker/Node20 + Puppeteer]
       │ 読取API                          │ CSV(共通カラム)
       └───────┬──────────────────────┘
               ▼
         [GCS: landing/tmp]  →  [BigQuery: stage_reservations_rb]  →  MERGE  →  [reservations_rb]
                                                                                │
                                                                                └→ [Connected Sheets]
```
- リージョンは原則 **asia-northeast1** で統一。タイムゾーンは **JST**。

---

## 5. 機能要件（FR）
### FR-1. 店舗マスタ（スプレッドシート）
- シート名 `Stores`、ヘッダー例:
  - `active, store_id, store_name, rb_username, rb_password, days_back, from_date, to_date, note`
- `active=true` の行のみ処理対象。
- `days_back`: 前日+N日を再取得（遅延キャンセル反映。初期値7）。
- `from_date/to_date`: 指定があれば期間優先でバックフィル。
- 共有は最小限。監査ログ配慮。MVPは平置きを許容。

### FR-2. スクレイピング（Puppeteer）
- RB ログイン → 期間設定 → CSVダウンロード。
- ダウンロード先は `/tmp`。Shift_JIS→UTF-8へ変換。
- 失敗時は最大3回リトライ（指数バックオフ）。
- 2FA/画像認証がある店舗は `active=false` で除外（MVP）。

### FR-3. 整形（Transform）
- 共通カラムへ正規化（#7 参照）。
- 欠損/異常値はスキップまたはNULL化。日付/時間は正規フォーマットへ。
- 1店舗ごとCSV生成、さらに全店舗分を1ファイルに集約可能。

### FR-4. GCS への配置
- オブジェクト命名規則（例）:
  - `landing/restaurant_board/YYYY/MM/DD/run_<run_id>/rb_<store_id>_<YYYYMMDD>.csv`
  - 手動アップロード用: `manual/restaurant_board/YYYY/MM/...`

### FR-5. BigQuery への取り込み（Load Job）
- 取り込み先は **ステージ表** `stage_reservations_rb`（日付は `ingestion_ts` パーティション）。
- スキーマは共通カラム + メタ（#7）。

### FR-6. ステージ→MERGE（差分反映）
- 一意キー `record_key` と内容ハッシュ `record_hash` を用いて、本番 `reservations_rb` に**UPSERT**。
- ステージ内重複は `ROW_NUMBER()` で排除（最新 `ingestion_ts` を優先）。
- 再取得窓は **前日 + 過去7日** を推奨。

### FR-7. 表示（Connected Sheets）
- `reservations_rb` を**期間指定**で参照（必要列に限定するカスタムSQL）。
- ヘッダーはシート上で日本語に差し替え可（物理名は英語）。

### FR-8. 手動テスト/バックフィル
- 手動で共通カラムCSVをGCSにアップ → Load Job → MERGE 実行で検証できる。

---

## 6. 非機能要件（NFR）
- **性能/スケール**: 100〜500店舗、1店舗/日 あたり数百〜数千行規模を想定。日次処理は1時間以内完了。
- **可用性**: 失敗時の再実行（リトライ/手動再実行）で翌朝6時までにデータ反映を目標。
- **セキュリティ**: MVPは平置きだが、パスワードはログ出力禁止/共有最小。将来Secret Manager移行。
- **可観測性**: 実行件数/失敗率/処理時間/ロード行数をログ・ダッシュボードで確認可。
- **コスト**: BQは `reserve_date` パーティション + `store_id, channel` クラスタ。`SELECT *`禁止、日付絞り必須。ステージ表はTTL 14〜30日。

---

## 7. 共通カラム定義（論理名/物理名/型）
| 表示名（日本語） | 物理名（BigQuery） | 型 | 必須 | 説明 |
|---|---|---|---|---|
| 店舗_ID | store_id | STRING | ◯ | AIR店舗ID 等。シート側の `store_id` を優先 |
| 店舗名 | store_name | STRING | △ | 表示用名称 |
| 予約日 | reserve_date | DATE | ◯ | 来店日（DATE） |
| 予約受付日 | booking_date | DATE | △ | 受付日/登録日 |
| 予約開始時間 | start_time | TIME | △ | HH:MM:SS |
| 予約終了時間 | end_time | TIME | △ | HH:MM:SS |
| コース名 | course_name | STRING | △ | プラン名/メニュー名 |
| 人数 | headcount | INT64 | △ | 名数 |
| 経路 | channel | STRING | △ | 媒体/流入元（例: ホットペッパー/食べログ 等） |
| 予約ステータス | status | STRING | △ | 例: 確定/キャンセル 等 |
| ベンダー | vendor | STRING | ◯ | 固定値 'restaurant_board' |
| 取込時刻 | ingestion_ts | TIMESTAMP | ◯ | 取込実行のタイムスタンプ |
| 実行ID | run_id | STRING | ◯ | バッチ実行の識別子 |
| レコードキー | record_key | STRING | ◯ | **予約の恒久キー**（予約番号 or 合成キー） |
| 内容ハッシュ | record_hash | STRING | ◯ | **内容のMD5** 等（変更検知） |

> **レコードキー設計**: 予約番号があればそれを使用。無い場合は `store_id|reserve_date|start_time|course_name|headcount|channel` で合成。

---

## 8. データモデル（物理）
- **BigQuery データセット**: `rb`
- **本番表**: `rb.reservations_rb`
  - `PARTITION BY reserve_date`
  - `CLUSTER BY store_id, channel`
- **ステージ表**: `rb.stage_reservations_rb`
  - `PARTITION BY DATE(ingestion_ts)`
  - TTL 14〜30日（推奨）

---

## 9. バッチ/スケジュール
- 実行時刻: 毎日 **01:15 JST**（Cloud Scheduler）
- 処理窓: 前日 + 過去7日（可変）
- 冪等性: 同一 `run_id` の再実行で重複が生じないこと（MERGE/キー設計で担保）

---

## 10. 重複/差分反映ルール
- ステージ内は `ROW_NUMBER() OVER(PARTITION BY record_key ORDER BY ingestion_ts DESC)` で最新1行に正規化。
- MERGE 条件: `vendor, store_id, record_key` が一致
  - **一致 & ハッシュ相違** → UPDATE（内容更新）
  - **未一致** → INSERT（新規）
  - **一致 & ハッシュ同一** → 何もしない

---

## 11. エラーハンドリング
- ログイン失敗/ダウンロード失敗/CSV不整合/Load失敗は**店舗単位で継続**し、最後にサマリ通知（将来）。
- 致命的失敗時は **非ゼロ終了**。運用者が再実行可能。

---

## 12. セキュリティ/権限
- Cloud Run 実行SAには最小権限（BQ: jobUser/dataEditor、GCS: objectAdmin、Sheets: readonly）。
- パスワードはログ出力禁止。シート共有は必要最小限、リンク共有OFF。

---

## 13. Connected Sheets（表示要件）
- データコネクタで `rb.reservations_rb` へ接続。
- **カスタムSQL**で日付絞り + 必要列のみ参照（スキャン最小化）。
- 初期ビューは明細表示。将来 `fact_daily_metrics` の前計算テーブルを追加しKPIカード/グラフを高速表示。

---

## 14. テスト計画（抜粋）
- **T-1 文字コード**: Shift_JIS→UTF-8で文字化けしない（仮名/機種依存文字含む）
- **T-2 正規化**: 日付（DATE）/時間（TIME）/人数（INT64）へ正しく変換
- **T-3 差分**: 同一予約の更新で UPDATE になる（ハッシュ相違時）
- **T-4 重複**: 同一CSV再投入で重複行が増えない
- **T-5 期間**: Connected Sheetsで指定期間のみ表示される
- **T-6 規模**: 30日×100店舗×1,000行/日 程度で処理完了時間<1h

---

## 15. 受け入れ基準（サンプル）
- D+1の朝6時（JST）までに前日分+過去7日の変動が反映されている
- 手動アップロード→Load→MERGEで月次バックフィルが成功
- Connected Sheets で任意期間の明細が2秒以内に表示（1万行程度）
- 同一runの再実行で本番表の行数が不正に増加しない

---

## 16. デプロイ/運用要件
- **Docker完結**でローカル→本番までビルド/実行可能。
- Cloud Run/Scheduler の設定値（環境変数）:
  - `PROJECT_ID, BQ_DATASET, GCS_BUCKET, STORES_SHEET_ID, DAYS_BACK, FROM_DATE, TO_DATE, REGION`
- ログはCloud Loggingに集約。将来アラート連携（メール/Slack等）。

---

## 17. コスト要件
- BigQueryは**オンデマンド課金**を想定。
- パーティション/クラスタの活用、`SELECT *`禁止、日付絞り必須、事前集計の導入で**月額を最小化**。
- ステージ表はTTLで自動削除。

---

## 18. リスクと対応
- **DOM変更/規約変更**: 選択子変更監視/迅速改修（SLA: 48h 以内）
- **2FA発生**: 対象店舗はMVP対象外→平時は `active=false` 運用
- **CSV仕様差異**: ベンダー別スキーマYAMLで吸収（RBは現物優先で更新）
- **平置きパスワード**: 移行計画（Secret Manager）を別紙で管理

---

## 19. フェーズ計画
- **フェーズ1（本書）**: RBのみ + Connected Sheets 表示
- **フェーズ2**: ベンダー追加（Ebica/TORETA/TableCheck/ほか）、資格情報をSecret Managerへ、`fact_daily_metrics`導入
- **フェーズ3**: FastAPI/Reactアプリ（master/sales ロール、KPIダッシュボード、CSVエクスポート）

---

## 20. 参考: BQオブジェクト（要求仕様）
- データセット: `rb`
- テーブル: `rb.stage_reservations_rb`, `rb.reservations_rb`
- ストアドプロシージャ: `rb.merge_reservations_rb(run STRING)`
- （任意）ビュー/マテビュー: `rb.vw_daily_kpi`, `rb.fact_daily_metrics`

---

## 21. 参考: Connected Sheets カスタムSQL（要件）
- 期間指定で必要列のみ。
```
SELECT store_id, store_name, reserve_date, booking_date,
       start_time, end_time, course_name, headcount, channel, status
FROM `rb.reservations_rb`
WHERE reserve_date BETWEEN @from AND @to
ORDER BY reserve_date, store_id;
```

---

## 22. オープン課題（要確定）
- RB画面の**ログインURL/セレクタ**の最終確定
- **予約番号の有無**（`record_key` を自然キーにできるか）
- **ステータス値の体系**（"キャンセル" 判定の一貫性）
- **チャンネル（経路）コードの標準化**（マスタ化の要否）
- Connected Sheets の**更新頻度/責任者**

---

*本要件定義書はMVPの合意ベース。変更点は版管理（v1→）で追記・改訂する。*

