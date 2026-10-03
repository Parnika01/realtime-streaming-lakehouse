import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from src.config import config

class LakehouseStreamProcessor:
    """
    Production Lakehouse Streaming Processor (Medallion Architecture).
    Consumes streaming micro-batches, enriches with anomaly detection rules,
    and writes partitioned Parquet tables (Bronze -> Silver -> Gold).
    """
    def __init__(self):
        config.ensure_directories()
        self.db = duckdb.connect(":memory:")
        self.processed_offset = 0
        self.buffer_file = config.BRONZE_PATH / "streaming_buffer.jsonl"

    def read_new_events(self) -> List[Dict[str, Any]]:
        """Reads newly appended events from the streaming buffer."""
        if not self.buffer_file.exists():
            return []

        events = []
        with open(self.buffer_file, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= self.processed_offset:
                    line = line.strip()
                    if line:
                        try:
                            events.append(json.loads(line))
                        except Exception:
                            continue
        self.processed_offset += len(events)
        return events

    def transform_silver(self, raw_events: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Silver Transformation Layer:
        - Schema enforcement & casting
        - Event-time parsing
        - Real-time rule-based Anomaly & Risk Scoring engine
        """
        df = pd.DataFrame(raw_events)
        if df.empty:
            return df

        # Ensure datetime types
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

        # Feature Engineering: Streaming Risk Scoring (0 to 100)
        def calculate_risk(row) -> int:
            score = 10
            # Rule 1: High value spending
            if row["amount"] > 3000.0:
                score += 55
            elif row["amount"] > 1000.0:
                score += 30

            # Rule 2: Luxury or high-risk merchant category
            if row["merchant_category"] in ["luxury", "electronics"]:
                score += 20

            # Rule 3: Micro-transactions pattern (card testing)
            if row["amount"] < 2.0:
                score += 25

            # Ground truth indicator
            if row.get("is_fraud_flag", False):
                score = max(score, 85)

            return min(score, 100)

        df["risk_score"] = df.apply(calculate_risk, axis=1)
        df["is_high_risk_alert"] = df["risk_score"] >= 75
        df["processing_time"] = datetime.now(timezone.utc)
        
        # Partition columns
        df["year"] = df["event_timestamp"].dt.year
        df["month"] = df["event_timestamp"].dt.month
        df["day"] = df["event_timestamp"].dt.day
        df["hour"] = df["event_timestamp"].dt.hour

        return df

    def write_silver_lakehouse(self, df_silver: pd.DataFrame):
        """Writes enriched silver records to partitioned Parquet lakehouse storage."""
        if df_silver.empty:
            return

        table = pa.Table.from_pandas(df_silver)
        pq.write_to_dataset(
            table,
            root_path=str(config.SILVER_PATH),
            partition_cols=["year", "month", "day", "hour"],
            existing_data_behavior="overwrite_or_ignore"
        )
        print(f"[Silver Layer] Persisted {len(df_silver)} curated events to {config.SILVER_PATH}")

    def update_gold_aggregates(self):
        """
        Gold Transformation Layer:
        Computes 1-minute rolling window metrics using analytical SQL (DuckDB):
        - Total throughput and volume
        - Anomaly counts & risk rates
        - Merchant category breakdowns
        """
        try:
            # Query all silver parquet files directly using DuckDB
            silver_glob = str(config.SILVER_PATH / "**" / "*.parquet")
            
            gold_query = f"""
                SELECT
                    time_bucket(INTERVAL '1 minute', event_timestamp) AS window_start,
                    time_bucket(INTERVAL '1 minute', event_timestamp) + INTERVAL '1 minute' AS window_end,
                    COUNT(*) AS total_transactions,
                    ROUND(SUM(amount), 2) AS total_volume_usd,
                    ROUND(AVG(amount), 2) AS avg_transaction_usd,
                    COUNT(CASE WHEN is_high_risk_alert THEN 1 END) AS high_risk_alerts_count,
                    ROUND(100.0 * COUNT(CASE WHEN is_high_risk_alert THEN 1 END) / COUNT(*), 2) AS alert_rate_pct,
                    COUNT(DISTINCT user_id) AS active_users,
                    COUNT(DISTINCT merchant_name) AS active_merchants
                FROM read_parquet('{silver_glob}', hive_partitioning=true)
                GROUP BY window_start, window_end
                ORDER BY window_start DESC
            """
            gold_df = self.db.execute(gold_query).df()
            
            # Write Gold aggregated business mart
            gold_file = config.GOLD_PATH / "hourly_fraud_kpis.parquet"
            gold_df.to_parquet(gold_file, index=False)
            print(f"[Gold Layer] Aggregated {len(gold_df)} time windows into {gold_file.name}")
            return gold_df
        except Exception as e:
            # If no parquet files yet, skip gracefully
            return pd.DataFrame()

    def process_micro_batch(self) -> int:
        """Executes one end-to-end streaming micro-batch iteration."""
        raw_events = self.read_new_events()
        if not raw_events:
            return 0

        # Silver enrichment
        df_silver = self.transform_silver(raw_events)
        self.write_silver_lakehouse(df_silver)

        # Gold aggregation
        self.update_gold_aggregates()

        return len(raw_events)

    def start_streaming_job(self, poll_interval: float = 2.0):
        """Continuous micro-batch streaming loop."""
        print(f"[StreamProcessor] Starting Streaming Engine (poll interval: {poll_interval}s)...")
        try:
            while True:
                processed = self.process_micro_batch()
                if processed > 0:
                    print(f"[StreamProcessor] Successfully processed micro-batch of {processed} events.")
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            print("\n[StreamProcessor] Gracefully shutting down stream processing engine.")

if __name__ == "__main__":
    processor = LakehouseStreamProcessor()
    processor.start_streaming_job(poll_interval=2.0)
