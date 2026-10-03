"""
Quick CLI Lakehouse Inspector (DuckDB Powered)
Inspects the partitioned Parquet files and displays key operational metrics.
"""

import sys
from pathlib import Path
import duckdb

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.config import config

def inspect():
    db = duckdb.connect(":memory:")
    silver_glob = str(config.SILVER_PATH / "**" / "*.parquet").replace("\\", "/")
    
    print("\n" + "=" * 65)
    print(" 📊 REAL-TIME LAKEHOUSE INSPECTOR (SILVER LAYER)")
    print("=" * 65)

    try:
        # 1. High-level Summary
        summary_query = f"""
            SELECT 
                COUNT(*) AS total_transactions,
                ROUND(SUM(amount), 2) AS total_volume_usd,
                ROUND(AVG(amount), 2) AS avg_transaction_usd,
                COUNT(CASE WHEN is_high_risk_alert THEN 1 END) AS fraud_alerts_count,
                ROUND(100.0 * COUNT(CASE WHEN is_high_risk_alert THEN 1 END) / COUNT(*), 2) AS fraud_rate_pct
            FROM read_parquet('{silver_glob}', hive_partitioning=true)
        """
        summary_df = db.execute(summary_query).df()
        print("\n--- KPI METRICS ---")
        print(summary_df.to_string(index=False))

        # 2. Category Breakdown
        cat_query = f"""
            SELECT 
                merchant_category,
                COUNT(*) AS tx_count,
                ROUND(SUM(amount), 2) AS volume_usd,
                COUNT(CASE WHEN is_high_risk_alert THEN 1 END) AS flagged_alerts
            FROM read_parquet('{silver_glob}', hive_partitioning=true)
            GROUP BY merchant_category
            ORDER BY volume_usd DESC
            LIMIT 5
        """
        cat_df = db.execute(cat_query).df()
        print("\n--- TOP 5 SPENDING CATEGORIES ---")
        print(cat_df.to_string(index=False))

        # 3. Recent High-Risk Fraud Alerts
        alerts_query = f"""
            SELECT 
                event_timestamp,
                merchant_name,
                amount,
                city,
                country,
                risk_score,
                fraud_type
            FROM read_parquet('{silver_glob}', hive_partitioning=true)
            WHERE is_high_risk_alert = true
            ORDER BY event_timestamp DESC
            LIMIT 5
        """
        alerts_df = db.execute(alerts_query).df()
        print("\n--- RECENT DETECTED FRAUD ALERTS ---")
        print(alerts_df.to_string(index=False))

        print("\n" + "=" * 65)
        print(" [SUCCESS] Partitioned Parquet Lakehouse healthy & queryable.")
        print("=" * 65 + "\n")

    except Exception as e:
        print(f"[ERROR] Failed to query lakehouse: {e}")

if __name__ == "__main__":
    inspect()
