"""
Pipeline Performance & Benchmark Suite
Benchmarks throughput, serialization latency, compression ratio, and DuckDB analytical query speeds.
"""

import time
import json
import sys
from pathlib import Path
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.producer.transaction_generator import generate_transaction
from src.streaming.stream_processor import LakehouseStreamProcessor

def run_benchmarks(sample_size: int = 5000):
    print("\n" + "=" * 65)
    print(f" 🚀 EXECUTING PIPELINE BENCHMARK SUITE ({sample_size:,} EVENTS)")
    print("=" * 65)

    processor = LakehouseStreamProcessor()

    # 1. Event Generation & Schema Validation Throughput
    t0 = time.perf_counter()
    raw_events = [generate_transaction() for _ in range(sample_size)]
    t_gen = time.perf_counter() - t0
    gen_throughput = sample_size / t_gen
    print(f"[*] Ingestion Generation: {gen_throughput:,.0f} events/sec ({t_gen*1000/sample_size:.3f} ms/event)")

    # 2. Silver Transformation & Anomaly Scoring Speed
    t0 = time.perf_counter()
    df_silver = processor.transform_silver(raw_events)
    t_transform = time.perf_counter() - t0
    trans_throughput = sample_size / t_transform
    print(f"[*] Silver Transformation & Risk Scoring: {trans_throughput:,.0f} events/sec ({t_transform*1000/sample_size:.3f} ms/event)")

    # 3. Compression Benchmark: Raw JSON vs Parquet
    benchmark_dir = Path("lakehouse_storage") / "benchmarks"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    
    json_path = benchmark_dir / "bench_raw.json"
    parquet_path = benchmark_dir / "bench_curated.parquet"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(raw_events, f)
    raw_json_bytes = json_path.stat().st_size

    table = pa.Table.from_pandas(df_silver)
    pq.write_table(table, parquet_path, compression="snappy")
    parquet_bytes = parquet_path.stat().st_size

    compression_savings = (1 - (parquet_bytes / raw_json_bytes)) * 100
    print(f"[*] Storage Compression: Raw JSON = {raw_json_bytes/1024:.1f} KB | Parquet (Snappy) = {parquet_bytes/1024:.1f} KB")
    print(f"    -> Storage Footprint Reduction: {compression_savings:.1f}% savings")

    # 4. DuckDB Analytical Query Latency (Zero-Copy Columnar Scan)
    db = duckdb.connect(":memory:")
    parquet_glob = str(parquet_path).replace("\\", "/")
    
    query = f"""
        SELECT 
            merchant_category,
            COUNT(*) as total_tx,
            ROUND(SUM(amount), 2) as volume,
            ROUND(AVG(risk_score), 2) as avg_risk,
            COUNT(CASE WHEN is_high_risk_alert THEN 1 END) as fraud_count
        FROM read_parquet('{parquet_glob}')
        GROUP BY merchant_category
        ORDER BY volume DESC
    """
    
    # Run 10 iterations to measure average latency
    latencies = []
    for _ in range(10):
        t0 = time.perf_counter()
        db.execute(query).fetchall()
        latencies.append((time.perf_counter() - t0) * 1000)

    avg_query_latency_ms = sum(latencies) / len(latencies)
    print(f"[*] DuckDB Aggregation Query Latency: {avg_query_latency_ms:.2f} ms across {sample_size:,} records")

    print("\n" + "=" * 65)
    print(" 📊 OFFICIAL BENCHMARK SUMMARY FOR BADGES & PORTFOLIO")
    print("=" * 65)
    print(f" - Throughput:        ~{trans_throughput:,.0f} events/sec")
    print(f" - Per-Event Latency: {t_transform*1000/sample_size:.3f} ms")
    print(f" - Storage Savings:   {compression_savings:.1f}% vs JSON")
    print(f" - OLAP Query Speed:  {avg_query_latency_ms:.2f} ms (DuckDB)")
    print("=" * 65 + "\n")

    return {
        "throughput": round(trans_throughput),
        "latency_ms": round(t_transform*1000/sample_size, 3),
        "savings_pct": round(compression_savings, 1),
        "query_latency_ms": round(avg_query_latency_ms, 2)
    }

if __name__ == "__main__":
    run_benchmarks(sample_size=5000)
