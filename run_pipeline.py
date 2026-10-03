import argparse
import sys
import threading
import time
from pathlib import Path

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.config import config
from src.producer.transaction_generator import TransactionProducer
from src.streaming.stream_processor import LakehouseStreamProcessor

def run_continuous_pipeline(rate: float = 3.0):
    """Runs producer and stream processor concurrently."""
    print("=" * 70)
    print("⚡ Real-Time Streaming Financial Lakehouse Pipeline Starting")
    print("=" * 70)
    print(f"[*] Kafka broker status: {'🟢 ONLINE' if config.is_kafka_available() else '🟡 LOCAL BUFFER MODE'}")
    print(f"[*] Lakehouse Directory: {config.STORAGE_BASE_DIR}")
    print("=" * 70)

    producer = TransactionProducer()
    processor = LakehouseStreamProcessor()

    # Worker thread for streaming processor
    def processor_worker():
        while True:
            try:
                processor.process_micro_batch()
            except Exception as e:
                print(f"[Processor Error] {e}")
            time.sleep(2.0)

    processor_thread = threading.Thread(target=processor_worker, daemon=True)
    processor_thread.start()

    # Main thread runs producer
    try:
        producer.run_stream(events_per_second=rate)
    except KeyboardInterrupt:
        print("\n[*] Shutting down pipeline cleanly.")

def seed_initial_data(count: int = 150):
    """Seeds initial batch of transactions so lakehouse has historical data immediately."""
    print(f"[*] Generating initial batch of {count} transactions...")
    producer = TransactionProducer()
    processor = LakehouseStreamProcessor()

    for _ in range(count):
        producer.emit(producer.generate_transaction() if hasattr(producer, "generate_transaction") else __import__("src.producer.transaction_generator", fromlist=["generate_transaction"]).generate_transaction())

    processed = processor.process_micro_batch()
    print(f"[SUCCESS] Initialized Lakehouse with {processed} curated records.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-Time Streaming Lakehouse Runner")
    parser.add_argument("--seed", type=int, default=100, help="Seed N events before starting")
    parser.add_argument("--rate", type=float, default=2.5, help="Events per second (0 to exit after seeding)")
    args = parser.parse_args()

    if args.seed > 0:
        seed_initial_data(args.seed)

    if args.rate > 0:
        run_continuous_pipeline(rate=args.rate)
    else:
        print("[SUCCESS] Batch initialization completed. Lakehouse is populated and ready for queries.")
