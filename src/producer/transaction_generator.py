import json
import time
import random
import uuid
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator, Optional
from faker import Faker

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.config import config

fake = Faker()

# Pre-seeded reference data for realistic financial transactions
MERCHANTS = [
    {"name": "Whole Foods Market", "category": "groceries", "avg_spend": 85.0},
    {"name": "Amazon US", "category": "ecommerce", "avg_spend": 65.0},
    {"name": "Apple Store", "category": "electronics", "avg_spend": 850.0},
    {"name": "Shell Gas Station", "category": "fuel", "avg_spend": 45.0},
    {"name": "Starbucks Coffee", "category": "dining", "avg_spend": 7.50},
    {"name": "Delta Air Lines", "category": "travel", "avg_spend": 450.0},
    {"name": "Rolex Boutique", "category": "luxury", "avg_spend": 6200.0},
    {"name": "Steam Games", "category": "entertainment", "avg_spend": 29.99},
    {"name": "Uber Rides", "category": "transport", "avg_spend": 22.0},
    {"name": "Gucci Flagship", "category": "luxury", "avg_spend": 3400.0}
]

LOCATIONS = [
    {"city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"city": "San Francisco", "country": "US", "lat": 37.7749, "lon": -122.4194},
    {"city": "Chicago", "country": "US", "lat": 41.8781, "lon": -87.6298},
    {"city": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"city": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
    {"city": "Toronto", "country": "CA", "lat": 43.6532, "lon": -79.3832},
    {"city": "Singapore", "country": "SG", "lat": 1.3521, "lon": 103.8198}
]

# Track recent user state to simulate realistic fraud (e.g. geo hopping)
USER_STATE_CACHE = {}

def generate_transaction() -> Dict[str, Any]:
    """Generates a realistic transaction event with simulated fraud anomalies."""
    now_utc = datetime.now(timezone.utc).isoformat()
    user_id = f"usr_{random.randint(1000, 9999)}"
    
    # 7% probability of generating a simulated fraud anomaly
    is_fraud = random.random() < 0.07
    fraud_type: Optional[str] = None

    if is_fraud:
        fraud_scenario = random.choice([
            "HIGH_VALUE_OUTLIER",
            "GEO_IMPOSSIBLE_TRAVEL",
            "CARD_TESTING_MICROPAYMENT"
        ])
        fraud_type = fraud_scenario

        if fraud_scenario == "HIGH_VALUE_OUTLIER":
            merchant = random.choice([m for m in MERCHANTS if m["category"] == "luxury"])
            amount = round(random.uniform(4500.0, 12000.0), 2)
            loc = random.choice(LOCATIONS)
        elif fraud_scenario == "GEO_IMPOSSIBLE_TRAVEL":
            merchant = random.choice(MERCHANTS)
            amount = round(random.uniform(50.0, 400.0), 2)
            # Pick a drastically different continent
            loc = random.choice([l for l in LOCATIONS if l["country"] in ["JP", "SG", "GB"]])
        else: # CARD_TESTING_MICROPAYMENT
            merchant = random.choice(MERCHANTS)
            amount = round(random.uniform(0.50, 2.50), 2)
            loc = random.choice(LOCATIONS)
    else:
        merchant = random.choice(MERCHANTS)
        # Normal distribution around merchant avg spend
        amount = max(1.0, round(random.gauss(merchant["avg_spend"], merchant["avg_spend"] * 0.35), 2))
        loc = random.choice(LOCATIONS)

    # Check for previous user state
    USER_STATE_CACHE[user_id] = {
        "last_seen": now_utc,
        "last_loc": loc["city"],
        "last_country": loc["country"]
    }

    event = {
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": now_utc,
        "user_id": user_id,
        "card_number_masked": f"4{random.randint(100, 999)}-XXXX-XXXX-{random.randint(1000, 9999)}",
        "amount": amount,
        "currency": "USD",
        "merchant_name": merchant["name"],
        "merchant_category": merchant["category"],
        "city": loc["city"],
        "country": loc["country"],
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "device_id": f"dev_{hash(user_id) % 500:04d}",
        "ip_address": fake.ipv4_public(),
        "is_fraud_flag": is_fraud,
        "fraud_type": fraud_type if is_fraud else "NONE",
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
    }
    return event

class TransactionProducer:
    """
    Robust Streaming Producer.
    Tries Kafka broker first. If Kafka is not running, falls back to local micro-batch buffer.
    """
    def __init__(self):
        self.kafka_available = config.is_kafka_available()
        self.producer = None
        self.buffer_file = config.BRONZE_PATH / "streaming_buffer.jsonl"
        self.buffer_file.parent.mkdir(parents=True, exist_ok=True)

        if self.kafka_available:
            try:
                from kafka import KafkaProducer
                self.producer = KafkaProducer(
                    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=3
                )
                print(f"[Producer] Connected to Kafka broker at {config.KAFKA_BOOTSTRAP_SERVERS}")
            except Exception as e:
                print(f"[Producer] Kafka init failed ({e}), falling back to file buffer.")
                self.kafka_available = False
        else:
            print(f"[Producer] Kafka broker offline. Streaming directly to Bronze buffer: {self.buffer_file}")

    def emit(self, event: Dict[str, Any]):
        """Publish a single transaction event."""
        # 1. Always append to local streaming buffer for local processing & audit
        with open(self.buffer_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

        # 2. If Kafka is active, push to Kafka topic
        if self.producer and self.kafka_available:
            self.producer.send(config.KAFKA_TOPIC, value=event)

    def run_stream(self, events_per_second: float = 5.0, total_events: Optional[int] = None):
        """Continuously generate events at specified rate."""
        delay = 1.0 / max(0.1, events_per_second)
        count = 0
        print(f"[Producer] Starting live transaction stream (~{events_per_second} events/sec)...")
        try:
            while total_events is None or count < total_events:
                event = generate_transaction()
                self.emit(event)
                count += 1
                fraud_tag = f"🚨 [{event['fraud_type']}]" if event["is_fraud_flag"] else "✓ [NORMAL]"
                print(f"[{count:05d}] {fraud_tag} ${event['amount']:>8.2f} | {event['merchant_name']:<20} | {event['city']}, {event['country']}")
                time.sleep(delay)
        except KeyboardInterrupt:
            print("\n[Producer] Stream stopped by user.")
        finally:
            if self.producer:
                self.producer.flush()

if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_stream(events_per_second=4, total_events=50)
