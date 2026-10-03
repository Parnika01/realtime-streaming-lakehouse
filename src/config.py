import os
import socket
from pathlib import Path
from pydantic_settings import BaseSettings

PROJECT_ROOT = Path(__file__).resolve().parent.parent

class PipelineConfig(BaseSettings):
    # Kafka / Redpanda Settings
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "financial_transactions")
    KAFKA_ANOMALY_TOPIC: str = os.getenv("KAFKA_ANOMALY_TOPIC", "transaction_anomalies")
    CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID", "lakehouse_stream_consumer_v1")

    # Storage Paths (Local Lakehouse & MinIO)
    STORAGE_BASE_DIR: Path = PROJECT_ROOT / "lakehouse_storage"
    BRONZE_PATH: Path = PROJECT_ROOT / "lakehouse_storage" / "bronze"
    SILVER_PATH: Path = PROJECT_ROOT / "lakehouse_storage" / "silver"
    GOLD_PATH: Path = PROJECT_ROOT / "lakehouse_storage" / "gold"
    CHECKPOINT_PATH: Path = PROJECT_ROOT / "lakehouse_storage" / "checkpoints"

    # MinIO / S3 Settings
    S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "lakehouse-data")

    # Streaming Parameters
    BATCH_INTERVAL_SECONDS: int = 2
    WATERMARK_DELAY: str = "10 minutes"
    WINDOW_DURATION: str = "1 minute"
    SLIDE_DURATION: str = "30 seconds"

    def is_kafka_available(self) -> bool:
        """Check if Kafka/Redpanda broker is actively listening on bootstrap port."""
        try:
            host, port_str = self.KAFKA_BOOTSTRAP_SERVERS.split(",")[0].split(":")
            with socket.create_connection((host, int(port_str)), timeout=1.0):
                return True
        except Exception:
            return False

    def ensure_directories(self):
        """Create necessary lakehouse storage folders."""
        for p in [self.BRONZE_PATH, self.SILVER_PATH, self.GOLD_PATH, self.CHECKPOINT_PATH]:
            p.mkdir(parents=True, exist_ok=True)

config = PipelineConfig()
config.ensure_directories()
