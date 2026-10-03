import pytest
import pandas as pd
from datetime import datetime, timezone
from src.producer.transaction_generator import generate_transaction
from src.streaming.stream_processor import LakehouseStreamProcessor

def test_generate_transaction_schema():
    """Verify that generated transaction events conform to the expected schema."""
    tx = generate_transaction()
    required_fields = [
        "transaction_id", "event_timestamp", "user_id", "card_number_masked",
        "amount", "currency", "merchant_name", "merchant_category",
        "city", "country", "latitude", "longitude", "is_fraud_flag"
    ]
    for field in required_fields:
        assert field in tx, f"Missing required field: {field}"

    assert isinstance(tx["amount"], (int, float))
    assert tx["amount"] > 0
    assert -90.0 <= tx["latitude"] <= 90.0
    assert -180.0 <= tx["longitude"] <= 180.0

def test_silver_transformation_and_risk_scoring():
    """Verify that high-value transactions receive appropriate risk scores and alerts."""
    processor = LakehouseStreamProcessor()
    
    sample_events = [
        {
            "transaction_id": "test-1",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": "usr_999",
            "card_number_masked": "4111-XXXX-XXXX-1111",
            "amount": 5500.0,  # High value outlier
            "currency": "USD",
            "merchant_name": "Rolex Boutique",
            "merchant_category": "luxury",
            "city": "New York",
            "country": "US",
            "latitude": 40.71,
            "longitude": -74.00,
            "is_fraud_flag": True,
            "fraud_type": "HIGH_VALUE_OUTLIER"
        },
        {
            "transaction_id": "test-2",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": "usr_888",
            "card_number_masked": "4111-XXXX-XXXX-2222",
            "amount": 15.0,  # Normal transaction
            "currency": "USD",
            "merchant_name": "Starbucks Coffee",
            "merchant_category": "dining",
            "city": "Chicago",
            "country": "US",
            "latitude": 41.87,
            "longitude": -87.62,
            "is_fraud_flag": False,
            "fraud_type": "NONE"
        }
    ]

    df_silver = processor.transform_silver(sample_events)
    assert len(df_silver) == 2
    assert "risk_score" in df_silver.columns
    assert "is_high_risk_alert" in df_silver.columns

    # High value luxury transaction should trigger alert
    luxury_row = df_silver[df_silver["transaction_id"] == "test-1"].iloc[0]
    assert luxury_row["risk_score"] >= 75
    assert luxury_row["is_high_risk_alert"] == True

    # Standard coffee transaction should have low risk
    coffee_row = df_silver[df_silver["transaction_id"] == "test-2"].iloc[0]
    assert coffee_row["risk_score"] < 50
    assert coffee_row["is_high_risk_alert"] == False
