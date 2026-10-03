"""
PySpark Structured Streaming Application
Demonstrates enterprise-grade streaming consumption from Apache Kafka / Redpanda,
schema enforcement, event-time watermarking, tumbling window aggregations,
and sink writing into Apache Iceberg / Parquet lakehouse storage.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, expr,
    current_timestamp, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType
)

# 1. Define strict transaction schema
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("card_number_masked", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("is_fraud_flag", BooleanType(), True),
    StructField("fraud_type", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])

def create_spark_session(app_name: str = "FinancialFraudStreaming") -> SparkSession:
    """Build Spark session configured for Kafka and Parquet/Delta lakehouse."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.streaming.checkpointLocation", "./lakehouse_storage/checkpoints")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def run_pyspark_pipeline(kafka_bootstrap_servers: str = "localhost:19092", topic: str = "financial_transactions"):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Subscribing to Kafka stream at {kafka_bootstrap_servers}, topic: {topic}")

    # Step 1: Ingest streaming bytes from Kafka broker
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Step 2: Bronze Layer - Parse binary payload to structured schema
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_received_time")
        .select(
            from_json(col("json_payload"), TRANSACTION_SCHEMA).alias("data"),
            col("kafka_received_time")
        )
        .select("data.*", "kafka_received_time")
        .withColumn("event_time", to_timestamp(col("event_timestamp")))
    )

    # Step 3: Silver Layer - Watermarking and Anomaly Risk Scoring
    silver_df = (
        parsed_df
        .withWatermark("event_time", "10 minutes")
        .withColumn(
            "risk_score",
            when(col("amount") > 3000.0, 75)
            .when(col("amount") > 1000.0, 50)
            .when(col("amount") < 2.0, 40)
            .otherwise(15)
        )
        .withColumn("is_high_risk", col("risk_score") >= 60)
        .withColumn("processing_time", current_timestamp())
    )

    # Step 4: Gold Layer - 1-minute Tumbling Window Aggregation
    gold_windowed_df = (
        silver_df
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("merchant_category")
        )
        .agg(
            expr("count(*) as transaction_count"),
            expr("sum(amount) as total_volume_usd"),
            expr("avg(amount) as avg_amount_usd"),
            expr("sum(case when is_high_risk then 1 else 0 end) as high_risk_alerts")
        )
    )

    # Step 5: Streaming sinks
    # Sink 1: Silver partitioned Parquet sink
    silver_query = (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "./lakehouse_storage/silver")
        .option("checkpointLocation", "./lakehouse_storage/checkpoints/silver")
        .start()
    )

    # Sink 2: Console output for live debugging
    console_query = (
        gold_windowed_df.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("[*] Streaming jobs active. Awaiting new events...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_pyspark_pipeline()
