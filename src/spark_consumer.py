"""
spark_consumer.py
PySpark Structured Streaming consumer.
Reads from Kafka → validates → writes to Delta Lake bronze/silver layers.

Run with: python src/spark_consumer.py
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "raw-events"
BRONZE_PATH = "./data/bronze"
SILVER_PATH = "./data/silver"
CHECKPOINT_BRONZE = "./data/checkpoints/bronze"
CHECKPOINT_SILVER = "./data/checkpoints/silver"
DLQ_PATH = "./data/dlq"  # dead letter queue for invalid events

# Baseline schema — new fields from schema evolution will land in bronze as JSON
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("producer_id", StringType(), True),
    # v2 evolution fields — nullable so v1 events still parse
    StructField("analyst_rating", StringType(), True),
    StructField("market_cap_billions", DoubleType(), True),
])


def create_spark_session() -> SparkSession:
    """Create a local SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("StreamingPipeline-StockTicks")
        .master("local[*]")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BRONZE)
        .config("spark.sql.shuffle.partitions", "4")  # small for local dev
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def bronze_writer(batch_df, batch_id):
    """
    Write micro-batch to bronze layer (raw, append-only).
    Adds pipeline metadata columns.
    """
    if batch_df.rdd.isEmpty():
        return

    enriched = batch_df.withColumn("_ingested_at", F.current_timestamp()) \
                       .withColumn("_batch_id", F.lit(batch_id)) \
                       .withColumn("_pipeline", F.lit("spark-streaming-v1"))

    enriched.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("exchange", "schema_version") \
        .save(BRONZE_PATH)

    count = batch_df.count()
    logger.info(f"Bronze batch {batch_id}: wrote {count} records")


def silver_writer(batch_df, batch_id):
    """
    Silver layer: clean, validate, deduplicate.
    Filters null symbols/prices, deduplicates by event_id.
    """
    if batch_df.rdd.isEmpty():
        return

    # Separate valid from invalid events
    valid = batch_df.filter(
        F.col("symbol").isNotNull() &
        F.col("price").isNotNull() &
        F.col("price").cast("double").isNotNull() &
        (F.col("price") > 0)
    )

    invalid = batch_df.subtract(valid)

    # Write invalid events to DLQ
    if not invalid.rdd.isEmpty():
        invalid.withColumn("_rejected_at", F.current_timestamp()) \
               .withColumn("_reason", F.lit("null or invalid symbol/price")) \
               .write.format("delta").mode("append").save(DLQ_PATH)
        logger.warning(f"Silver batch {batch_id}: sent {invalid.count()} events to DLQ")

    # Deduplicate within the batch by event_id
    deduped = valid.dropDuplicates(["event_id"])

    # Add spread and mid-price derived columns
    enriched = deduped \
        .withColumn("spread", F.round(F.col("ask") - F.col("bid"), 4)) \
        .withColumn("mid_price", F.round((F.col("ask") + F.col("bid")) / 2, 4)) \
        .withColumn("event_ts", F.to_timestamp("event_timestamp")) \
        .withColumn("_processed_at", F.current_timestamp()) \
        .withColumn("_batch_id", F.lit(batch_id))

    enriched.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("symbol") \
        .save(SILVER_PATH)

    logger.info(f"Silver batch {batch_id}: wrote {deduped.count()} valid records")


def run():
    for path in [BRONZE_PATH, SILVER_PATH, DLQ_PATH, CHECKPOINT_BRONZE, CHECKPOINT_SILVER]:
        os.makedirs(path, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created. Connecting to Kafka...")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON events
    parsed = raw_stream.select(
        F.from_json(
            F.col("value").cast("string"),
            EVENT_SCHEMA
        ).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition"),
        F.col("offset")
    ).select("data.*", "kafka_timestamp", "partition", "offset")

    logger.info("Awaiting first micro-batch from Kafka topic: " + KAFKA_TOPIC)

    # Bronze stream — raw data sink
    bronze_query = parsed.writeStream \
        .foreachBatch(bronze_writer) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", CHECKPOINT_BRONZE) \
        .start()

    # Silver stream — validated and cleaned sink
    silver_query = parsed.writeStream \
        .foreachBatch(silver_writer) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", CHECKPOINT_SILVER) \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
