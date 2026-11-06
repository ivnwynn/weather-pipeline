"""Spark Structured Streaming job: Kafka -> parse/aggregate -> MongoDB.

Query 1: every valid record is upserted into `weather_raw` (history).
Query 2: 1-minute windowed aggregates (avg/min/max/count) are upserted into
         `weather_agg` (data aggregation).
"""
import logging
import os
import shutil
import sys
from datetime import datetime

import pyspark
from pymongo import ASCENDING, MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark_job")

SPARK_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoints")

SCHEMA = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("value", DoubleType()),
        StructField("metric_type", StringType()),
        StructField("sensor_id", StringType()),
    ]
)


def parse_ts(text):
    return datetime.strptime(text, config.TS_FORMAT)  # naive datetime, UTC


def get_collections():
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[config.MONGO_DB]
    raw, agg = db[config.RAW_COLLECTION], db[config.AGG_COLLECTION]
    raw.create_index(
        [("sensor_id", ASCENDING), ("metric_type", ASCENDING), ("timestamp", ASCENDING)],
        unique=True,
    )
    raw.create_index([("timestamp", ASCENDING)])
    agg.create_index(
        [("window_start", ASCENDING), ("sensor_id", ASCENDING), ("metric_type", ASCENDING)],
        unique=True,
    )
    return raw, agg


def build_spark():
    packages = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
    spark = (
        SparkSession.builder.appName("WeatherStreamingPipeline")
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def make_raw_writer(raw):
    def write(batch_df, batch_id):
        rows = batch_df.select("timestamp", "value", "metric_type", "sensor_id").collect()
        ops = []
        for r in rows:
            try:
                ts = parse_ts(r["timestamp"])
            except (TypeError, ValueError):
                continue
            key = {"sensor_id": r["sensor_id"], "metric_type": r["metric_type"], "timestamp": ts}
            ops.append(ReplaceOne(key, {**key, "value": float(r["value"])}, upsert=True))
        if ops:
            raw.bulk_write(ops, ordered=False)
            log.info("[raw] batch %s: stored %d records", batch_id, len(ops))

    return write


def make_agg_writer(agg):
    def write(batch_df, batch_id):
        ops = []
        for r in batch_df.collect():
            key = {
                "window_start": parse_ts(r["window_start"]),
                "sensor_id": r["sensor_id"],
                "metric_type": r["metric_type"],
            }
            doc = {
                **key,
                "window_end": parse_ts(r["window_end"]),
                "avg_value": round(float(r["avg_value"]), 4),
                "min_value": float(r["min_value"]),
                "max_value": float(r["max_value"]),
                "count": int(r["count"]),
            }
            ops.append(ReplaceOne(key, doc, upsert=True))
        if ops:
            agg.bulk_write(ops, ordered=False)
            log.info("[agg] batch %s: upserted %d windows", batch_id, len(ops))

    return write


def ensure_java():
    """Spark 3.5 needs Java 8/11/17(/21). Prefer Arch's jdk17-openjdk when present."""
    java17 = "/usr/lib/jvm/java-17-openjdk"
    if not os.environ.get("JAVA_HOME") and os.path.isdir(java17):
        os.environ["JAVA_HOME"] = java17
        os.environ["PATH"] = f"{java17}/bin:" + os.environ.get("PATH", "")
    if not shutil.which("java"):
        sys.exit("Java not found. On Arch: sudo pacman -S jdk17-openjdk")


def main():
    ensure_java()

    log.info("Checking MongoDB and Kafka...")
    try:
        raw, agg = get_collections()
    except Exception as exc:
        sys.exit(f"MongoDB not reachable at {config.MONGO_URI}: {exc}")
    config.ensure_topic()

    spark = build_spark()

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        stream.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp", SPARK_TS_FORMAT))
        .dropna(subset=["event_time", "value", "metric_type", "sensor_id"])
    )

    aggregated = (
        parsed.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", config.AGG_WINDOW), "sensor_id", "metric_type")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("count"),
        )
        .select(
            F.date_format("window.start", SPARK_TS_FORMAT).alias("window_start"),
            F.date_format("window.end", SPARK_TS_FORMAT).alias("window_end"),
            "sensor_id", "metric_type", "avg_value", "min_value", "max_value", "count",
        )
    )

    q_raw = (
        parsed.writeStream.outputMode("append")
        .foreachBatch(make_raw_writer(raw))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw"))
        .trigger(processingTime="5 seconds")
        .start()
    )
    q_agg = (
        aggregated.writeStream.outputMode("update")
        .foreachBatch(make_agg_writer(agg))
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "agg"))
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("Streaming started. Spark UI: http://localhost:4040  (Ctrl+C to stop)")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Stopping...")
    finally:
        for q in (q_raw, q_agg):
            if q.isActive:
                q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
