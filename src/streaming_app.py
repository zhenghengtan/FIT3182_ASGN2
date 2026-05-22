import json
import logging
from typing import Any, Dict, List

import pymongo
from pymongo import UpdateOne
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

import config
from rules import ViolationDetector

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MongoViolationSink:
    """A foreachBatch sink for writing detected violations safely to MongoDB."""

    def __init__(self, uri: str, database: str, collection: str, max_retries: int = 3, retry_backoff_seconds: float = 1.0) -> None:
        self.uri = uri
        self.database = database
        self.collection = collection
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.client = None
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Create a reusable MongoDB client with a simple retry strategy."""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.client = pymongo.MongoClient(self.uri, serverSelectionTimeoutMS=5000)
                self.client.admin.command("ping")
                logger.info("Connected to MongoDB successfully")
                return
            except Exception as exc:
                logger.warning(
                    "MongoDB connection attempt %d failed: %s",
                    attempt,
                    exc,
                )
                if attempt == self.max_retries:
                    logger.error("Exceeded MongoDB connection retries")
                    raise
                else:
                    from time import sleep

                    sleep(self.retry_backoff_seconds * attempt)

    def _build_bulk_operations(self, docs: List[Dict[str, Any]]) -> List[UpdateOne]:
        """Create upsert operations for each violation document."""
        ops: List[UpdateOne] = []
        for doc in docs:
            filter_doc = {
                "car_plate": doc["car_plate"],
                "violation_date": doc["violation_date"],
                "violation_type": doc["violation_type"],
                "event_details": doc["event_details"],
            }
            ops.append(UpdateOne(filter_doc, {"$setOnInsert": doc}, upsert=True))
        return ops

    def write_batch(self, df: DataFrame, epoch_id: int) -> None:
        """Write a micro-batch of violations to MongoDB using bulk upsert semantics."""
        if df.rdd.isEmpty():
            logger.debug("No records to write for epoch %d", epoch_id)
            return

        docs = [row.asDict(recursive=True) for row in df.collect()]
        logger.info("Preparing %d violation documents for MongoDB write", len(docs))

        for attempt in range(1, self.max_retries + 1):
            try:
                collection = self.client[self.database][self.collection]
                operations = self._build_bulk_operations(docs)
                if operations:
                    result = collection.bulk_write(operations, ordered=False)
                    logger.info(
                        "Epoch %d: MongoDB write complete. upserted=%d, modified=%d",
                        epoch_id,
                        len(result.upserted_ids) if result.upserted_ids else 0,
                        result.modified_count,
                    )
                return
            except Exception as exc:
                logger.warning(
                    "MongoDB write attempt %d failed for epoch %d: %s",
                    attempt,
                    epoch_id,
                    exc,
                )
                if attempt == self.max_retries:
                    logger.error("MongoDB write failed after %d attempts", self.max_retries)
                    raise
                from time import sleep

                sleep(self.retry_backoff_seconds * attempt)


def create_spark_session() -> SparkSession:
    """Create the Spark session used for structured streaming."""
    return SparkSession.builder.master(config.SPARK_MASTER).appName(config.SPARK_APP_NAME).getOrCreate()


def get_event_schema() -> T.StructType:
    """Define the schema used to parse JSON-enriched Kafka records."""
    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("batch_id", T.LongType(), True),
            T.StructField("car_plate", T.StringType(), True),
            T.StructField("camera_id", T.IntegerType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("speed_reading", T.DoubleType(), True),
            T.StructField("ingestion_timestamp", T.StringType(), True),
        ]
    )


def parse_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read from Kafka and parse the JSON payload into annotated event rows."""
    raw_df = (
        spark.readStream.format("kafka")
        .options(**config.KAFKA_SOURCE_CONFIG)
        .load()
    )

    parsed_df = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(F.col("value").cast("string"), get_event_schema()).alias("event")
    ).select("kafka_key", "event.*")

    return (
        parsed_df
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("speed_reading", F.col("speed_reading").cast("double"))
        .withColumn("camera_id", F.col("camera_id").cast("integer"))
        .filter(F.col("car_plate").isNotNull() & F.col("camera_id").isNotNull() & F.col("timestamp").isNotNull())
    )


def enrich_speed_limits(events_df: DataFrame) -> DataFrame:
    """Add configured speed limits to each camera event using a broadcast join."""
    camera_limit_entries = [(cid, conf["speed_limit"]) for cid, conf in config.CAMERA_CONFIG.items()]
    camera_limits_df = events_df.sparkSession.createDataFrame(camera_limit_entries, ["camera_id", "speed_limit"])
    camera_limits_df = F.broadcast(camera_limits_df)

    enriched = events_df.join(camera_limits_df, on="camera_id", how="left")
    return enriched


def build_average_speed_join(events_df: DataFrame) -> DataFrame:
    """Join camera events over the configured tolerance window for average-speed detection.

    This stream-stream join uses event-time watermarks to bound state and ensure
    unmatched or late records are cleaned up after the watermark threshold. The
    join conservatively retains records for at most `watermark_seconds` seconds,
    while only pairing events that occur within the configured tolerance window.
    """
    watermark_seconds = config.STREAMING_WINDOW_CONFIG["watermark_seconds"]
    tolerance_seconds = config.STREAMING_WINDOW_CONFIG["inter_camera_time_tolerance_seconds"]

    streaming_events = events_df.withWatermark("timestamp", f"{watermark_seconds} seconds")

    start = streaming_events.alias("start")
    end = streaming_events.alias("end")

    join_condition = (
        (F.col("start.car_plate") == F.col("end.car_plate"))
        & (F.col("start.camera_id") < F.col("end.camera_id"))
        & (F.col("end.timestamp") > F.col("start.timestamp"))
        & (
            F.col("end.timestamp").cast("long") - F.col("start.timestamp").cast("long")
            <= tolerance_seconds
        )
    )

    joined = (
        start.join(end, join_condition, how="inner")
        .select(
            F.struct("start.*").alias("start"),
            F.struct("end.*").alias("end"),
            F.col("start.camera_id").alias("camera_id_start"),
            F.col("end.camera_id").alias("camera_id_end"),
            F.col("start.timestamp").alias("timestamp_start"),
            F.col("end.timestamp").alias("timestamp_end"),
            F.col("end.speed_limit").alias("speed_limit_end"),
        )
    )

    return joined


def build_violation_stream(events_df: DataFrame) -> DataFrame:
    """Detect instantaneous and average-speed violations and merge them into one stream."""
    detector = ViolationDetector()
    enriched_df = enrich_speed_limits(events_df)
    instant_violations = detector.detect_instantaneous(enriched_df)

    average_event_pairs = build_average_speed_join(enriched_df)
    average_violations = detector.detect_average_speed(average_event_pairs)

    return instant_violations.unionByName(average_violations)


def run_streaming_app() -> None:
    """Run the structured streaming pipeline end-to-end."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    events_df = parse_kafka_stream(spark)
    violations_df = build_violation_stream(events_df)

    mongo_sink = MongoViolationSink(
        uri=config.MONGODB_URI,
        database=config.MONGODB_DATABASE,
        collection="violations",
        max_retries=3,
        retry_backoff_seconds=1.0,
    )

    query = (
        violations_df.writeStream
        .foreachBatch(mongo_sink.write_batch)
        .outputMode("append")
        .option("checkpointLocation", config.OUTPUT_CONFIG["checkpoint_location"])
        .trigger(processingTime=config.OUTPUT_CONFIG["trigger_interval"])
    )

    logger.info("Starting structured streaming query...")
    query.start().awaitTermination()


if __name__ == "__main__":
    run_streaming_app()
