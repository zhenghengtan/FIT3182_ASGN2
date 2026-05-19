import sys
import pandas as pd
import json
import logging
import time
from datetime import datetime, timezone  # Add 'timezone'
from typing import Dict, List, Tuple
from kafka import KafkaProducer

from config import KAFKA_PRODUCER_CONFIG, DATA_FILES, PRODUCER_CONFIG

logger = logging.getLogger(__name__)

class CameraEventProducer:
    """
    Produces camera event messages to Kafka topics using Pandas for data handling.
    """
    def __init__(self, config: Dict, producer_id: str) -> None:
        """
        Initialize Kafka producer.
        
        Args:
            config: Configuration dictionary with Kafka settings
            producer_id: Identifier for this specific producer (e.g., 'camera_producer_A')
        """
        self.config = config
        self.producer_id = producer_id
        self.producer = None
        self.initialize_producer()
        
    def initialize_producer(self) -> None:
        """
        Initialize Kafka producer with configured settings.
        """
        kafka_config = self.config.get("kafka_producer_config", {})
        self.producer = KafkaProducer(
            **kafka_config,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        logger.info(f"Kafka producer {self.producer_id} initialized successfully")

    
    def load_and_group_events(self, csv_path: str) -> Dict[int, List[Dict]]:
        """
        Use Pandas to load CSV and group by batch_id.
        Returns a dictionary where keys are batch_ids and values are lists of event dictionaries.

        Args:
            csv_path: Path to the CSV file containing camera events.
        """
        df = pd.read_csv(csv_path)
        df['producer_id'] = self.producer_id
        
        batches = {
            batch_id: group.to_dict('records') 
            for batch_id, group in df.groupby('batch_id')
        }
            
        logger.info(f"Loaded {len(df)} events from {csv_path} into {len(batches)} batches")
        return batches

    def publish_batch(self, topic: str, batch_id: int, events: List[Dict]) -> Tuple[int, int]:
        """
        Publish a batch of events to Kafka with simplified metadata.
        Returns a tuple of (success_count, fail_count) for logging purposes.

        Args:
            topic: Kafka topic to publish to
            batch_id: Identifier for the batch being published
            events: List of event dictionaries to publish
        """
        success_count = 0
        fail_count = 0
        
        for event in events:
            message_value = {
                **event,
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "batch_id": batch_id # Ensure batch_id is present
            }
            
            message_key = str(event.get('car_plate', ''))
                
            # Send message without extra headers Inside publish_batch
            try:
                future = self.producer.send(topic, key=message_key, value=message_value)
                # Removing .get() makes it async and much faster
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                fail_count += 1
                continue
        
        return success_count, fail_count

    def stream_events(self, csv_path: str, topic: str, batch_interval_seconds: float = 5.0) -> None:
        """
        Streams batches with defined intervals and robust logging.

        Args:
            csv_path: Path to the CSV file containing camera events.
            topic: Kafka topic to publish to.
            batch_interval_seconds: Interval between batch publications.
        """
        batches = self.load_and_group_events(csv_path)
        
        for batch_id in sorted(batches.keys()):
            batch_events = batches[batch_id]
            logger.info(f"[{self.producer_id}] Publishing batch {batch_id}...")
            
            success, fail = self.publish_batch(topic, batch_id, batch_events)
            
            logger.info(f"Published {success} success, {fail} failed. Sleeping {batch_interval_seconds}s...")
            if batch_id < max(batches.keys()):
                time.sleep(batch_interval_seconds)
        
        self.producer.flush()

class MultiProducer:
    """
    Manages multiple producers in a structured way.
    """
    def __init__(self, config: Dict) -> None:
        """
        Initialize Kafka producer.
        
        Args:
            config: Configuration dictionary with Kafka settings and data file paths
        """
        self.config = config
        self.producers = []

    def run_producer(self, file_key: str) -> None:
        """
        Runs a single producer based on config.

        Args:            
            file_key: Key to identify which camera's data to produce (e.g., 'camera_a')
        """
        file_config = self.config["data_files"][file_key]
        producer = CameraEventProducer(self.config, producer_id=f"camera_producer_{file_key[-1]}")
        
        producer.stream_events(
            csv_path=file_config["path"],
            topic=file_config["kafka_topic"],
            batch_interval_seconds=self.config["producer_config"]["batch_interval_seconds"]
        )


def run_producer_main():
    combined_config = {
        "kafka_producer_config": KAFKA_PRODUCER_CONFIG,
        "data_files": DATA_FILES,
        "producer_config": PRODUCER_CONFIG,
    }

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python producer_utils.py <camera_a|camera_b|camera_c>")

    camera_key = sys.argv[1]
    multiproducer = MultiProducer(combined_config)
    multiproducer.run_producer(camera_key)


if __name__ == "__main__":
    # output logging to console with timestamps and log levels
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run_producer_main()
