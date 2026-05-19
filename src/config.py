import os
from datetime import timedelta

# ============================================================================
# KAFKA CONFIGURATION
# ============================================================================

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

# KAFKA_PRODUCER_CONFIG and used underscores for kafka-python compatibility
KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BROKERS,
    "client_id": "speed-violation-producer",
    "acks": "all",
    "retries": 3,
    "retry_backoff_ms": 100,
    "compression_type": "gzip",
}

# Topic names for each camera event stream
KAFKA_TOPICS = {
    "camera_a": "camera-events-camera-1",
    "camera_b": "camera-events-camera-2",
    "camera_c": "camera-events-camera-3",
    "violations": "speed-violations",
}

KAFKA_CONSUMER_GROUP = "speed-violation-streaming-app"

KAFKA_SOURCE_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BROKERS,
    "subscribe": ",".join(KAFKA_TOPICS.values()),
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": "10000",
    "group.id": KAFKA_CONSUMER_GROUP,
}

CAMERA_CONFIG = {
    1: {"camera_id": 1, "speed_limit": 110},
    2: {"camera_id": 2, "speed_limit": 110},
    3: {"camera_id": 3, "speed_limit": 90},
}

# ============================================================================
# MONGODB CONFIGURATION
# ============================================================================

MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.getenv("MONGODB_PORT", 27017))
MONGODB_URI = f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/"
MONGODB_DATABASE = "speed_enforcement_db"

# ============================================================================
# SPARK CONFIGURATION
# ============================================================================

SPARK_MASTER = "local[2]"
SPARK_APP_NAME = "SpeedViolationStreamingApp"
SPARK_BATCH_INTERVAL_SECONDS = 10

# ============================================================================
# LOGIC PARAMETERS (Manually defined constants for speed violation logic)
# ============================================================================

# Speed limits for each camera (in km/h)
CAMERA_SPEED_LIMITS = {
    "camera-1": 110,
    "camera-2": 110,
    "camera-3": 90,
}

# Distance between cameras (in kilometers)
# Used for average speed calculation
CAMERA_DISTANCES = {
    ("camera-1", "camera-2"): 1.0,  # 1 km between Cam 1 and 2
    ("camera-2", "camera-3"): 1.0,  # 1 km between Cam 2 and 3
    ("camera-1", "camera-3"): 2.0,  # Total distance
}

# Streaming window configurations
STREAMING_WINDOW_CONFIG = {
    "watermark_seconds": 60,
    "state_retention_seconds": 300,
    "inter_camera_time_tolerance_seconds": 120,
}

# ============================================================================
# PRODUCER SETTINGS
# ============================================================================

# How often to publish a new batch of events from CSV (to simulate real-time)
PRODUCER_CONFIG = {
    "batch_interval_seconds": 5,
}

# Dictionaries with a "path" key: file_config.get("path") in producer_utils.py
DATA_FILES = {
    "camera_a": {
        "path": "../data/camera_event_A.csv",
        "kafka_topic": "camera-events-camera-1",
        "camera_id": "camera-1",
    },
    "camera_b": {
        "path": "../data/camera_event_B.csv",
        "kafka_topic": "camera-events-camera-2",
        "camera_id": "camera-2",
    },
    "camera_c": {
        "path": "../data/camera_event_C.csv",
        "kafka_topic": "camera-events-camera-3",
        "camera_id": "camera-3",
    },
}

# ============================================================================
# OUTPUT CONFIGURATION
# ============================================================================

OUTPUT_CONFIG = {
    "checkpoint_location": "output",
    "trigger_interval": "10 seconds",
}