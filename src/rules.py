import logging
from enum import Enum
from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger(__name__)

class ViolationType(Enum):
    """
    Standardized Enum for violation categories to ensure data integrity
    and efficient storage in MongoDB.
    """
    SPEED = "speed"
    AVERAGE_SPEED = "average_speed"

class ViolationDetector:
    """
    Refined Violation Detector optimized for the Flattened Specific-Field schema.
    Supports: 
    1. Instantaneous Speed: speed > camera_limit
    2. Average Speed: avg_speed > ending_camera_limit
    """   
    def __init__(self):
        pass

    def detect_instantaneous(self, enriched_df: DataFrame) -> DataFrame:
        """
        Detects violations where car speed exceeds the specific camera limit.
        Nests telemetry data into 'event_details' to match the polymorphic schema.

        :Args:
            enriched_df: DataFrame containing camera events enriched with speed limits.
        """
        return enriched_df.filter(F.col("speed_reading") > F.col("speed_limit")) \
            .withColumn("violation_type", F.lit(ViolationType.SPEED.value)) \
            .withColumn("violation_date", F.date_format(F.col("timestamp"), "yyyy-MM-dd")) \
            .withColumn("event_details", F.struct(
                F.col("camera_id"),
                F.col("timestamp"),
                F.col("speed_reading").alias("speed")
            )) \
            .select(
                "car_plate",
                "violation_date",
                "violation_type",
                "speed_limit",
                "event_details"
            )

    def detect_average_speed(self, joined_df: DataFrame) -> DataFrame:
        """
        Detects violations where avg speed between two cameras exceeds the limit.
        Computes telemetry and nests it into 'event_details'.

        :Args:
            joined_df: DataFrame resulting from joining camera events for the same car across two cameras.
        """
        # Logic: distance / time_diff_hours
        # 3600.0 converts seconds (from unix_timestamp) to hours
        time_diff_hours = (F.unix_timestamp("timestamp_end") - F.unix_timestamp("timestamp_start")) / 3600.0
        # Use camera IDs as a proxy for physical ordering, since the source stream
        # does not include explicit position fields in this implementation.
        distance = F.abs(F.col("camera_id_end") - F.col("camera_id_start"))
        
        # Calculate speed and filter by limit
        result_df = joined_df.withColumn("avg_speed", distance / time_diff_hours) \
            .filter(F.col("avg_speed") > F.col("speed_limit_end")) \
            .withColumn("violation_type", F.lit(ViolationType.AVERAGE_SPEED.value)) \
            .withColumn("violation_date", F.date_format(F.col("timestamp_end"), "yyyy-MM-dd"))
        
        # Nest the specific point-to-point fields into event_details
        return result_df.withColumn("event_details", F.struct(
                F.col("camera_id_start"),
                F.col("camera_id_end"),
                F.col("timestamp_start"),
                F.col("timestamp_end"),
                F.col("avg_speed")
            )) \
            .select(
                F.col("start.car_plate").alias("car_plate"),
                "violation_date",
                "violation_type",
                F.col("speed_limit_end").alias("speed_limit"),
                "event_details"
            )

class DailyViolationMerger:
    """
    Groups all violations for a car into a single daily record.
    Uses the new 'event_details' structure for the collect_list aggregation.
    """
    def merge(self, violations_df: DataFrame) -> DataFrame:
        """
        Groups violations by car_plate and violation_date, collecting all details into a list.
        This is optimized for the Flattened Specific-Field schema where each violation has a structured 'event_details' field.

        :Args:
            violations_df: DataFrame containing detected violations with 'car_plate', 'violation_date', and 'event_details' fields.
        """
        # Notes: In a streaming context, ensure watermarks are applied before calling this.
        return violations_df.groupBy("car_plate", "violation_date") \
            .agg(
                F.collect_list(
                    F.struct(
                        "violation_type", 
                        "speed_limit", 
                        "event_details"
                    )
                ).alias("violations")
            )