"""Visualisation utilities for traffic violation analysis.

This module provides helper functions and classes to transform nested violation
records into a flat Pandas DataFrame and create summary visualisations.
"""

import logging
import pandas as pd
import matplotlib.pyplot as plt

import pymongo

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ViolationVisualizer:
    """Visualises violation events retrieved from MongoDB.

    The input record format is expected to contain a list of violations per event
    and nested event detail fields such as timestamps, speed and average speed.
    """

    def __init__(self, violations_data: list) -> None:
        """Initialize with data from MongoDB and flatten nested violation details.

        Args:
            violations_data: A list of violation documents, where each document may
                include a nested `violations` list and `event_details` object.
        """
        self.raw_df = pd.DataFrame(violations_data)
        self.violations_df = self._flatten_data()

    def _flatten_data(self) -> pd.DataFrame:
        """Flatten nested violation event data into a Pandas DataFrame.

        Returns:
            A DataFrame containing flat violation records with `arrival_time` and
            `speed_val` columns extracted from nested event detail fields.
        """
        if self.raw_df.empty:
            return pd.DataFrame()

        df_exploded = self.raw_df.explode('violations').reset_index(drop=True)
        violations_details = pd.json_normalize(df_exploded['violations'])

        violations_details['car_plate'] = df_exploded['car_plate']
        violations_details['violation_date'] = pd.to_datetime(df_exploded['violation_date'])

        violations_details['arrival_time'] = violations_details.apply(
            self._extract_arrival_time, axis=1
        )
        violations_details['speed_val'] = violations_details.apply(
            self._extract_speed_value, axis=1
        )

        return violations_details.dropna(subset=['arrival_time'])

    def _extract_arrival_time(self, row: pd.Series) -> pd.Timestamp:
        """Return the correct arrival time from nested event details.

        Args:
            row: A row containing normalized violation data.

        Returns:
            A pandas Timestamp for the violation arrival event.
        """
        if row.get('violation_type') == 'speed':
            return pd.to_datetime(row.get('event_details.timestamp'))

        return pd.to_datetime(row.get('event_details.timestamp_start'))

    def _extract_speed_value(self, row: pd.Series) -> float:
        """Return the relevant speed value depending on violation type.

        Args:
            row: A row containing normalized violation data.

        Returns:
            A numeric speed value for the violation.
        """
        if row.get('violation_type') == 'speed':
            return row.get('event_details.speed')
        return row.get('event_details.avg_speed')

    def plot_summary(self, output_path: str = "traffic_summary.png") -> None:
        """Create and save side-by-side plots for traffic volume and speed patterns.

        Args:
            output_path: Output file path for the generated PNG image.
        """
        if self.violations_df.empty:
            logger.warning("No data to plot.")
            return

        # Prepare Aggregation (Counts per minute)
        self.violations_df['minute'] = self.violations_df['arrival_time'].dt.floor('min')
        counts = self.violations_df.groupby('minute').size().reset_index(name='count')

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # --- Plot 1: Violations Over Time ---
        ax1.plot(counts['minute'], counts['count'], color='tab:red', marker='o', label='Violations')
        ax1.set_title('Violations over Time (Arrival)', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Annotate Peak
        if not counts.empty:
            max_row = counts.loc[counts['count'].idxmax()]
            ax1.annotate(f"Peak: {max_row['count']}", 
                         xy=(max_row['minute'], max_row['count']), 
                         xytext=(10, 10), textcoords='offset points', 
                         arrowprops=dict(arrowstyle='->', color='red'))

        # --- Plot 2: Speed Pattern Analysis ---
        # Instantaneous
        inst = self.violations_df[self.violations_df['violation_type'] == 'speed']
        ax2.scatter(inst['arrival_time'], inst['speed_val'], color='red', label='Instantaneous', alpha=0.6)
        
        # Average Speed
        avg = self.violations_df[self.violations_df['violation_type'] == 'average_speed']
        ax2.scatter(avg['arrival_time'], avg['speed_val'], color='blue', label='Avg Speed', alpha=0.6)

        # Plot Limits
        ax2.axhline(y=110, color='green', linestyle='--', label='Limit 110')
        ax2.axhline(y=90, color='orange', linestyle='--', label='Limit 90')
        
        ax2.set_title('Speed Readings at Violation Points', fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(output_path)
        logger.info(f"Visualisation saved to {output_path}")

if __name__ == "__main__":
    # Connect to MongoDB
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["speed_enforcement_db"]
        violations = list(db.violations.find())
        
        logger.info(f"Successfully fetched {len(violations)} records from MongoDB.")
        
        # Run Visualization
        visualizer = ViolationVisualizer(violations)
        visualizer.plot_summary(output_path="traffic_summary.png")
        
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")