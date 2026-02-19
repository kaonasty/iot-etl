"""
Batch ETL - Transform Phase
Transforms and enriches sensor data with data quality checks and feature engineering
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, avg, stddev, count, sum as spark_sum,
    lag, lead, datediff, hour, dayofweek, month,
    unix_timestamp, from_unixtime, window, expr
)
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkTransformer:
    """Transform and enrich IoT sensor data"""
    
    def __init__(self):
        """Initialize transformer"""
        pass
    
    def enrich_with_device_metadata(self, sensor_df, devices_df, device_types_df):
        """
        Join sensor readings with device metadata
        
        Args:
            sensor_df: Sensor readings DataFrame
            devices_df: Devices DataFrame
            device_types_df: Device types DataFrame
        
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching sensor data with device metadata...")
        
        if devices_df is None or device_types_df is None:
            logger.warning("Device metadata not available, skipping enrichment")
            return sensor_df
        
        # Join with devices
        enriched = sensor_df.join(
            devices_df.select('device_id', 'device_name', 'device_type_id', 'status'),
            on='device_id',
            how='left'
        )
        
        # Join with device types
        enriched = enriched.join(
            device_types_df.select('device_type_id', 'type_name'),
            on='device_type_id',
            how='left'
        )
        
        logger.info(f"Enrichment complete: {enriched.count()} rows")
        return enriched
    
    def enrich_with_location_data(self, sensor_df, locations_df):
        """
        Add location context from device_locations table
        
        Args:
            sensor_df: Sensor readings DataFrame
            locations_df: Device locations DataFrame (from PostgreSQL)
        
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching sensor data with location information...")
        
        if locations_df is None:
            logger.warning("Location data not available, skipping enrichment")
            return sensor_df
        
        # Join with device_locations (only has device_id, room_id, position, etc.)
        # Note: building_name, floor, room are in separate tables (buildings, floors, rooms)
        # For now, just join room_id - full hierarchy would require joining those tables too
        enriched = sensor_df.join(
            locations_df.select('device_id', 'room_id'),
            on='device_id',
            how='left'
        )
        
        logger.info(f"Location enrichment complete: {enriched.count()} rows")
        return enriched
    
    def add_time_features(self, df):
        """
        Add time-based features for analytics
        
        Args:
            df: DataFrame with 'time' column
        
        Returns:
            DataFrame with time features
        """
        logger.info("Adding time-based features...")
        
        df = df.withColumn('hour_of_day', hour(col('time'))) \
               .withColumn('day_of_week', dayofweek(col('time'))) \
               .withColumn('month', month(col('time'))) \
               .withColumn('is_weekend', when(col('day_of_week').isin([1, 7]), lit(True)).otherwise(lit(False))) \
               .withColumn('is_business_hours', when((col('hour_of_day') >= 9) & (col('hour_of_day') <= 17), lit(True)).otherwise(lit(False)))
        
        return df
    
    def calculate_rolling_statistics(self, df):
        """
        Calculate rolling averages and statistics
        
        Args:
            df: DataFrame with sensor readings
        
        Returns:
            DataFrame with rolling statistics
        """
        logger.info("Calculating rolling statistics...")
        
        # Add unix timestamp column for time-based windows
        df = df.withColumn('time_unix', unix_timestamp(col('time')))
        
        # Define window specifications using unix timestamp
        # 5-minute rolling window (300 seconds)
        window_5min = Window.partitionBy('device_id').orderBy('time_unix').rangeBetween(-300, 0)
        
        # 1-hour rolling window (3600 seconds)
        window_1h = Window.partitionBy('device_id').orderBy('time_unix').rangeBetween(-3600, 0)
        
        # Add rolling statistics
        df = df.withColumn('rolling_avg_5min', avg('value').over(window_5min)) \
               .withColumn('rolling_avg_1h', avg('value').over(window_1h)) \
               .withColumn('rolling_stddev_1h', stddev('value').over(window_1h))
        
        # Add lag features (previous readings) - use row-based window
        window_lag = Window.partitionBy('device_id').orderBy('time')
        df = df.withColumn('value_lag_1', lag('value', 1).over(window_lag)) \
               .withColumn('value_lag_2', lag('value', 2).over(window_lag))
        
        # Calculate value change
        df = df.withColumn('value_change', col('value') - col('value_lag_1'))
        
        return df
    
    def detect_anomalies(self, df):
        """
        Detect anomalies using statistical methods
        
        Args:
            df: DataFrame with sensor readings
        
        Returns:
            DataFrame with anomaly flags
        """
        logger.info("Detecting anomalies using Z-score method...")
        
        # Calculate Z-score for each device type
        window_stats = Window.partitionBy('device_id', 'device_type')
        
        df = df.withColumn('mean_value', avg('value').over(window_stats)) \
               .withColumn('stddev_value', stddev('value').over(window_stats))
        
        # Calculate Z-score
        df = df.withColumn('z_score', 
                          when(col('stddev_value') > 0, 
                               (col('value') - col('mean_value')) / col('stddev_value'))
                          .otherwise(lit(0)))
        
        # Flag anomalies (|Z-score| > 3)
        df = df.withColumn('is_anomaly_zscore', 
                          when((col('z_score') > 3) | (col('z_score') < -3), lit(True))
                          .otherwise(lit(False)))
        
        # Combine with existing anomaly flag
        df = df.withColumn('is_anomaly_combined', 
                          col('is_anomaly') | col('is_anomaly_zscore'))
        
        return df
    
    def apply_data_quality_checks(self, df):
        """
        Apply data quality checks and flag issues
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame with quality flags
        """
        logger.info("Applying data quality checks...")
        
        # Check for null values in critical columns
        df = df.withColumn('has_null_value', 
                          when(col('value').isNull(), lit(True)).otherwise(lit(False)))
        
        # Check for invalid quality scores
        df = df.withColumn('has_invalid_quality', 
                          when((col('quality_score') < 0) | (col('quality_score') > 100), lit(True))
                          .otherwise(lit(False)))
        
        # Overall data quality flag
        df = df.withColumn('is_valid_record', 
                          ~(col('has_null_value') | col('has_invalid_quality')))
        
        # Log data quality metrics
        total_records = df.count()
        invalid_records = df.filter(col('is_valid_record') == False).count()
        quality_rate = (total_records - invalid_records) / total_records * 100
        
        logger.info(f"Data Quality: {quality_rate:.2f}% valid records ({total_records - invalid_records}/{total_records})")
        
        return df
    
    def transform_all(self, data):
        """
        Apply all transformations to the data
        
        Args:
            data: Dictionary of DataFrames from extraction
        
        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info("Starting transformation pipeline...")
        
        # Start with sensor readings
        df = data['sensor_readings']
        
        # Enrich with device metadata
        df = self.enrich_with_device_metadata(df, data.get('devices'), data.get('device_types'))
        
        # Enrich with location data
        df = self.enrich_with_location_data(df, data.get('locations'))
        
        # Add time features
        df = self.add_time_features(df)
        
        # Calculate rolling statistics
        df = self.calculate_rolling_statistics(df)
        
        # Detect anomalies
        df = self.detect_anomalies(df)
        
        # Apply data quality checks
        df = self.apply_data_quality_checks(df)
        
        logger.info("Transformation pipeline completed successfully")
        
        return df


if __name__ == "__main__":
    """Test transformation (requires extracted data)"""
    from spark_extract import SparkExtractor
    
    extractor = SparkExtractor()
    transformer = SparkTransformer()
    
    try:
        # Extract data
        data = extractor.extract_all_sources()
        
        # Transform data
        transformed_df = transformer.transform_all(data)
        
        # Display results
        print("\n" + "="*60)
        print("Transformed Data Sample")
        print("="*60)
        transformed_df.select(
            'time', 'device_id', 'value', 'rolling_avg_5min', 
            'is_anomaly_combined', 'is_valid_record'
        ).show(10, truncate=False)
        
        # Show anomaly statistics
        print("\n" + "="*60)
        print("Anomaly Statistics")
        print("="*60)
        transformed_df.groupBy('device_id', 'is_anomaly_combined').count().show()
    
    finally:
        extractor.stop()
