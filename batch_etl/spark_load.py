"""
Batch ETL - Load Phase
Loads transformed data into TimescaleDB data warehouse
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
import logging
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DatabaseConfig, SparkConfig, ETLConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkLoader:
    """Load transformed data into data warehouse"""
    
    def __init__(self):
        """Initialize loader"""
        pass
    
    def load_to_timescale(self, df, table_name, mode='append'):
        """
        Load DataFrame to TimescaleDB
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Write mode ('append', 'overwrite', 'ignore')
        
        Returns:
            Boolean indicating success
        """
        logger.info(f"Loading data to TimescaleDB table: {table_name}")
        
        jdbc_options = {
            "url": DatabaseConfig.get_timescale_jdbc_url(),
            "dbtable": table_name,
            "user": DatabaseConfig.TIMESCALE_USER,
            "password": DatabaseConfig.TIMESCALE_PASSWORD,
            "driver": SparkConfig.POSTGRES_DRIVER
        }
        
        try:
            # Add load timestamp
            df = df.withColumn('etl_loaded_at', current_timestamp())
            
            # Write to database
            df.write.format("jdbc") \
                .options(**jdbc_options) \
                .mode(mode) \
                .save()
            
            logger.info(f"Successfully loaded {df.count()} rows to {table_name}")
            return True
        
        except Exception as e:
            logger.error(f"Error loading to TimescaleDB: {e}")
            raise
    
    def load_fact_table(self, df):
        """
        Load data to fact table (main enriched sensor readings)
        
        Args:
            df: Transformed DataFrame
        
        Returns:
            Boolean indicating success
        """
        logger.info("Loading data to fact table...")
        
        # Select columns for fact table
        fact_columns = [
            'time',
            'device_id',
            'device_type',
            'value',
            'unit',
            'quality_score',
            'is_anomaly_combined',
            'rolling_avg_5min',
            'rolling_avg_1h',
            'value_lag_1',
            'value_change',
            'hour_of_day',
            'day_of_week',
            'is_weekend',
            'is_business_hours',
            'is_valid_record'
        ]
        
        # Filter to valid records only
        fact_df = df.filter(col('is_valid_record') == True).select(fact_columns)
        
        # Load to fact table
        return self.load_to_timescale(
            fact_df, 
            f"{ETLConfig.DWH_SCHEMA}.{ETLConfig.FACT_TABLE}",
            mode='append'
        )
    
    def load_dimension_tables(self, data):
        """
        Load dimension tables (devices, locations)
        
        Args:
            data: Dictionary of DataFrames
        
        Returns:
            Boolean indicating success
        """
        logger.info("Loading dimension tables...")
        
        success = True
        
        # Load devices dimension
        if data.get('devices') is not None:
            try:
                self.load_to_timescale(
                    data['devices'],
                    f"{ETLConfig.DWH_SCHEMA}.{ETLConfig.DIM_DEVICES_TABLE}",
                    mode='overwrite'  # Overwrite for dimensions
                )
            except Exception as e:
                logger.warning(f"Could not load devices dimension: {e}")
                success = False
        
        # Load locations dimension
        if data.get('locations') is not None:
            try:
                self.load_to_timescale(
                    data['locations'],
                    f"{ETLConfig.DWH_SCHEMA}.{ETLConfig.DIM_LOCATIONS_TABLE}",
                    mode='overwrite'
                )
            except Exception as e:
                logger.warning(f"Could not load locations dimension: {e}")
                success = False
        
        return success
    
    def load_all(self, transformed_df, source_data):
        """
        Load all data to data warehouse
        
        Args:
            transformed_df: Transformed fact data
            source_data: Original source data for dimensions
        
        Returns:
            Boolean indicating success
        """
        logger.info("Starting load phase...")
        
        # Load dimension tables first
        dim_success = self.load_dimension_tables(source_data)
        
        # Load fact table
        fact_success = self.load_fact_table(transformed_df)
        
        if fact_success:
            logger.info("Load phase completed successfully")
        else:
            logger.error("Load phase failed")
        
        return fact_success and dim_success


if __name__ == "__main__":
    """Test loading (requires extracted and transformed data)"""
    from spark_extract import SparkExtractor
    from spark_transform import SparkTransformer
    
    extractor = SparkExtractor()
    transformer = SparkTransformer()
    loader = SparkLoader()
    
    try:
        # Extract data
        logger.info("Extracting data...")
        data = extractor.extract_all_sources()
        
        # Transform data
        logger.info("Transforming data...")
        transformed_df = transformer.transform_all(data)
        
        # Load data
        logger.info("Loading data...")
        success = loader.load_all(transformed_df, data)
        
        if success:
            print("\n" + "="*60)
            print("ETL Pipeline completed successfully!")
            print("="*60)
        else:
            print("\n" + "="*60)
            print("ETL Pipeline completed with warnings")
            print("="*60)
    
    finally:
        extractor.stop()
