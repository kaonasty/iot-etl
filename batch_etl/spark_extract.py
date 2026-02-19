"""
Batch ETL - Extract Phase
Extracts data from MySQL, PostgreSQL, and TimescaleDB using Apache Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DatabaseConfig, SparkConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkExtractor:
    """Extract data from multiple databases using Spark JDBC"""
    
    def __init__(self, spark_session=None):
        """Initialize Spark session"""
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create and configure Spark session"""
        logger.info("Creating Spark session...")
        
        # Get absolute path to JDBC drivers
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        jdbc_jars = os.path.join(project_root, "jars", "mysql-connector-j-8.2.0.jar") + "," + \
                    os.path.join(project_root, "jars", "postgresql-42.7.1.jar")
        
        spark = (SparkSession.builder
                .appName(SparkConfig.SPARK_APP_NAME + "_Extract")
                .master(SparkConfig.SPARK_MASTER_URL)
                .config("spark.jars", jdbc_jars)
                .config("spark.executor.memory", SparkConfig.EXECUTOR_MEMORY)
                .config("spark.executor.cores", SparkConfig.EXECUTOR_CORES)
                .config("spark.driver.memory", SparkConfig.DRIVER_MEMORY)
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate())
        
        logger.info(f"Spark session created: {spark.version}")
        return spark
    
    def extract_from_mysql(self, table_name, partition_column=None):
        """
        Extract data from MySQL database
        
        Args:
            table_name: Name of the table to extract
            partition_column: Column to use for partitioning (optional)
        
        Returns:
            Spark DataFrame
        """
        logger.info(f"Extracting data from MySQL table: {table_name}")
        
        jdbc_options = {
            "url": DatabaseConfig.get_mysql_jdbc_url(),
            "dbtable": table_name,
            "user": DatabaseConfig.MYSQL_USER,
            "password": DatabaseConfig.MYSQL_PASSWORD,
            "driver": SparkConfig.MYSQL_DRIVER
        }
        
        # Add partitioning for large tables
        if partition_column:
            jdbc_options.update({
                "partitionColumn": partition_column,
                "lowerBound": "1",
                "upperBound": "1000000",
                "numPartitions": "4"
            })
        
        try:
            df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            logger.info(f"Successfully extracted {df.count()} rows from MySQL.{table_name}")
            return df
        except Exception as e:
            logger.error(f"Error extracting from MySQL: {e}")
            raise
    
    def extract_from_postgres(self, table_name, partition_column=None):
        """
        Extract data from PostgreSQL database
        
        Args:
            table_name: Name of the table to extract
            partition_column: Column to use for partitioning (optional)
        
        Returns:
            Spark DataFrame
        """
        logger.info(f"Extracting data from PostgreSQL table: {table_name}")
        
        jdbc_options = {
            "url": DatabaseConfig.get_postgres_jdbc_url(),
            "dbtable": table_name,
            "user": DatabaseConfig.POSTGRES_USER,
            "password": DatabaseConfig.POSTGRES_PASSWORD,
            "driver": SparkConfig.POSTGRES_DRIVER
        }
        
        if partition_column:
            jdbc_options.update({
                "partitionColumn": partition_column,
                "lowerBound": "1",
                "upperBound": "1000000",
                "numPartitions": "4"
            })
        
        try:
            df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            logger.info(f"Successfully extracted {df.count()} rows from PostgreSQL.{table_name}")
            return df
        except Exception as e:
            logger.error(f"Error extracting from PostgreSQL: {e}")
            raise
    
    def extract_from_timescale(self, query, partition_column=None):
        """
        Extract data from TimescaleDB using custom query
        
        Args:
            query: SQL query to execute
            partition_column: Column to use for partitioning (optional)
        
        Returns:
            Spark DataFrame
        """
        logger.info(f"Extracting data from TimescaleDB with query")
        
        jdbc_options = {
            "url": DatabaseConfig.get_timescale_jdbc_url(),
            "dbtable": f"({query}) as subquery",
            "user": DatabaseConfig.TIMESCALE_USER,
            "password": DatabaseConfig.TIMESCALE_PASSWORD,
            "driver": SparkConfig.POSTGRES_DRIVER
        }
        
        if partition_column:
            jdbc_options.update({
                "partitionColumn": partition_column,
                "lowerBound": "1",
                "upperBound": "1000000",
                "numPartitions": "4"
            })
        
        try:
            df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            logger.info(f"Successfully extracted {df.count()} rows from TimescaleDB")
            return df
        except Exception as e:
            logger.error(f"Error extracting from TimescaleDB: {e}")
            raise
    
    def extract_all_sources(self):
        """
        Extract data from all sources for the ETL pipeline
        
        Returns:
            Dictionary of DataFrames
        """
        logger.info("Starting extraction from all data sources...")
        
        data = {}
        
        # Extract device metadata from MySQL
        try:
            data['devices'] = self.extract_from_mysql('devices')
            data['device_types'] = self.extract_from_mysql('device_types')
        except Exception as e:
            logger.warning(f"Could not extract from MySQL: {e}")
            data['devices'] = None
            data['device_types'] = None
        
        # Extract location data from PostgreSQL
        try:
            data['locations'] = self.extract_from_postgres('device_locations')
        except Exception as e:
            logger.warning(f"Could not extract from PostgreSQL: {e}")
            data['locations'] = None
        
        # Extract sensor readings from TimescaleDB (last 7 days)
        sensor_query = """
            SELECT 
                time,
                device_id,
                device_type,
                value,
                unit,
                quality_score,
                is_anomaly,
                metadata
            FROM sensor_readings
            WHERE time > NOW() - INTERVAL '7 days'
        """
        
        try:
            data['sensor_readings'] = self.extract_from_timescale(sensor_query)
        except Exception as e:
            logger.error(f"Could not extract sensor readings: {e}")
            raise
        
        logger.info("Extraction completed successfully")
        return data
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    """Test extraction"""
    extractor = SparkExtractor()
    
    try:
        # Extract all data
        data = extractor.extract_all_sources()
        
        # Display sample data
        for source, df in data.items():
            if df:
                print(f"\n{'='*60}")
                print(f"Source: {source}")
                print(f"{'='*60}")
                df.show(5, truncate=False)
                print(f"Total rows: {df.count()}")
    
    finally:
        extractor.stop()
