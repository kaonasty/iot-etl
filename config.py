"""
Configuration Management for ETL Learning Project
Loads environment variables and provides database connection strings
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig:
    """Database connection configurations"""
    
    # MySQL Configuration
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'iot_devices')
    
    @staticmethod
    def get_mysql_connection_string():
        """Get MySQL connection string for SQLAlchemy"""
        return (f"mysql+pymysql://{DatabaseConfig.MYSQL_USER}:"
                f"{DatabaseConfig.MYSQL_PASSWORD}@{DatabaseConfig.MYSQL_HOST}:"
                f"{DatabaseConfig.MYSQL_PORT}/{DatabaseConfig.MYSQL_DATABASE}")
    
    @staticmethod
    def get_mysql_jdbc_url():
        """Get MySQL JDBC URL for Spark"""
        return (f"jdbc:mysql://{DatabaseConfig.MYSQL_HOST}:"
                f"{DatabaseConfig.MYSQL_PORT}/{DatabaseConfig.MYSQL_DATABASE}")
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5433))
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'iot_locations')
    
    @staticmethod
    def get_postgres_connection_string():
        """Get PostgreSQL connection string for SQLAlchemy"""
        return (f"postgresql://{DatabaseConfig.POSTGRES_USER}:"
                f"{DatabaseConfig.POSTGRES_PASSWORD}@{DatabaseConfig.POSTGRES_HOST}:"
                f"{DatabaseConfig.POSTGRES_PORT}/{DatabaseConfig.POSTGRES_DATABASE}")
    
    @staticmethod
    def get_postgres_jdbc_url():
        """Get PostgreSQL JDBC URL for Spark"""
        return (f"jdbc:postgresql://{DatabaseConfig.POSTGRES_HOST}:"
                f"{DatabaseConfig.POSTGRES_PORT}/{DatabaseConfig.POSTGRES_DATABASE}")
    
    # TimescaleDB Configuration
    TIMESCALE_HOST = os.getenv('TIMESCALE_HOST', 'localhost')
    TIMESCALE_PORT = int(os.getenv('TIMESCALE_PORT', 5432))
    TIMESCALE_USER = os.getenv('TIMESCALE_USER', 'postgres')
    TIMESCALE_PASSWORD = os.getenv('TIMESCALE_PASSWORD', '')
    TIMESCALE_DATABASE = os.getenv('TIMESCALE_DATABASE', 'iot_timeseries')
    
    @staticmethod
    def get_timescale_connection_string():
        """Get TimescaleDB connection string for SQLAlchemy"""
        return (f"postgresql://{DatabaseConfig.TIMESCALE_USER}:"
                f"{DatabaseConfig.TIMESCALE_PASSWORD}@{DatabaseConfig.TIMESCALE_HOST}:"
                f"{DatabaseConfig.TIMESCALE_PORT}/{DatabaseConfig.TIMESCALE_DATABASE}")
    
    @staticmethod
    def get_timescale_jdbc_url():
        """Get TimescaleDB JDBC URL for Spark"""
        return (f"jdbc:postgresql://{DatabaseConfig.TIMESCALE_HOST}:"
                f"{DatabaseConfig.TIMESCALE_PORT}/{DatabaseConfig.TIMESCALE_DATABASE}"
                f"?stringtype=unspecified")


class SparkConfig:
    """Apache Spark configurations"""
    
    # Use local mode for development/learning (all cores)
    # For cluster mode, use: spark://localhost:7077
    SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'local[*]')
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'IoT_ETL_Pipeline')
    
    # Spark performance tuning
    EXECUTOR_MEMORY = '2g'
    EXECUTOR_CORES = 2
    DRIVER_MEMORY = '1g'
    
    # JDBC Driver paths (update these based on your setup)
    MYSQL_DRIVER = 'com.mysql.cj.jdbc.Driver'
    POSTGRES_DRIVER = 'org.postgresql.Driver'


class KafkaConfig:
    """Apache Kafka configurations"""
    
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC_IOT_STREAM = os.getenv('KAFKA_TOPIC_IOT_STREAM', 'iot-sensor-stream')
    
    # Consumer group
    CONSUMER_GROUP_ID = 'iot-etl-consumer-group'
    
    # Producer settings
    PRODUCER_ACKS = 'all'  # Wait for all replicas
    PRODUCER_RETRIES = 3
    
    # Consumer settings
    AUTO_OFFSET_RESET = 'earliest'  # Start from beginning if no offset
    ENABLE_AUTO_COMMIT = False  # Manual commit for exactly-once semantics


class ETLConfig:
    """ETL pipeline configurations"""
    
    # Batch processing
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    
    # Stream processing
    STREAM_WINDOW_SECONDS = int(os.getenv('STREAM_WINDOW_SECONDS', 5))
    
    # Data quality
    DATA_QUALITY_THRESHOLD = float(os.getenv('DATA_QUALITY_THRESHOLD', 0.95))
    
    # Anomaly detection
    ANOMALY_Z_SCORE_THRESHOLD = 3.0
    ANOMALY_IQR_MULTIPLIER = 1.5
    
    # Data warehouse schema
    DWH_SCHEMA = 'dwh'
    FACT_TABLE = 'fact_sensor_readings_enriched'
    DIM_DEVICES_TABLE = 'dim_devices'
    DIM_LOCATIONS_TABLE = 'dim_locations'
    DIM_TIME_TABLE = 'dim_time'


# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'colored': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'log_colors': {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'colored',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'etl_pipeline.log',
            'formatter': 'colored',
            'level': 'DEBUG',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
}
