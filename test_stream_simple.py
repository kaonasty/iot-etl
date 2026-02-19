"""
Simple test for Spark Streaming Consumer
Minimal version to isolate the issue
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "iot-sensor-stream"

# Sensor schema
SENSOR_SCHEMA = StructType([
    StructField("time", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("quality_score", IntegerType(), True),
    StructField("is_anomaly", BooleanType(), True),
    StructField("metadata", StringType(), True)
])

def main():
    logger.info("Creating Spark session...")
    
    # Create Spark session with Kafka support
    spark = (SparkSession.builder
            .appName("SimpleStreamTest")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
    
    logger.info(f"Spark session created: {spark.version}")
    
    try:
        # Read from Kafka
        logger.info(f"Reading from Kafka topic: {KAFKA_TOPIC}")
        kafka_df = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                   .option("subscribe", KAFKA_TOPIC)
                   .option("startingOffsets", "latest")
                   .option("failOnDataLoss", "false")
                   .load())
        
        logger.info("Kafka stream configured")
        
        # Parse JSON
        parsed_df = (kafka_df
                    .selectExpr("CAST(value AS STRING) as json_value")
                    .select(from_json(col("json_value"), SENSOR_SCHEMA).alias("data"))
                    .select("data.*"))
        
        logger.info("Starting console output...")
        
        # Write to console
        query = (parsed_df
                .writeStream
                .outputMode("append")
                .format("console")
                .option("truncate", False)
                .start())
        
        logger.info("Stream started successfully!")
        logger.info("Press Ctrl+C to stop...")
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
