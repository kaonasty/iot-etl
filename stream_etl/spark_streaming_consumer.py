"""
Spark Structured Streaming Consumer
Consumes IoT data from Kafka and processes in near real-time
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, sum as spark_sum,
    stddev, max as spark_max, min as spark_min, current_timestamp,
    when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
import logging
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DatabaseConfig, SparkConfig, KafkaConfig, ETLConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkStreamingConsumer:
    """Spark Structured Streaming consumer for IoT data"""
    
    # Define schema for incoming JSON messages
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
    
    def __init__(self):
        """Initialize Spark Streaming session"""
        self.spark = self._create_spark_session()
        logger.info("Spark Structured Streaming consumer initialized")
    
    def _create_spark_session(self):
        """Create Spark session with Kafka support"""
        logger.info("Creating Spark session with Kafka support...")
        
        # Kafka connector package for Spark 3.5.0
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        
        # Get JDBC driver paths
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        jars_dir = os.path.join(project_root, "jars")
        
        jdbc_jars = []
        if os.path.exists(jars_dir):
            for jar_file in os.listdir(jars_dir):
                if jar_file.endswith('.jar'):
                    jdbc_jars.append(os.path.join(jars_dir, jar_file))
        
        jdbc_jars_str = ",".join(jdbc_jars) if jdbc_jars else ""
        
        spark_builder = (SparkSession.builder
                .appName(SparkConfig.SPARK_APP_NAME + "_Streaming")
                .master(SparkConfig.SPARK_MASTER_URL)
                .config("spark.jars.packages", kafka_package))
        
        # Add JDBC drivers if found
        if jdbc_jars_str:
            spark_builder = spark_builder.config("spark.jars", jdbc_jars_str)
            logger.info(f"Added JDBC drivers: {len(jdbc_jars)} JAR files")
            for jar in jdbc_jars:
                 logger.info(f"  - {os.path.basename(jar)}")
        else:
            logger.warning("No JDBC drivers found in 'jars' directory! Database writes will fail.")
        
        spark = (spark_builder
                .config("spark.executor.memory", SparkConfig.EXECUTOR_MEMORY)
                .config("spark.executor.cores", SparkConfig.EXECUTOR_CORES)
                .config("spark.driver.memory", SparkConfig.DRIVER_MEMORY)
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .getOrCreate())
        
        logger.info(f"Spark session created: {spark.version}")
        return spark
    
    def read_from_kafka(self):
        """
        Read streaming data from Kafka
        
        Returns:
            Streaming DataFrame
        """
        logger.info(f"Reading from Kafka topic: {KafkaConfig.TOPIC_IOT_STREAM}")
        
        # Read from Kafka
        kafka_df = (self.spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS)
                   .option("subscribe", KafkaConfig.TOPIC_IOT_STREAM)
                   .option("startingOffsets", "earliest")  # Read all data from beginning
                   .option("failOnDataLoss", "false")
                   .load())
        
        # Parse JSON messages
        parsed_df = (kafka_df
                    .selectExpr("CAST(value AS STRING) as json_value",
                               "CAST(key AS STRING) as device_id_key",
                               "timestamp as kafka_timestamp")
                    .select(
                        from_json(col("json_value"), self.SENSOR_SCHEMA).alias("data"),
                        col("device_id_key"),
                        col("kafka_timestamp")
                    )
                    .select("data.*", "kafka_timestamp"))
        
        # Convert time string to timestamp
        parsed_df = parsed_df.withColumn("time", col("time").cast(TimestampType()))
        
        logger.info("Kafka stream configured successfully")
        return parsed_df
    
    def apply_transformations(self, stream_df):
        """
        Apply real-time transformations to streaming data
        
        Args:
            stream_df: Streaming DataFrame
        
        Returns:
            Transformed streaming DataFrame
        """
        logger.info("Applying real-time transformations...")
        
        # Add processing timestamp
        stream_df = stream_df.withColumn("processed_at", current_timestamp())
        
        # Data quality checks
        stream_df = stream_df.withColumn(
            "is_valid",
            when((col("value").isNotNull()) & 
                 (col("quality_score") >= 0) & 
                 (col("quality_score") <= 100), lit(True))
            .otherwise(lit(False))
        )
        
        # Calculate latency (processing time - event time)
        stream_df = stream_df.withColumn(
            "latency_seconds",
            (col("processed_at").cast("long") - col("time").cast("long"))
        )
        
        return stream_df
    
    def create_windowed_aggregates(self, stream_df):
        """
        Create windowed aggregations (1-min, 5-min windows)
        
        Args:
            stream_df: Streaming DataFrame
        
        Returns:
            Aggregated streaming DataFrame
        """
        logger.info("Creating windowed aggregations...")
        
        # 1-minute tumbling window aggregations
        windowed_agg = (stream_df
                       .filter(col("is_valid") == True)
                       .groupBy(
                           window(col("time"), "1 minute"),
                           col("device_id"),
                           col("device_type")
                       )
                       .agg(
                           count("*").alias("reading_count"),
                           avg("value").alias("avg_value"),
                           spark_min("value").alias("min_value"),
                           spark_max("value").alias("max_value"),
                           stddev("value").alias("stddev_value"),
                           spark_sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
                           avg("quality_score").alias("avg_quality_score"),
                           avg("latency_seconds").alias("avg_latency_seconds")
                       ))
        
        # Flatten window struct
        windowed_agg = windowed_agg.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            col("device_type"),
            col("reading_count"),
            col("avg_value"),
            col("min_value"),
            col("max_value"),
            col("stddev_value"),
            col("anomaly_count"),
            col("avg_quality_score"),
            col("avg_latency_seconds")
        )
        
        return windowed_agg
    
    def write_to_timescale(self, stream_df, table_name, checkpoint_location):
        """
        Write streaming data to TimescaleDB
        
        Args:
            stream_df: Streaming DataFrame
            table_name: Target table name
            checkpoint_location: Checkpoint directory for fault tolerance
        
        Returns:
            Streaming query
        """
        logger.info(f"Writing stream to TimescaleDB table: {table_name}")
        
        jdbc_url = DatabaseConfig.get_timescale_jdbc_url()
        
        def write_batch_to_postgres(batch_df, batch_id):
            """Write each micro-batch to PostgreSQL"""
            if batch_df.count() > 0:
                logger.info(f"Writing batch {batch_id} with {batch_df.count()} rows to {table_name}")
                
                batch_df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", DatabaseConfig.TIMESCALE_USER) \
                    .option("password", DatabaseConfig.TIMESCALE_PASSWORD) \
                    .option("driver", SparkConfig.POSTGRES_DRIVER) \
                    .mode("append") \
                    .save()
        
        # Write stream using foreachBatch
        # Note: Use "update" mode for aggregations, "append" for raw data
        output_mode = "update" if "agg" in table_name.lower() else "append"
        
        query = (stream_df
                .writeStream
                .foreachBatch(write_batch_to_postgres)
                .outputMode(output_mode)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime=f"{ETLConfig.STREAM_WINDOW_SECONDS} seconds")
                .start())
        
        logger.info(f"Streaming query started (checkpoint: {checkpoint_location})")
        return query
    
    def write_to_console(self, stream_df, truncate=False):
        """
        Write streaming data to console for debugging
        
        Args:
            stream_df: Streaming DataFrame
            truncate: Whether to truncate output
        
        Returns:
            Streaming query
        """
        logger.info("Writing stream to console...")
        
        query = (stream_df
                .writeStream
                .outputMode("append")
                .format("console")
                .option("truncate", truncate)
                .trigger(processingTime=f"{ETLConfig.STREAM_WINDOW_SECONDS} seconds")
                .start())
        
        return query
    
    def run_streaming_pipeline(self, output_mode="database", debug=False):
        """
        Run the complete streaming ETL pipeline
        
        Args:
            output_mode: "database", "console", or "both"
            debug: Enable debug output
        
        Returns:
            List of streaming queries
        """
        logger.info("="*80)
        logger.info("STARTING SPARK STRUCTURED STREAMING PIPELINE")
        logger.info("="*80)
        
        queries = []
        
        try:
            # Read from Kafka
            stream_df = self.read_from_kafka()
            
            # Apply transformations
            transformed_df = self.apply_transformations(stream_df)
            
            # Write raw data to database
            if output_mode in ["database", "both"]:
                # Select only columns that match SENSOR_SCHEMA
                raw_df = transformed_df.filter(col("is_valid") == True).select(
                    "time", "device_id", "device_type", "value", 
                    "unit", "quality_score", "is_anomaly", "metadata"
                )
                
                raw_query = self.write_to_timescale(
                    raw_df,
                    "sensor_readings",
                    "./checkpoints/raw_data"
                )
                queries.append(raw_query)
            
            # Create and write windowed aggregates
            windowed_df = self.create_windowed_aggregates(transformed_df)
            
            if output_mode in ["database", "both"]:
                agg_query = self.write_to_timescale(
                    windowed_df,
                    "sensor_readings_stream_agg",
                    "./checkpoints/windowed_agg"
                )
                queries.append(agg_query)
            
            # Debug output to console
            if debug or output_mode in ["console", "both"]:
                console_query = self.write_to_console(transformed_df.limit(10))
                queries.append(console_query)
            
            logger.info(f"Started {len(queries)} streaming queries")
            
            # Wait for termination
            for query in queries:
                query.awaitTermination()
        
        except KeyboardInterrupt:
            logger.info("Streaming pipeline interrupted by user")
        
        except Exception as e:
            logger.error(f"Streaming pipeline failed: {e}", exc_info=True)
            raise
        
        finally:
            self.stop_all_queries(queries)
    
    def stop_all_queries(self, queries):
        """Stop all streaming queries"""
        logger.info("Stopping all streaming queries...")
        
        for query in queries:
            if query and query.isActive:
                query.stop()
        
        logger.info("All queries stopped")
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    """Run streaming consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Spark Structured Streaming Consumer')
    parser.add_argument('--output-mode', type=str, default='database',
                       choices=['database', 'console', 'both'],
                       help='Output mode (default: database)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug output')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode (console output only)')
    
    args = parser.parse_args()
    
    # Test mode overrides
    if args.test_mode:
        args.output_mode = 'console'
        args.debug = True
    
    # Create and run consumer
    consumer = SparkStreamingConsumer()
    
    try:
        consumer.run_streaming_pipeline(
            output_mode=args.output_mode,
            debug=args.debug
        )
    
    except Exception as e:
        logger.error(f"Consumer failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        consumer.stop()
