"""
Kafka Producer for IoT Sensor Data
Publishes sensor readings to Kafka topic
"""

import json
import logging
import argparse
import sys
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KafkaConfig
from stream_etl.iot_simulator import IoTDataSimulator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IoTKafkaProducer:
    """Kafka producer for IoT sensor data"""
    
    def __init__(self, bootstrap_servers=None, topic=None):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
        """
        self.bootstrap_servers = bootstrap_servers or KafkaConfig.BOOTSTRAP_SERVERS
        self.topic = topic or KafkaConfig.TOPIC_IOT_STREAM
        
        self.producer = self._create_producer()
        self.message_count = 0
        self.error_count = 0
        
        logger.info(f"Kafka Producer initialized (servers={self.bootstrap_servers}, topic={self.topic})")
    
    def _create_producer(self):
        """Create Kafka producer instance"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=KafkaConfig.PRODUCER_ACKS,
                retries=KafkaConfig.PRODUCER_RETRIES,
                compression_type='gzip',  # Compress messages
                linger_ms=10,  # Batch messages for 10ms
                batch_size=16384  # 16KB batch size
            )
            
            logger.info("Kafka producer created successfully")
            return producer
        
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def _on_send_success(self, record_metadata):
        """Callback for successful message send"""
        self.message_count += 1
        
        if self.message_count % 100 == 0:
            logger.info(f"Sent {self.message_count} messages "
                       f"(topic={record_metadata.topic}, "
                       f"partition={record_metadata.partition}, "
                       f"offset={record_metadata.offset})")
    
    def _on_send_error(self, exception):
        """Callback for failed message send"""
        self.error_count += 1
        logger.error(f"Error sending message: {exception}")
    
    def send_reading(self, reading):
        """
        Send a single sensor reading to Kafka
        
        Args:
            reading: Sensor reading dictionary
        
        Returns:
            Future object
        """
        try:
            # Send message asynchronously
            future = self.producer.send(
                self.topic,
                value=reading,
                key=reading['device_id'].encode('utf-8')  # Partition by device_id
            )
            
            # Add callbacks
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return future
        
        except Exception as e:
            logger.error(f"Error sending reading: {e}")
            self.error_count += 1
            raise
    
    def stream_from_simulator(self, interval=1.0, duration=None):
        """
        Stream data from IoT simulator to Kafka
        
        Args:
            interval: Seconds between readings
            duration: Total duration in seconds (None for infinite)
        """
        logger.info(f"Starting stream from simulator (interval={interval}s, duration={duration}s)")
        if duration is None:
            logger.info("Running in infinite mode. Press Ctrl+C to stop.")
        
        simulator = IoTDataSimulator()
        
        try:
            for reading in simulator.stream_readings(interval=interval, duration=duration):
                # Send to Kafka
                self.send_reading(reading)
                
                # Print progress
                if self.message_count % 10 == 0:
                    logger.info(f"Streaming: {self.message_count} messages sent, "
                               f"{self.error_count} errors")
        
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        
        finally:
            self.close()
    
    def close(self):
        """Close Kafka producer and flush pending messages"""
        logger.info("Closing Kafka producer...")
        
        if self.producer:
            # Flush pending messages
            self.producer.flush()
            
            # Close producer
            self.producer.close()
            
            logger.info(f"Producer closed. Total messages sent: {self.message_count}, "
                       f"Errors: {self.error_count}")
    
    def get_stats(self):
        """Get producer statistics"""
        return {
            'messages_sent': self.message_count,
            'errors': self.error_count,
            'success_rate': (self.message_count / (self.message_count + self.error_count) * 100) 
                           if (self.message_count + self.error_count) > 0 else 0
        }


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Kafka Producer for IoT Data')
    parser.add_argument('--interval', type=float, default=1.0,
                       help='Seconds between readings (default: 1.0)')
    parser.add_argument('--duration', type=int, default=None,
                       help='Duration in seconds (default: infinite)')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode (60 seconds)')
    parser.add_argument('--bootstrap-servers', type=str, default=None,
                       help='Kafka bootstrap servers (default: from config)')
    parser.add_argument('--topic', type=str, default=None,
                       help='Kafka topic (default: from config)')
    
    args = parser.parse_args()
    
    # Test mode settings
    if args.test_mode:
        args.duration = 60
        logger.info("Running in test mode (60 seconds)")
    
    # Create producer
    producer = IoTKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    try:
        # Stream data
        producer.stream_from_simulator(
            interval=args.interval,
            duration=args.duration
        )
        
        # Print final stats
        stats = producer.get_stats()
        print("\n" + "="*60)
        print("PRODUCER STATISTICS")
        print("="*60)
        print(f"Messages Sent: {stats['messages_sent']}")
        print(f"Errors: {stats['errors']}")
        print(f"Success Rate: {stats['success_rate']:.2f}%")
        print("="*60)
    
    except Exception as e:
        logger.error(f"Producer failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
