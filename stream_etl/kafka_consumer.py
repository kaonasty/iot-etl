"""
Simple Kafka Consumer for Monitoring
Reads and displays messages from Kafka topic
"""

import json
import logging
import argparse
import sys
import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KafkaConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IoTKafkaConsumer:
    """Simple Kafka consumer for monitoring IoT data stream"""
    
    def __init__(self, bootstrap_servers=None, topic=None, group_id=None):
        """
        Initialize Kafka consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers or KafkaConfig.BOOTSTRAP_SERVERS
        self.topic = topic or KafkaConfig.TOPIC_IOT_STREAM
        self.group_id = group_id or KafkaConfig.CONSUMER_GROUP_ID
        
        self.consumer = self._create_consumer()
        self.message_count = 0
        
        logger.info(f"Kafka Consumer initialized (topic={self.topic}, group={self.group_id})")
    
    def _create_consumer(self):
        """Create Kafka consumer instance"""
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=KafkaConfig.AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            logger.info("Kafka consumer created successfully")
            return consumer
        
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise
    
    def consume_messages(self, max_messages=None, timeout_ms=1000):
        """
        Consume messages from Kafka topic
        
        Args:
            max_messages: Maximum number of messages to consume (None for infinite)
            timeout_ms: Timeout for polling in milliseconds
        """
        logger.info(f"Starting to consume messages (max={max_messages})...")
        
        try:
            for message in self.consumer:
                self.message_count += 1
                
                # Parse message
                reading = message.value
                
                # Display message
                self._display_message(message, reading)
                
                # Check if max reached
                if max_messages and self.message_count >= max_messages:
                    logger.info(f"Reached max messages: {max_messages}")
                    break
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        
        finally:
            self.close()
    
    def _display_message(self, message, reading):
        """Display consumed message"""
        print(f"\n{'='*80}")
        print(f"Message #{self.message_count}")
        print(f"{'='*80}")
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key.decode('utf-8') if message.key else None}")
        print(f"Timestamp: {message.timestamp}")
        print(f"\nReading:")
        print(f"  Device: {reading['device_id']} ({reading['device_type']})")
        print(f"  Value: {reading['value']}{reading['unit']}")
        print(f"  Quality: {reading['quality_score']}%")
        print(f"  Anomaly: {reading['is_anomaly']}")
        print(f"  Time: {reading['time']}")
    
    def close(self):
        """Close Kafka consumer"""
        logger.info("Closing Kafka consumer...")
        
        if self.consumer:
            self.consumer.close()
            logger.info(f"Consumer closed. Total messages consumed: {self.message_count}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Kafka Consumer for IoT Data Monitoring')
    parser.add_argument('--max-messages', type=int, default=None,
                       help='Maximum messages to consume (default: infinite)')
    parser.add_argument('--bootstrap-servers', type=str, default=None,
                       help='Kafka bootstrap servers (default: from config)')
    parser.add_argument('--topic', type=str, default=None,
                       help='Kafka topic (default: from config)')
    parser.add_argument('--group-id', type=str, default=None,
                       help='Consumer group ID (default: from config)')
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = IoTKafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id
    )
    
    try:
        # Consume messages
        consumer.consume_messages(max_messages=args.max_messages)
    
    except Exception as e:
        logger.error(f"Consumer failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
