"""
IoT Data Simulator
Generates realistic sensor data for streaming ETL pipeline
"""

import random
import time
import json
from datetime import datetime
import math
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IoTDataSimulator:
    """Simulates realistic IoT sensor data"""
    
    # Device configurations
    DEVICES = [
        {
            'device_id': 'TEMP-001',
            'device_type': 'temperature',
            'unit': '°C',
            'base_value': 22.0,
            'variation': 3.0,
            'anomaly_rate': 0.02
        },
        {
            'device_id': 'TEMP-002',
            'device_type': 'temperature',
            'unit': '°C',
            'base_value': 20.0,
            'variation': 2.5,
            'anomaly_rate': 0.02
        },
        {
            'device_id': 'HUM-001',
            'device_type': 'humidity',
            'unit': '%',
            'base_value': 50.0,
            'variation': 10.0,
            'anomaly_rate': 0.03
        },
        {
            'device_id': 'MOT-001',
            'device_type': 'motion',
            'unit': 'boolean',
            'base_value': 0.0,
            'variation': 1.0,
            'anomaly_rate': 0.0
        },
        {
            'device_id': 'ENR-001',
            'device_type': 'energy',
            'unit': 'kWh',
            'base_value': 100.0,
            'variation': 20.0,
            'anomaly_rate': 0.01
        }
    ]
    
    def __init__(self, seed=None):
        """
        Initialize simulator
        
        Args:
            seed: Random seed for reproducibility
        """
        if seed:
            random.seed(seed)
        
        self.start_time = time.time()
        logger.info(f"IoT Data Simulator initialized with {len(self.DEVICES)} devices")
    
    def _add_daily_pattern(self, base_value, hour):
        """
        Add realistic daily pattern to sensor value
        
        Args:
            base_value: Base sensor value
            hour: Hour of day (0-23)
        
        Returns:
            Adjusted value with daily pattern
        """
        # Sinusoidal pattern (higher during day, lower at night)
        daily_factor = math.sin((hour - 6) * math.pi / 12)  # Peak at 6 PM
        return base_value + (daily_factor * base_value * 0.1)
    
    def _generate_value(self, device_config):
        """
        Generate realistic sensor value
        
        Args:
            device_config: Device configuration dictionary
        
        Returns:
            Generated sensor value
        """
        base_value = device_config['base_value']
        variation = device_config['variation']
        device_type = device_config['device_type']
        
        # Get current hour for daily pattern
        current_hour = datetime.now().hour
        
        # Add daily pattern
        value = self._add_daily_pattern(base_value, current_hour)
        
        # Add random variation
        value += random.uniform(-variation, variation)
        
        # Special handling for motion sensors (binary)
        if device_type == 'motion':
            value = 1.0 if random.random() > 0.7 else 0.0
        
        # Ensure non-negative for certain sensor types
        if device_type in ['humidity', 'energy']:
            value = max(0, value)
        
        # Ensure humidity is within 0-100%
        if device_type == 'humidity':
            value = min(100, value)
        
        return round(value, 2)
    
    def _is_anomaly(self, device_config):
        """
        Determine if this reading should be an anomaly
        
        Args:
            device_config: Device configuration dictionary
        
        Returns:
            Boolean indicating if anomaly
        """
        return random.random() < device_config['anomaly_rate']
    
    def _generate_anomalous_value(self, device_config, normal_value):
        """
        Generate an anomalous sensor value
        
        Args:
            device_config: Device configuration dictionary
            normal_value: Normal value to deviate from
        
        Returns:
            Anomalous value
        """
        # Anomaly is 3-5x the normal variation
        anomaly_factor = random.uniform(3, 5) * device_config['variation']
        
        # Randomly add or subtract
        if random.random() > 0.5:
            return round(normal_value + anomaly_factor, 2)
        else:
            return round(normal_value - anomaly_factor, 2)
    
    def generate_reading(self, device_config=None):
        """
        Generate a single sensor reading
        
        Args:
            device_config: Specific device config, or random if None
        
        Returns:
            Dictionary with sensor reading
        """
        if device_config is None:
            device_config = random.choice(self.DEVICES)
        
        # Generate value
        value = self._generate_value(device_config)
        is_anomaly = self._is_anomaly(device_config)
        
        # If anomaly, modify value
        if is_anomaly:
            value = self._generate_anomalous_value(device_config, value)
        
        # Generate quality score (90-100 normally, lower for anomalies)
        if is_anomaly:
            quality_score = random.randint(70, 90)
        else:
            quality_score = random.randint(90, 100)
        
        # Create reading
        reading = {
            'time': datetime.now().isoformat(),
            'device_id': device_config['device_id'],
            'device_type': device_config['device_type'],
            'value': value,
            'unit': device_config['unit'],
            'quality_score': quality_score,
            'is_anomaly': is_anomaly,
            'metadata': {
                'simulator_version': '1.0',
                'generated_at': datetime.now().isoformat()
            }
        }
        
        return reading
    
    def generate_batch(self, count=10):
        """
        Generate a batch of sensor readings
        
        Args:
            count: Number of readings to generate
        
        Returns:
            List of sensor readings
        """
        readings = []
        for _ in range(count):
            reading = self.generate_reading()
            readings.append(reading)
        
        return readings
    
    def stream_readings(self, interval=1.0, duration=None, callback=None):
        """
        Stream sensor readings continuously
        
        Args:
            interval: Seconds between readings (default 1.0)
            duration: Total duration in seconds (None for infinite)
            callback: Function to call with each reading
        
        Yields:
            Sensor readings
        """
        logger.info(f"Starting data stream (interval={interval}s, duration={duration}s)")
        
        start_time = time.time()
        reading_count = 0
        
        try:
            while True:
                # Check duration
                if duration and (time.time() - start_time) > duration:
                    logger.info(f"Stream duration reached. Generated {reading_count} readings.")
                    break
                
                # Generate reading
                reading = self.generate_reading()
                reading_count += 1
                
                # Call callback if provided
                if callback:
                    callback(reading)
                
                # Yield reading
                yield reading
                
                # Wait for interval
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info(f"Stream interrupted. Generated {reading_count} readings.")
    
    def print_reading(self, reading):
        """Pretty print a sensor reading"""
        print(f"[{reading['time']}] {reading['device_id']}: "
              f"{reading['value']}{reading['unit']} "
              f"(Quality: {reading['quality_score']}%, "
              f"Anomaly: {reading['is_anomaly']})")


if __name__ == "__main__":
    """Test simulator"""
    import argparse
    
    parser = argparse.ArgumentParser(description='IoT Data Simulator')
    parser.add_argument('--interval', type=float, default=1.0,
                       help='Seconds between readings (default: 1.0)')
    parser.add_argument('--duration', type=int, default=60,
                       help='Duration in seconds (default: 60)')
    parser.add_argument('--batch', type=int, default=None,
                       help='Generate batch instead of stream')
    
    args = parser.parse_args()
    
    simulator = IoTDataSimulator()
    
    if args.batch:
        # Generate batch
        logger.info(f"Generating batch of {args.batch} readings...")
        readings = simulator.generate_batch(args.batch)
        
        for reading in readings:
            simulator.print_reading(reading)
            print(json.dumps(reading, indent=2))
    
    else:
        # Stream readings
        logger.info("Starting streaming mode (Ctrl+C to stop)...")
        
        for reading in simulator.stream_readings(
            interval=args.interval,
            duration=args.duration,
            callback=simulator.print_reading
        ):
            pass
