"""
Simple test to identify the exact error in batch ETL
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from batch_etl.spark_extract import SparkExtractor
from batch_etl.spark_transform import SparkTransformer

print("="*80)
print("TESTING BATCH ETL - EXTRACT AND TRANSFORM")
print("="*80)

extractor = SparkExtractor()

try:
    # PHASE 1: EXTRACT
    print("\nPHASE 1: EXTRACT")
    print("-"*80)
    data = extractor.extract_all_sources()
    
    print(f"✅ Extracted devices: {data['devices'].count() if data['devices'] else 0} rows")
    print(f"✅ Extracted device_types: {data['device_types'].count() if data['device_types'] else 0} rows")
    print(f"✅ Extracted locations: {data['locations'].count() if data['locations'] else 0} rows")
    print(f"✅ Extracted sensor_readings: {data['sensor_readings'].count() if data['sensor_readings'] else 0} rows")
    
    # Show schemas
    print("\nDevice schema:")
    if data['devices']:
        data['devices'].printSchema()
    
    print("\nSensor readings schema:")
    if data['sensor_readings']:
        data['sensor_readings'].printSchema()
    
    # PHASE 2: TRANSFORM
    print("\nPHASE 2: TRANSFORM")
    print("-"*80)
    transformer = SparkTransformer()
    transformed_df = transformer.transform_all(data)
    
    print(f"✅ Transformed: {transformed_df.count()} rows")
    print("\nTransformed schema:")
    transformed_df.printSchema()
    
    print("\n" + "="*80)
    print("SUCCESS!")
    print("="*80)
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    extractor.stop()
