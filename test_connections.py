"""
Quick test to verify database connections and JDBC drivers
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import DatabaseConfig, SparkConfig
from pyspark.sql import SparkSession

print("="*80)
print("TESTING DATABASE CONNECTIONS AND JDBC DRIVERS")
print("="*80)

# Test configuration
print("\n1. Configuration:")
print(f"   Spark Master: {SparkConfig.SPARK_MASTER_URL}")
print(f"   MySQL: {DatabaseConfig.MYSQL_HOST}:{DatabaseConfig.MYSQL_PORT}")
print(f"   PostgreSQL: {DatabaseConfig.POSTGRES_HOST}:{DatabaseConfig.POSTGRES_PORT}")
print(f"   TimescaleDB: {DatabaseConfig.TIMESCALE_HOST}:{DatabaseConfig.TIMESCALE_PORT}")

# Test JDBC drivers exist
print("\n2. JDBC Drivers:")
project_root = os.path.dirname(os.path.abspath(__file__))
mysql_jar = os.path.join(project_root, "jars", "mysql-connector-j-8.2.0.jar")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.1.jar")

print(f"   MySQL JAR exists: {os.path.exists(mysql_jar)}")
print(f"   PostgreSQL JAR exists: {os.path.exists(postgres_jar)}")

# Test Spark session creation
print("\n3. Creating Spark session...")
try:
    jdbc_jars = mysql_jar + "," + postgres_jar
    
    spark = (SparkSession.builder
            .appName("Test_Connection")
            .master(SparkConfig.SPARK_MASTER_URL)
            .config("spark.jars", jdbc_jars)
            .getOrCreate())
    
    print(f"   ✅ Spark session created: {spark.version}")
    
    # Test MySQL connection
    print("\n4. Testing MySQL connection...")
    try:
        mysql_df = spark.read.format("jdbc").options(
            url=DatabaseConfig.get_mysql_jdbc_url(),
            dbtable="devices",
            user=DatabaseConfig.MYSQL_USER,
            password=DatabaseConfig.MYSQL_PASSWORD,
            driver=SparkConfig.MYSQL_DRIVER
        ).load()
        
        count = mysql_df.count()
        print(f"   ✅ MySQL connected! Found {count} rows in 'devices' table")
    except Exception as e:
        print(f"   ❌ MySQL connection failed: {str(e)[:200]}")
    
    # Test TimescaleDB connection
    print("\n5. Testing TimescaleDB connection...")
    try:
        timescale_df = spark.read.format("jdbc").options(
            url=DatabaseConfig.get_timescale_jdbc_url(),
            dbtable="sensor_readings",
            user=DatabaseConfig.TIMESCALE_USER,
            password=DatabaseConfig.TIMESCALE_PASSWORD,
            driver=SparkConfig.POSTGRES_DRIVER
        ).load()
        
        count = timescale_df.count()
        print(f"   ✅ TimescaleDB connected! Found {count} rows in 'sensor_readings' table")
    except Exception as e:
        print(f"   ❌ TimescaleDB connection failed: {str(e)[:200]}")
    
    # Test PostgreSQL connection
    print("\n6. Testing PostgreSQL connection...")
    try:
        postgres_df = spark.read.format("jdbc").options(
            url=DatabaseConfig.get_postgres_jdbc_url(),
            dbtable="device_locations",
            user=DatabaseConfig.POSTGRES_USER,
            password=DatabaseConfig.POSTGRES_PASSWORD,
            driver=SparkConfig.POSTGRES_DRIVER
        ).load()
        
        count = postgres_df.count()
        print(f"   ✅ PostgreSQL connected! Found {count} rows in 'device_locations' table")
    except Exception as e:
        print(f"   ❌ PostgreSQL connection failed: {str(e)[:200]}")
    
    spark.stop()
    print("\n" + "="*80)
    print("TEST COMPLETED")
    print("="*80)
    
except Exception as e:
    print(f"   ❌ Spark session creation failed: {e}")
    import traceback
    traceback.print_exc()
