# JDBC Drivers Setup for Spark

Spark needs JDBC driver JAR files to connect to MySQL and PostgreSQL databases.

## Quick Fix: Download JDBC Drivers

Run these commands in PowerShell:

```powershell
# Create jars directory
New-Item -ItemType Directory -Force -Path "C:\Users\tritr\Documents\Tritronik\etl-learning-project\jars"

cd C:\Users\tritr\Documents\Tritronik\etl-learning-project\jars

# Download MySQL JDBC Driver
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar" -OutFile "mysql-connector-j-8.2.0.jar"

# Download PostgreSQL JDBC Driver
Invoke-WebRequest -Uri "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar" -OutFile "postgresql-42.7.1.jar"
```

## Then Update Spark Code

Add `--jars` parameter when creating Spark session, or set `PYSPARK_SUBMIT_ARGS` environment variable.

### Option 1: Environment Variable (Recommended)

Add to `.env`:
```env
PYSPARK_SUBMIT_ARGS=--jars C:/Users/tritr/Documents/Tritronik/etl-learning-project/jars/mysql-connector-j-8.2.0.jar,C:/Users/tritr/Documents/Tritronik/etl-learning-project/jars/postgresql-42.7.1.jar pyspark-shell
```

### Option 2: Modify Spark Session Creation

In `batch_etl/spark_extract.py`, update `_create_spark_session`:

```python
spark = (SparkSession.builder
        .appName(SparkConfig.SPARK_APP_NAME + "_Extract")
        .master(SparkConfig.SPARK_MASTER_URL)
        .config("spark.jars", "jars/mysql-connector-j-8.2.0.jar,jars/postgresql-42.7.1.jar")
        .config("spark.executor.memory", SparkConfig.EXECUTOR_MEMORY)
        .config("spark.executor.cores", SparkConfig.EXECUTOR_CORES)
        .config("spark.driver.memory", SparkConfig.DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())
```

## Verify

After setup, run:
```powershell
python batch_etl/run_batch_pipeline.py --test-mode
```

Should see "PHASE 1: EXTRACT" without ClassNotFoundException!
