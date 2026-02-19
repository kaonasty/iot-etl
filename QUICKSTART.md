# Quick Setup Guide

## ✅ What's Already Running

Based on your existing Docker containers:
- ✅ **MySQL** (`mysql-learn`) - Running
- ✅ **PostgreSQL/TimescaleDB** (`postgres`) - Running on port 5433
- ✅ **Kafka** (`kafka`) - Now running on port 9092
- ✅ **Zookeeper** (`zookeeper`) - Now running on port 2181
- ✅ **Spark** - Running

## 🚀 Quick Start (Using Your Existing Setup)

### 1. Environment Setup (One-time)

```powershell
cd C:\Users\tritr\Documents\Tritronik\etl-learning-project

# Activate virtual environment
.venv\Scripts\Activate.ps1

# Copy and configure .env
cp .env.example .env
```

**Edit `.env` file** with your actual database passwords:
```env
MYSQL_PASSWORD=your_actual_mysql_password
POSTGRES_PASSWORD=your_actual_postgres_password
TIMESCALE_PASSWORD=your_actual_timescale_password
```

### 2. Initialize Data Warehouse (One-time)

Open **DBeaver**, connect to TimescaleDB (port 5433), and run:
```sql
-- Execute: warehouse/create_dwh_schema.sql
```

This creates:
- Dimensional model (fact + dimension tables)
- Continuous aggregates
- Compression and retention policies

### 3. Start Kafka & Zookeeper (If Not Running)

```powershell
# Check if running
docker ps | grep -E "kafka|zookeeper"

# If not running, start them
docker start zookeeper kafka

# Wait 10 seconds for Kafka to be ready
timeout /t 10
```

### 4. Test Batch ETL Pipeline

```powershell
# Run in test mode (processes 1000 rows)
python batch_etl/run_batch_pipeline.py --test-mode
```

**Expected output:**
```
PHASE 1: EXTRACT
Successfully extracted X rows from MySQL.devices
Successfully extracted X rows from PostgreSQL.device_locations
Successfully extracted X rows from TimescaleDB

PHASE 2: TRANSFORM
Data Quality: 95.00% valid records

PHASE 3: LOAD
Successfully loaded to dwh.fact_sensor_readings_enriched

BATCH ETL PIPELINE COMPLETED
Status: SUCCESS
```

**Verify in DBeaver:**
```sql
SELECT COUNT(*) FROM dwh.fact_sensor_readings_enriched;
SELECT * FROM dwh.v_latest_readings;
```

### 5. Test Stream ETL Pipeline

**Terminal 1: Kafka Producer**
```powershell
# Stream IoT data to Kafka (60 seconds in test mode)
python stream_etl/kafka_producer.py --test-mode
```

**Expected output:**
```
INFO:__main__:Running in test mode (60 seconds)
INFO:__main__:Streaming: 10 messages sent, 0 errors
INFO:__main__:Streaming: 20 messages sent, 0 errors
...
Messages Sent: 60
Success Rate: 100.00%
```

**Terminal 2: Spark Streaming Consumer** (Optional - for real-time processing)
```powershell
# Consume from Kafka and write to TimescaleDB
python stream_etl/spark_streaming_consumer.py --test-mode
```

**Terminal 3: Monitor Kafka** (Optional - for debugging)
```powershell
# View messages in Kafka topic
python stream_etl/kafka_consumer.py --max-messages 10
```

**Verify streaming data in DBeaver:**
```sql
-- Check if messages arrived in Kafka topic (via Spark Streaming)
SELECT COUNT(*), MAX(time) 
FROM sensor_readings 
WHERE time > NOW() - INTERVAL '1 minute';

-- Check windowed aggregates
SELECT * FROM sensor_readings_stream_agg
ORDER BY window_start DESC
LIMIT 10;
```

## 📊 Explore Analytics

Once data is loaded, run analytical queries:

```sql
-- Device health summary
SELECT * FROM dwh.v_device_health_24h;

-- Hourly aggregates for TEMP-001
SELECT * FROM dwh.fact_hourly_agg
WHERE device_id = 'TEMP-001'
ORDER BY bucket DESC
LIMIT 24;

-- Anomaly trends
SELECT * FROM dwh.v_anomaly_trends_7d
WHERE device_id = 'TEMP-001';
```

See `warehouse/analytical_queries.sql` for 19 more query examples!

## 🐛 Troubleshooting

### Kafka Connection Error
```
ERROR: Closing connection. Connection to broker lost
```

**Solution:**
```powershell
# Start Kafka and Zookeeper
docker start zookeeper kafka

# Wait 10 seconds
timeout /t 10

# Retry
python stream_etl/kafka_producer.py --test-mode
```

### Database Connection Error
```
ERROR: Could not connect to database
```

**Solution:**
- Check `.env` file has correct passwords
- Verify databases are running: `docker ps`
- Test connection in DBeaver first

### Spark Connection Error
```
ERROR: Could not connect to Spark master
```

**Solution:**
```powershell
# Check Spark is running
docker ps | grep spark

# If not, start from db-learn directory
cd C:\Users\tritr\Documents\Tritronik\db-learn
docker-compose -f docker-compose-spark.yml up -d
```

## 📚 Next Steps

1. **Week 1**: Read `docs/ETL_CONCEPTS.md` and run batch pipeline
2. **Week 2**: Experiment with stream pipeline and Kafka
3. **Week 3**: Explore data warehouse and analytical queries
4. **Week 4**: Build ML models with exported datasets

See `README.md` for the complete 4-week learning path!

## ✅ Verified Working

- ✅ Kafka producer successfully sends 60 messages/minute
- ✅ All Docker services running
- ✅ Batch ETL pipeline ready to run
- ✅ Stream ETL pipeline ready to run
- ✅ Data warehouse schema ready
