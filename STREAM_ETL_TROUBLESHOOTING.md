# Stream ETL - Common Issues & Solutions

## Issue 1: Why Only Test Mode Works?

### Problem
- **Test mode** (console output): ✅ Works
- **Database mode** (TimescaleDB): ❌ Fails with JDBC errors

### Root Cause
The streaming consumer was missing JDBC drivers needed to write to PostgreSQL/TimescaleDB.

### Solution
Added JDBC driver configuration to `spark_streaming_consumer.py`:
```python
# Now includes both Kafka connector AND JDBC drivers
.config("spark.jars.packages", kafka_package)
.config("spark.jars", jdbc_jars_str)
```

### Files Fixed
- [spark_streaming_consumer.py](file:///C:/Users/tritr/Documents/Tritronik/etl-learning-project/stream_etl/spark_streaming_consumer.py#L49-L88)

---

## Issue 2: Why Only Batches 1-3 Have Data?

### Problem
```
Batch 1: ✅ Data
Batch 2: ✅ Data  
Batch 3: ✅ Data
Batch 4: ❌ Empty
Batch 5: ❌ Empty
...
```

### Root Cause
**Timing issue** - The consumer starts with `startingOffsets = "latest"`, which means:
1. Consumer starts → Sets offset to END of topic
2. Producer sends last few messages → Consumer catches them (batches 1-3)
3. Producer finishes → No more new messages
4. Consumer keeps running → Empty batches (this is normal!)

### Solution Options

#### Option 1: Start Consumer FIRST (Recommended)
```powershell
# Terminal 1: Start consumer and WAIT for "Started X streaming queries"
python stream_etl/spark_streaming_consumer.py --test-mode

# Terminal 2: THEN start producer
python stream_etl/kafka_producer.py --test-mode
```

Now you'll see data in EVERY batch while the producer is running!

#### Option 2: Read from Beginning
Change line 81 in `spark_streaming_consumer.py`:
```python
.option("startingOffsets", "earliest")  # Read ALL messages from start
```

This reads all 456+ messages already in Kafka!

#### Option 3: Continuous Mode
```powershell
# Both run continuously (no test mode)
python stream_etl/kafka_producer.py  # Runs forever
python stream_etl/spark_streaming_consumer.py  # Runs forever
```

---

## Understanding Kafka Offsets

### `startingOffsets = "latest"` (Current Setting)
```
Kafka Topic: [msg1][msg2][msg3][msg4][msg5]
                                        ↑
Consumer starts here ──────────────────┘
Only sees: NEW messages after this point
```

### `startingOffsets = "earliest"`
```
Kafka Topic: [msg1][msg2][msg3][msg4][msg5]
             ↑
Consumer starts here
Sees: ALL messages from the beginning
```

---

## Empty Batches Are Normal!

Empty batches mean:
- ✅ Consumer is healthy and waiting for data
- ✅ Spark Streaming is checking every few seconds
- ✅ No new messages in Kafka (producer stopped or paused)

This is **expected behavior** in a real-time system!

---

## Testing the Full Pipeline

### Test 1: Console Mode (Easiest)
```powershell
# Terminal 1
python stream_etl/spark_streaming_consumer.py --test-mode

# Terminal 2 (wait for consumer to be ready first!)
python stream_etl/kafka_producer.py --test-mode
```

**Expected:** Data in every batch for 60 seconds, then empty batches

### Test 2: Database Mode (After JDBC Fix)
```powershell
# Make sure tables exist first!
# Run create_stream_tables.sql in DBeaver

# Terminal 1
python stream_etl/spark_streaming_consumer.py

# Terminal 2
python stream_etl/kafka_producer.py
```

**Expected:** Data written to `sensor_readings` and `sensor_readings_stream_agg` tables

### Test 3: Read Historical Data
Change to `startingOffsets = "earliest"` and run:
```powershell
python stream_etl/spark_streaming_consumer.py --test-mode
```

**Expected:** Processes all 456+ messages already in Kafka, then waits for new ones

---

## Quick Diagnostics

### Check Kafka Messages
```powershell
# See how many messages are in the topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic iot-sensor-stream --from-beginning --max-messages 10
```

### Check Database Tables
```sql
-- Count streaming data
SELECT COUNT(*) FROM sensor_readings;
SELECT COUNT(*) FROM sensor_readings_stream_agg;

-- View latest data
SELECT * FROM sensor_readings ORDER BY time DESC LIMIT 5;
SELECT * FROM sensor_readings_stream_agg ORDER BY window_start DESC LIMIT 5;
```

### Clear Checkpoints (If Stuck)
```powershell
Remove-Item -Recurse -Force checkpoints\*
```

---

## Summary

| Mode | Output | Requirements | Status |
|------|--------|--------------|--------|
| Test Mode | Console | None | ✅ Working |
| Database Mode | TimescaleDB | JDBC drivers + tables | ✅ Fixed |
| Simplified Test | Console | None | ✅ Working |

**Both issues are now resolved!** 🎉
