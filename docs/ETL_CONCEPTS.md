# ETL Concepts and Best Practices

## What is ETL?

**ETL** stands for **Extract, Transform, Load** - a data integration process that:

1. **Extract**: Retrieves data from source systems (databases, APIs, files)
2. **Transform**: Cleans, enriches, and reshapes data for analytics
3. **Load**: Writes transformed data to a target system (data warehouse, data lake)

## ETL vs ELT

### ETL (Extract-Transform-Load)
- Transform data **before** loading to warehouse
- Better for complex transformations
- Reduces load on warehouse
- **Example**: Our Spark batch pipeline

```
Source DB → Spark (Transform) → Data Warehouse
```

### ELT (Extract-Load-Transform)
- Load raw data first, transform **in** the warehouse
- Leverages warehouse compute power
- Faster initial load
- **Example**: Modern cloud data warehouses (Snowflake, BigQuery)

```
Source DB → Data Warehouse → Transform (SQL/dbt)
```

## Batch vs Stream Processing

### Batch ETL
- Processes data in **large chunks** at scheduled intervals
- **Latency**: Minutes to hours
- **Use Cases**: Daily reports, historical analysis
- **Tools**: Apache Spark, Apache Airflow

**Example from our project:**
```python
# Run every night at 2 AM
# Extract last 24 hours of data
# Transform and load to warehouse
```

### Stream ETL
- Processes data **continuously** in real-time
- **Latency**: Seconds to milliseconds
- **Use Cases**: Real-time dashboards, alerting, fraud detection
- **Tools**: Apache Kafka, Apache Flink, Spark Structured Streaming

**Example from our project:**
```python
# Continuous processing
# IoT sensor → Kafka → Spark Streaming → Database
# Latency: < 10 seconds
```

## Data Quality Best Practices

### 1. Validation Rules
```python
# Check for null values
df = df.withColumn('has_null_value', 
                  when(col('value').isNull(), lit(True)).otherwise(lit(False)))

# Check for valid ranges
df = df.withColumn('has_invalid_quality', 
                  when((col('quality_score') < 0) | (col('quality_score') > 100), lit(True))
                  .otherwise(lit(False)))
```

### 2. Data Profiling
- **Completeness**: % of non-null values
- **Accuracy**: Values within expected ranges
- **Consistency**: Data matches across sources
- **Timeliness**: Data freshness

### 3. Error Handling
```python
try:
    df = extract_from_source()
except Exception as e:
    logger.error(f"Extraction failed: {e}")
    # Send alert
    # Retry with exponential backoff
    # Write to dead letter queue
```

## Idempotency

**Idempotent operations** produce the same result when run multiple times.

### Why it matters:
- Pipeline failures require re-runs
- Duplicate processing should not corrupt data

### How to achieve:
1. **Upsert instead of Insert**
   ```sql
   INSERT INTO table (id, value)
   VALUES (1, 100)
   ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value;
   ```

2. **Deduplication**
   ```python
   df = df.dropDuplicates(['device_id', 'time'])
   ```

3. **Partition-based loading**
   ```python
   # Overwrite only today's partition
   df.write.mode('overwrite').partitionBy('date').save()
   ```

## Exactly-Once Semantics

Ensures each record is processed **exactly once**, even with failures.

### Challenges:
- Network failures
- Process crashes
- Duplicate messages

### Solutions:

#### 1. Checkpointing (Spark Streaming)
```python
query = stream_df.writeStream \
    .option("checkpointLocation", "./checkpoints") \
    .start()
```

#### 2. Transactional Writes
```python
# Atomic commit - all or nothing
with transaction:
    write_to_database(batch)
    update_offset(kafka_offset)
```

#### 3. Idempotent Producers (Kafka)
```python
producer = KafkaProducer(
    enable_idempotence=True,
    acks='all'
)
```

## Data Partitioning Strategies

### Time-based Partitioning
```sql
-- TimescaleDB hypertables
SELECT create_hypertable(
    'sensor_readings',
    'time',
    chunk_time_interval => INTERVAL '1 day'
);
```

**Benefits:**
- Fast time-range queries
- Easy data retention (drop old partitions)
- Parallel processing

### Hash Partitioning
```python
# Spark - partition by device_id
df.repartition(4, 'device_id')
```

**Benefits:**
- Even data distribution
- Parallel processing by key

## Incremental Loading

Load only **new or changed** data, not full dataset.

### Strategies:

#### 1. Timestamp-based
```sql
SELECT * FROM source_table
WHERE updated_at > :last_load_time
```

#### 2. Change Data Capture (CDC)
- Capture INSERT/UPDATE/DELETE events
- Tools: Debezium, AWS DMS

#### 3. Watermarking
```python
# Spark Streaming - handle late data
df.withWatermark("time", "10 minutes")
```

## Monitoring & Observability

### Key Metrics:

1. **Data Freshness**
   ```sql
   SELECT MAX(time) FROM sensor_readings;
   -- Alert if > 5 minutes old
   ```

2. **Pipeline Latency**
   ```python
   latency = processing_time - event_time
   ```

3. **Data Quality Score**
   ```python
   quality_rate = valid_records / total_records * 100
   ```

4. **Throughput**
   ```
   Records per second
   Bytes per second
   ```

### Alerting:
- Pipeline failures
- Data quality degradation
- Latency spikes
- Missing data

## Schema Evolution

Handling schema changes over time.

### Strategies:

#### 1. Schema Versioning
```python
schema_v1 = StructType([...])
schema_v2 = StructType([...])  # Added new field

if version == 1:
    df = spark.read.schema(schema_v1).json(path)
else:
    df = spark.read.schema(schema_v2).json(path)
```

#### 2. Schema Inference
```python
# Let Spark infer schema
df = spark.read.option("inferSchema", "true").json(path)
```

#### 3. Schema Registry (Kafka)
- Centralized schema management
- Backward/forward compatibility checks

## Testing ETL Pipelines

### Unit Tests
```python
def test_anomaly_detection():
    # Create test data
    test_df = create_test_dataframe()
    
    # Apply transformation
    result_df = detect_anomalies(test_df)
    
    # Assert expected behavior
    assert result_df.filter(col('is_anomaly')).count() == 2
```

### Integration Tests
```python
def test_end_to_end_pipeline():
    # Extract
    data = extractor.extract_all_sources()
    
    # Transform
    transformed = transformer.transform_all(data)
    
    # Load
    success = loader.load_all(transformed, data)
    
    # Verify
    assert success == True
    assert row_count_matches()
```

### Data Quality Tests
```python
def test_data_quality():
    df = load_from_warehouse()
    
    # No nulls in critical columns
    assert df.filter(col('device_id').isNull()).count() == 0
    
    # All values in valid range
    assert df.filter(col('quality_score') > 100).count() == 0
```

## Common ETL Patterns

### 1. Slowly Changing Dimensions (SCD)

**Type 1**: Overwrite old values
```sql
UPDATE dim_devices SET status = 'inactive' WHERE device_id = 'TEMP-001';
```

**Type 2**: Keep history
```sql
-- Add new row with version
INSERT INTO dim_devices (device_id, status, valid_from, valid_to)
VALUES ('TEMP-001', 'inactive', NOW(), NULL);

-- Close previous version
UPDATE dim_devices 
SET valid_to = NOW() 
WHERE device_id = 'TEMP-001' AND valid_to IS NULL;
```

### 2. Star Schema
```
       Fact Table (center)
            /    |    \
           /     |     \
    Dim_Device Dim_Time Dim_Location
```

### 3. Lambda Architecture
- **Batch Layer**: Historical data (Spark batch)
- **Speed Layer**: Real-time data (Spark streaming)
- **Serving Layer**: Merge both for queries

## Performance Optimization

### 1. Predicate Pushdown
```python
# Filter at source, not after loading
df = spark.read.jdbc(url, table, 
    predicates=["date >= '2024-01-01'"])
```

### 2. Partition Pruning
```sql
-- Only scan relevant partitions
SELECT * FROM sensor_readings
WHERE time BETWEEN '2024-01-01' AND '2024-01-02';
```

### 3. Columnar Storage
- Parquet, ORC formats
- Better compression
- Faster analytical queries

### 4. Caching
```python
# Cache frequently accessed data
df.cache()
```

## Security Best Practices

1. **Encryption**
   - At rest: Database encryption
   - In transit: TLS/SSL

2. **Access Control**
   - Principle of least privilege
   - Role-based access control (RBAC)

3. **Secrets Management**
   ```python
   # Don't hardcode credentials
   password = os.getenv('DB_PASSWORD')
   ```

4. **Data Masking**
   ```python
   # Mask PII in non-production
   df = df.withColumn('email', 
       when(env == 'prod', col('email'))
       .otherwise(lit('masked@example.com')))
   ```

## Further Reading

- [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann
- [The Data Warehouse Toolkit](https://www.kimballgroup.com/) by Ralph Kimball
- [Streaming Systems](https://www.oreilly.com/library/view/streaming-systems/9781491983867/) by Tyler Akidau
