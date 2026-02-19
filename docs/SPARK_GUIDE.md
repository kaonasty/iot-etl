# Apache Spark Guide

## What is Apache Spark?

**Apache Spark** is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python, and R, and an optimized engine that supports general execution graphs.

In this project, we use **PySpark** (Python API) for both batch and stream processing.

---

## Core Concepts

### 1. Resilient Distributed Dataset (RDD)
The fundamental data structure of Spark. It is an immutable distributed collection of objects. RDDs are fault-tolerant and operate in parallel.

### 2. DataFrame
A distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in Python/R, but with richer optimizations under the hood.

### 3. Dataset
Available in Scala and Java, Datasets provide the benefits of RDDs (strong typing, lambda functions) with the benefits of Spark SQL's optimized execution engine.

---

## Spark Architecture in This Project

We run Spark in **Local Mode** for development, but the code is ready for cluster deployment.

### Components:
- **Driver Program**: The process running the `main()` function of the application and creating the `SparkContext`.
- **Executors**: Processes running on worker nodes that execute tasks and store data.
- **Cluster Manager**: Acquires resources on the cluster (e.g., Standalone, Mesos, YARN, Kubernetes).

### Configuration (`config.py`):
```python
SPARK_MASTER_URL = 'local[*]'  # Use all available cores
EXECUTOR_MEMORY = '2g'
DRIVER_MEMORY = '1g'
```

---

## Batch Processing (Spark SQL)

Used in `batch_etl/` to process historical data.

### Key Operations:

1.  **Reading Data (Extract)**
    ```python
    df = spark.read.format("jdbc").options(...).load()
    ```

2.  **Transformations (Lazy Evaluation)**
    - `select()`, `filter()`, `groupBy()`
    - `withColumn()`: Add or replace columns
    - `join()`: Combine DataFrames
    - **Window Functions**:
      ```python
      window_spec = Window.partitionBy("device_id").orderBy("time")
      df.withColumn("prev_value", lag("value").over(window_spec))
      ```

3.  **Actions (Trigger Computation)**
    - `count()`, `show()`, `collect()`
    - `write()`: Save results (`mode="append"`, `mode="overwrite"`)

---

## Stream Processing (Spark Structured Streaming)

Used in `stream_etl/` to process real-time Kafka data.

### Key Concepts:

1.  **Micro-Batch Processing**
    Spark treats a stream as a series of small batch jobs. New data in the stream is treated as new rows appended to an input table.

2.  **Output Modes**
    - **Append**: Only new rows added to the result table.
    - **Complete**: The whole result table is outputted (for aggregations).
    - **Update**: Only rows that were updated are outputted.

3.  **Watermarking**
    Handles late-arriving data by specifying how long to wait before finalizing a window.
    ```python
    .withWatermark("timestamp", "10 minutes")
    ```

4.  **Checkpointing**
    Crucial for fault tolerance. Saves the state of the query to reliable storage (HDFS, S3, local disk) to recover from failures.

### Usage in Project:
- **Source**: Kafka (`format("kafka")`)
- **Sink**: TimescaleDB via JDBC (`foreachBatch`)
- **Logic**: 
  - Read from Kafka
  - Parse JSON
  - Apply 1-minute tumbling windows
  - Write to database

---

## Common Commands

### Submitting a Job
```powershell
# Local python execution (wraps spark-submit in local mode)
python batch_etl/run_batch_pipeline.py

# Production spark-submit
spark-submit \
  --master spark://master:7077 \
  --deploy-mode cluster \
  --conf spark.executor.memory=2g \
  batch_etl/run_batch_pipeline.py
```

### Monitoring
- **Spark UI**: Access at `http://localhost:4040` (driver) or `http://localhost:8080` (master) to view job progress, DAGs, and executor status.

---

## Best Practices

1.  **Avoid `collect()` on large datasets**: It brings all data to the driver, causing OOM errors.
2.  **Partitioning**: Ensure data is evenly distributed using `repartition()`, `coalesce()`.
3.  **Broadcasting**: Use `broadcast()` for joining large tables with small tables to avoid shuffling.
4.  **Caching**: Use `cache()` or `persist()` for DataFrames reused multiple times.

