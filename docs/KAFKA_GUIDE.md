# Apache Kafka Guide

## What is Apache Kafka?

**Apache Kafka** is a distributed event streaming platform capable of handling trillions of events a day. It is used for building real-time data pipelines and streaming apps.

In this project, Kafka acts as the message broker between the **Producer** (IoT Simulator) and the **Consumer** (Spark Streaming).

---

## Core Concepts

### 1. Topic
A category or feed name to which records are published. Topics in Kafka are always multi-subscriber.
- **Config**: `Topic: iot-sensor-stream`

### 2. Partition
Topics are split into partitions. Partitions allow parallel processing and scalability.
- **Order Guarantee**: Only within a partition.
- **Offset**: A unique ID assigned to messages within a partition.

### 3. Producer
Applications that publish (write) data to topics.
- **Config (`kafka_producer.py`)**:
  - `bootstrap.servers`: List of broker addresses.
  - `key.serializer`: Serializer for message keys (devices).
  - `value.serializer`: Serializer for message values (JSON).

### 4. Consumer
Applications that subscribe (read) from topics.
- **Consumer Group**: A group of consumers working together to consume a topic. Each partition is consumed by exactly one consumer per group.
- **Config (`kafka_consumer.py`, `spark_streaming_consumer.py`)**:
  - `group.id`: Identify the consumer group.
  - `auto.offset.reset`: Where to start reading (`earliest` or `latest`).

### 5. Broker
A Kafka server. A cluster consists of multiple brokers to maintain load balance and reliability.

---

## Kafka Architecture in This Project

```
[IoT Simulator] --> (Producer) --> [Kafka Broker] --> (Topic: iot-sensor-stream) --> [Spark Consumer]
```

### Flow:
1.  **Producer**: Generates JSON message: `{"device_id": "TEMP-001", "temp": 22.5, ...}`
2.  **Broker**: Stores message in partition 0 at offset 105.
3.  **Consumer**: Reads offset 105, processes message, commits offset.

---

## Common Commands

### Basic Operations
(Run inside the Kafka container `kafka-etl`)

#### Create Topic
```bash
kafka-topics --create --topic iot-sensor-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### List Topics
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

#### Describe Topic
```bash
kafka-topics --describe --topic iot-sensor-stream --bootstrap-server localhost:9092
```

#### Produce Messages (Console)
```bash
kafka-console-producer --topic iot-sensor-stream --bootstrap-server localhost:9092
> {"message": "hello world"}
```

#### Consume Messages (Console)
```bash
kafka-console-consumer --topic iot-sensor-stream --from-beginning --bootstrap-server localhost:9092
```

---

## Best Practices

1.  **Keyed Messages**: Use keys (e.g., `device_id`) to ensure messages from the same device go to the same partition (order guarantee).
2.  **Idempotence**: Enable `enable.idempotence=true` on producer to prevent duplicate messages.
3.  **Compression**: Use `compression.type='snappy'` or `'gzip'` for high throughput.
4.  **Checkpointing**: Store offsets externally (or use `__consumer_offsets`) to resume processing after failure without data loss.

