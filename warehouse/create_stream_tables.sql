-- Stream ETL Tables for TimescaleDB
-- Run this in DBeaver on the iot_timeseries database

-- Table for windowed aggregations from stream processing
CREATE TABLE IF NOT EXISTS sensor_readings_stream_agg (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    reading_count BIGINT,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    stddev_value DOUBLE PRECISION,
    anomaly_count BIGINT,
    avg_quality_score DOUBLE PRECISION,
    avg_latency_seconds DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        window_start,
        device_id,
        device_type
    )
);

-- Create hypertable for time-series optimization
SELECT create_hypertable (
        'sensor_readings_stream_agg', 'window_start', if_not_exists = > TRUE, migrate_data = > TRUE
    );

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_stream_agg_device_id ON sensor_readings_stream_agg (device_id, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_stream_agg_device_type ON sensor_readings_stream_agg (
    device_type,
    window_start DESC
);

CREATE INDEX IF NOT EXISTS idx_stream_agg_anomaly ON sensor_readings_stream_agg (window_start DESC)
WHERE
    anomaly_count > 0;

-- Enable compression on the hypertable
ALTER TABLE sensor_readings_stream_agg SET(
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id, device_type'
);

-- Add compression policy (compress data older than 7 days)
SELECT add_compression_policy (
        'sensor_readings_stream_agg', INTERVAL '7 days', if_not_exists = > TRUE
    );

-- Add retention policy (keep data for 90 days)
SELECT add_retention_policy (
        'sensor_readings_stream_agg', INTERVAL '90 days', if_not_exists = > TRUE
    );

COMMENT ON
TABLE sensor_readings_stream_agg IS 'Real-time windowed aggregations from Kafka stream processing';