-- ============================================================================
-- Data Warehouse Schema for IoT Analytics
-- Database: TimescaleDB (iot_timeseries)
-- Purpose: Dimensional model for analytical queries and ML feature engineering
-- ============================================================================

-- Create schema for data warehouse
CREATE SCHEMA IF NOT EXISTS dwh;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension: Devices
CREATE TABLE IF NOT EXISTS dwh.dim_devices (
    device_id VARCHAR(50) PRIMARY KEY,
    device_name VARCHAR(100),
    device_type_id INT,
    type_name VARCHAR(50),
    manufacturer VARCHAR(100),
    location_id INT,
    status VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dimension: Locations
CREATE TABLE IF NOT EXISTS dwh.dim_locations (
    device_id VARCHAR(50) PRIMARY KEY,
    building_name VARCHAR(100),
    floor INT,
    room VARCHAR(50),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dimension: Time (for time-based analytics)
CREATE TABLE IF NOT EXISTS dwh.dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20),
    week_of_year INT,
    month INT NOT NULL,
    month_name VARCHAR(20),
    quarter INT,
    year INT NOT NULL,
    is_weekend BOOLEAN,
    is_business_hours BOOLEAN,
    UNIQUE (date, hour)
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- Fact Table: Enriched Sensor Readings
CREATE TABLE IF NOT EXISTS dwh.fact_sensor_readings_enriched (
    time TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit VARCHAR(20),
    quality_score SMALLINT,
    is_anomaly_combined BOOLEAN,

-- Rolling statistics
rolling_avg_5min DOUBLE PRECISION,
rolling_avg_1h DOUBLE PRECISION,
value_lag_1 DOUBLE PRECISION,
value_change DOUBLE PRECISION,

-- Time features
hour_of_day INT,
day_of_week INT,
is_weekend BOOLEAN,
is_business_hours BOOLEAN,

-- Data quality
is_valid_record BOOLEAN,

-- ETL metadata
etl_loaded_at TIMESTAMPTZ DEFAULT NOW() );

-- Convert to hypertable
SELECT create_hypertable (
        'dwh.fact_sensor_readings_enriched', 'time', chunk_time_interval = > INTERVAL '1 day', if_not_exists = > TRUE
    );

-- ============================================================================
-- STREAMING AGGREGATES TABLE
-- ============================================================================

-- Table for real-time windowed aggregates from Spark Streaming
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to hypertable
SELECT create_hypertable (
        'sensor_readings_stream_agg', 'window_start', chunk_time_interval = > INTERVAL '1 day', if_not_exists = > TRUE
    );

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_device_time ON dwh.fact_sensor_readings_enriched (device_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_fact_device_type_time ON dwh.fact_sensor_readings_enriched (device_type, time DESC);

CREATE INDEX IF NOT EXISTS idx_fact_anomaly ON dwh.fact_sensor_readings_enriched (
    is_anomaly_combined,
    time DESC
)
WHERE
    is_anomaly_combined = TRUE;

-- Stream aggregates indexes
CREATE INDEX IF NOT EXISTS idx_stream_agg_device_window ON sensor_readings_stream_agg (device_id, window_start DESC);

-- ============================================================================
-- CONTINUOUS AGGREGATES (Pre-computed aggregations)
-- ============================================================================

-- Hourly aggregates from fact table
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.fact_hourly_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket ('1 hour', time) AS bucket,
    device_id,
    device_type,
    COUNT(*) AS reading_count,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    STDDEV(value) AS stddev_value,
    PERCENTILE_CONT (0.5) WITHIN GROUP (
        ORDER BY value
    ) AS median_value,
    PERCENTILE_CONT (0.95) WITHIN GROUP (
        ORDER BY value
    ) AS p95_value,
    SUM(
        CASE
            WHEN is_anomaly_combined THEN 1
            ELSE 0
        END
    ) AS anomaly_count,
    AVG(quality_score) AS avg_quality_score
FROM dwh.fact_sensor_readings_enriched
GROUP BY
    bucket,
    device_id,
    device_type;

-- Daily aggregates from fact table
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.fact_daily_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket ('1 day', time) AS bucket,
    device_id,
    device_type,
    COUNT(*) AS reading_count,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    STDDEV(value) AS stddev_value,
    PERCENTILE_CONT (0.5) WITHIN GROUP (
        ORDER BY value
    ) AS median_value,
    SUM(
        CASE
            WHEN is_anomaly_combined THEN 1
            ELSE 0
        END
    ) AS anomaly_count,
    AVG(quality_score) AS avg_quality_score,
    -- Day-specific metrics
    AVG(
        CASE
            WHEN is_business_hours THEN value
        END
    ) AS avg_value_business_hours,
    AVG(
        CASE
            WHEN NOT is_business_hours THEN value
        END
    ) AS avg_value_non_business_hours
FROM dwh.fact_sensor_readings_enriched
GROUP BY
    bucket,
    device_id,
    device_type;

-- ============================================================================
-- REFRESH POLICIES
-- ============================================================================

-- Refresh hourly aggregates every hour
SELECT
    add_continuous_aggregate_policy (
        'dwh.fact_hourly_agg',
        start_offset = > INTERVAL '3 hours',
        end_offset = > INTERVAL '1 hour',
        schedule_interval = > INTERVAL '1 hour',
        if_not_exists = > TRUE
    );

-- Refresh daily aggregates every day
SELECT
    add_continuous_aggregate_policy (
        'dwh.fact_daily_agg',
        start_offset = > INTERVAL '3 days',
        end_offset = > INTERVAL '1 day',
        schedule_interval = > INTERVAL '1 day',
        if_not_exists = > TRUE
    );

-- ============================================================================
-- COMPRESSION POLICIES
-- ============================================================================

-- Compress fact table chunks older than 7 days
ALTER TABLE dwh.fact_sensor_readings_enriched SET(
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id, device_type',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy (
        'dwh.fact_sensor_readings_enriched', INTERVAL '7 days', if_not_exists = > TRUE
    );

-- ============================================================================
-- RETENTION POLICIES
-- ============================================================================

-- Keep raw fact data for 90 days
SELECT add_retention_policy (
        'dwh.fact_sensor_readings_enriched', INTERVAL '90 days', if_not_exists = > TRUE
    );

-- Keep stream aggregates for 30 days (raw data is in fact table)
SELECT add_retention_policy (
        'sensor_readings_stream_agg', INTERVAL '30 days', if_not_exists = > TRUE
    );

-- ============================================================================
-- ANALYTICAL VIEWS
-- ============================================================================

-- View: Latest readings per device
CREATE OR REPLACE VIEW dwh.v_latest_readings AS
SELECT DISTINCT
    ON (device_id) device_id,
    device_type,
    time,
    value,
    unit,
    quality_score,
    is_anomaly_combined,
    rolling_avg_1h
FROM dwh.fact_sensor_readings_enriched
ORDER BY device_id, time DESC;

-- View: Device health summary (last 24 hours)
CREATE OR REPLACE VIEW dwh.v_device_health_24h AS
SELECT
    f.device_id,
    f.device_type,
    COUNT(*) AS reading_count,
    AVG(f.value) AS avg_value,
    MIN(f.value) AS min_value,
    MAX(f.value) AS max_value,
    AVG(f.quality_score) AS avg_quality_score,
    SUM(
        CASE
            WHEN f.is_anomaly_combined THEN 1
            ELSE 0
        END
    ) AS anomaly_count,
    MAX(f.time) AS last_reading_time,
    NOW() - MAX(f.time) AS time_since_last_reading
FROM dwh.fact_sensor_readings_enriched f
WHERE
    f.time > NOW() - INTERVAL '24 hours'
GROUP BY
    f.device_id,
    f.device_type;

-- View: Anomaly trends (last 7 days)
CREATE OR REPLACE VIEW dwh.v_anomaly_trends_7d AS
SELECT
    time_bucket ('1 hour', time) AS hour,
    device_id,
    device_type,
    COUNT(*) AS total_readings,
    SUM(
        CASE
            WHEN is_anomaly_combined THEN 1
            ELSE 0
        END
    ) AS anomaly_count,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN is_anomaly_combined THEN 1
                ELSE 0
            END
        ) / COUNT(*),
        2
    ) AS anomaly_rate_pct
FROM dwh.fact_sensor_readings_enriched
WHERE
    time > NOW() - INTERVAL '7 days'
GROUP BY
    hour,
    device_id,
    device_type
ORDER BY hour DESC, device_id;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function: Get device statistics for date range
CREATE OR REPLACE FUNCTION dwh.get_device_stats(
    p_device_id VARCHAR,
    p_start_date TIMESTAMPTZ,
    p_end_date TIMESTAMPTZ
)
RETURNS TABLE (
    total_readings BIGINT,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    stddev_value DOUBLE PRECISION,
    anomaly_count BIGINT,
    anomaly_rate_pct DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT,
        AVG(f.value),
        MIN(f.value),
        MAX(f.value),
        STDDEV(f.value),
        SUM(CASE WHEN f.is_anomaly_combined THEN 1 ELSE 0 END)::BIGINT,
        ROUND(
            100.0 * SUM(CASE WHEN f.is_anomaly_combined THEN 1 ELSE 0 END) / COUNT(*),
            2
        )
    FROM dwh.fact_sensor_readings_enriched f
    WHERE f.device_id = p_device_id
        AND f.time >= p_start_date
        AND f.time <= p_end_date;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

-- Query 1: Get latest reading for each device
-- SELECT * FROM dwh.v_latest_readings;

-- Query 2: Get device health summary
-- SELECT * FROM dwh.v_device_health_24h;

-- Query 3: Get hourly aggregates for a device
-- SELECT * FROM dwh.fact_hourly_agg
-- WHERE device_id = 'TEMP-001'
-- ORDER BY bucket DESC
-- LIMIT 24;

-- Query 4: Get anomaly trends
-- SELECT * FROM dwh.v_anomaly_trends_7d
-- WHERE device_id = 'TEMP-001'
-- LIMIT 168;  -- 7 days * 24 hours

-- Query 5: Compare business hours vs non-business hours
-- SELECT
--     device_id,
--     AVG(CASE WHEN is_business_hours THEN value END) AS avg_business_hours,
--     AVG(CASE WHEN NOT is_business_hours THEN value END) AS avg_non_business_hours
-- FROM dwh.fact_sensor_readings_enriched
-- WHERE time > NOW() - INTERVAL '7 days'
-- GROUP BY device_id;

-- ============================================================================
-- END OF DATA WAREHOUSE SCHEMA
-- ============================================================================