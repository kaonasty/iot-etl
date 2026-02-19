-- ============================================================================
-- Analytical Queries for IoT Data Warehouse
-- Purpose: Sample queries demonstrating analytical capabilities
-- ============================================================================

-- ============================================================================
-- BASIC QUERIES
-- ============================================================================

-- Query 1: Get latest reading for each device
SELECT * FROM dwh.v_latest_readings ORDER BY device_id;

-- Query 2: Get device health summary (last 24 hours)
SELECT 
    device_id,
    device_type,
    reading_count,
    ROUND(avg_value::numeric, 2) AS avg_value,
    ROUND(avg_quality_score::numeric, 2) AS avg_quality,
    anomaly_count,
    ROUND((anomaly_count::numeric / reading_count * 100), 2) AS anomaly_rate_pct,
    last_reading_time,
    time_since_last_reading
FROM dwh.v_device_health_24h
ORDER BY device_id;

-- Query 3: Get hourly aggregates for specific device (last 24 hours)
SELECT 
    bucket,
    device_id,
    reading_count,
    ROUND(avg_value::numeric, 2) AS avg_value,
    ROUND(min_value::numeric, 2) AS min_value,
    ROUND(max_value::numeric, 2) AS max_value,
    anomaly_count
FROM dwh.fact_hourly_agg
WHERE device_id = 'TEMP-001'
    AND bucket > NOW() - INTERVAL '24 hours'
ORDER BY bucket DESC;

-- ============================================================================
-- TIME-SERIES ANALYSIS
-- ============================================================================

-- Query 4: Daily temperature trends (last 7 days)
SELECT 
    bucket AS day,
    device_id,
    ROUND(avg_value::numeric, 2) AS avg_temp,
    ROUND(min_value::numeric, 2) AS min_temp,
    ROUND(max_value::numeric, 2) AS max_temp,
    ROUND(stddev_value::numeric, 2) AS temp_variation
FROM dwh.fact_daily_agg
WHERE device_type = 'temperature'
    AND bucket > NOW() - INTERVAL '7 days'
ORDER BY bucket DESC, device_id;

-- Query 5: Hour-of-day pattern analysis
SELECT 
    hour_of_day,
    device_id,
    COUNT(*) AS reading_count,
    ROUND(AVG(value)::numeric, 2) AS avg_value,
    ROUND(STDDEV(value)::numeric, 2) AS stddev_value
FROM dwh.fact_sensor_readings_enriched
WHERE device_type = 'temperature'
    AND time > NOW() - INTERVAL '7 days'
GROUP BY hour_of_day, device_id
ORDER BY device_id, hour_of_day;

-- Query 6: Weekend vs Weekday comparison
SELECT 
    device_id,
    is_weekend,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS period,
    COUNT(*) AS reading_count,
    ROUND(AVG(value)::numeric, 2) AS avg_value,
    ROUND(MIN(value)::numeric, 2) AS min_value,
    ROUND(MAX(value)::numeric, 2) AS max_value
FROM dwh.fact_sensor_readings_enriched
WHERE device_type = 'temperature'
    AND time > NOW() - INTERVAL '30 days'
GROUP BY device_id, is_weekend
ORDER BY device_id, is_weekend;

-- ============================================================================
-- ANOMALY ANALYSIS
-- ============================================================================

-- Query 7: Anomaly trends by hour (last 7 days)
SELECT *
FROM dwh.v_anomaly_trends_7d
WHERE
    device_id = 'TEMP-001'
ORDER BY hour DESC
LIMIT 168;
-- 7 days * 24 hours

-- Query 8: Top devices by anomaly rate
SELECT
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
    device_id,
    device_type
HAVING
    COUNT(*) > 100 -- At least 100 readings
ORDER BY anomaly_rate_pct DESC;

-- Query 9: Recent anomalies with context
SELECT 
    time,
    device_id,
    device_type,
    ROUND(value::numeric, 2) AS value,
    unit,
    ROUND(rolling_avg_1h::numeric, 2) AS rolling_avg_1h,
    ROUND((value - rolling_avg_1h)::numeric, 2) AS deviation_from_avg,
    quality_score
FROM dwh.fact_sensor_readings_enriched
WHERE is_anomaly_combined = TRUE
    AND time > NOW() - INTERVAL '24 hours'
ORDER BY time DESC
LIMIT 50;

-- ============================================================================
-- PERFORMANCE ANALYSIS
-- ============================================================================

-- Query 10: Business hours vs Non-business hours comparison
SELECT 
    device_id,
    device_type,
    ROUND(AVG(CASE WHEN is_business_hours THEN value END)::numeric, 2) AS avg_business_hours,
    ROUND(AVG(CASE WHEN NOT is_business_hours THEN value END)::numeric, 2) AS avg_non_business_hours,
    ROUND(
        (AVG(CASE WHEN is_business_hours THEN value END) - 
         AVG(CASE WHEN NOT is_business_hours THEN value END))::numeric, 
        2
    ) AS difference
FROM dwh.fact_sensor_readings_enriched
WHERE time > NOW() - INTERVAL '7 days'
GROUP BY device_id, device_type
ORDER BY device_id;

-- Query 11: Data quality metrics by device
SELECT 
    device_id,
    device_type,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN is_valid_record THEN 1 ELSE 0 END) AS valid_readings,
    ROUND(
        100.0 * SUM(CASE WHEN is_valid_record THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS data_quality_pct,
    ROUND(AVG(quality_score)::numeric, 2) AS avg_quality_score
FROM dwh.fact_sensor_readings_enriched
WHERE time > NOW() - INTERVAL '7 days'
GROUP BY device_id, device_type
ORDER BY data_quality_pct ASC;

-- ============================================================================
-- STREAMING ANALYTICS
-- ============================================================================

-- Query 12: Real-time windowed aggregates (last hour)
SELECT 
    window_start,
    window_end,
    device_id,
    reading_count,
    ROUND(avg_value::numeric, 2) AS avg_value,
    ROUND(min_value::numeric, 2) AS min_value,
    ROUND(max_value::numeric, 2) AS max_value,
    anomaly_count,
    ROUND(avg_latency_seconds::numeric, 2) AS avg_latency_sec
FROM sensor_readings_stream_agg
WHERE window_start > NOW() - INTERVAL '1 hour'
ORDER BY window_start DESC, device_id;

-- Query 13: Streaming pipeline latency analysis
SELECT 
    device_id,
    COUNT(*) AS window_count,
    ROUND(AVG(avg_latency_seconds)::numeric, 2) AS avg_latency_sec,
    ROUND(MIN(avg_latency_seconds)::numeric, 2) AS min_latency_sec,
    ROUND(MAX(avg_latency_seconds)::numeric, 2) AS max_latency_sec
FROM sensor_readings_stream_agg
WHERE window_start > NOW() - INTERVAL '1 hour'
GROUP BY device_id
ORDER BY avg_latency_sec DESC;

-- ============================================================================
-- ADVANCED ANALYTICS
-- ============================================================================

-- Query 14: Moving average comparison (actual vs 1-hour rolling avg)
SELECT 
    time,
    device_id,
    ROUND(value::numeric, 2) AS actual_value,
    ROUND(rolling_avg_1h::numeric, 2) AS rolling_avg_1h,
    ROUND((value - rolling_avg_1h)::numeric, 2) AS deviation,
    CASE 
        WHEN ABS(value - rolling_avg_1h) > 5 THEN 'High Deviation'
        WHEN ABS(value - rolling_avg_1h) > 2 THEN 'Medium Deviation'
        ELSE 'Normal'
    END AS deviation_category
FROM dwh.fact_sensor_readings_enriched
WHERE device_id = 'TEMP-001'
    AND time > NOW() - INTERVAL '24 hours'
    AND rolling_avg_1h IS NOT NULL
ORDER BY time DESC
LIMIT 100;

-- Query 15: Correlation between devices (temperature sensors)
SELECT 
    t1.time_bucket,
    ROUND(t1.avg_value::numeric, 2) AS temp_001_avg,
    ROUND(t2.avg_value::numeric, 2) AS temp_002_avg,
    ROUND((t1.avg_value - t2.avg_value)::numeric, 2) AS temperature_diff
FROM (
    SELECT 
        time_bucket('1 hour', time) AS time_bucket,
        AVG(value) AS avg_value
    FROM dwh.fact_sensor_readings_enriched
    WHERE device_id = 'TEMP-001'
        AND time > NOW() - INTERVAL '24 hours'
    GROUP BY time_bucket
) t1
JOIN (
    SELECT 
        time_bucket('1 hour', time) AS time_bucket,
        AVG(value) AS avg_value
    FROM dwh.fact_sensor_readings_enriched
    WHERE device_id = 'TEMP-002'
        AND time > NOW() - INTERVAL '24 hours'
    GROUP BY time_bucket
) t2 ON t1.time_bucket = t2.time_bucket
ORDER BY t1.time_bucket DESC;

-- ============================================================================
-- HELPER FUNCTION USAGE
-- ============================================================================

-- Query 16: Use helper function to get device statistics
SELECT *
FROM dwh.get_device_stats (
        'TEMP-001', NOW() - INTERVAL '7 days', NOW()
    );

-- ============================================================================
-- DATA EXPORT FOR ML
-- ============================================================================

-- Query 17: Export feature-engineered data for ML (last 30 days)
SELECT
    time,
    device_id,
    device_type,
    value,
    rolling_avg_5min,
    rolling_avg_1h,
    value_lag_1,
    value_lag_2,
    value_change,
    hour_of_day,
    day_of_week,
    is_weekend,
    is_business_hours,
    is_anomaly_combined AS label
FROM dwh.fact_sensor_readings_enriched
WHERE
    time > NOW() - INTERVAL '30 days'
    AND is_valid_record = TRUE
    AND rolling_avg_1h IS NOT NULL
ORDER BY device_id, time;

-- ============================================================================
-- COMPRESSION & STORAGE ANALYSIS
-- ============================================================================

-- Query 18: Check compression statistics
SELECT
    chunk_name,
    pg_size_pretty(before_compression_total_bytes) AS before_compression,
    pg_size_pretty(after_compression_total_bytes) AS after_compression,
    ROUND(
        100 - (after_compression_total_bytes::NUMERIC / before_compression_total_bytes::NUMERIC * 100),
        2
    ) AS compression_ratio_pct
FROM timescaledb_information.compressed_chunk_stats
WHERE hypertable_name = 'fact_sensor_readings_enriched'
ORDER BY chunk_name DESC;

-- Query 19: Table size analysis
SELECT
    hypertable_name,
    pg_size_pretty (total_bytes) AS total_size,
    pg_size_pretty (index_bytes) AS index_size,
    pg_size_pretty (toast_bytes) AS toast_size,
    pg_size_pretty (table_bytes) AS table_size
FROM timescaledb_information.hypertable
WHERE
    hypertable_schema = 'dwh';

-- ============================================================================
-- END OF ANALYTICAL QUERIES
-- ============================================================================