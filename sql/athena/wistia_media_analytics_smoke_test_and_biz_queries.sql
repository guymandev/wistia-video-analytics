-- Confirm databases and tables

SHOW DATABASES;

SHOW TABLES in wistia_gold;

SHOW TABLES in wistia_marts;

-- Inspect schemas

DESCRIBE wistia_gold.dim_media;

DESCRIBE wistia_gold.fact_media_engagement;

DESCRIBE wistia_marts.mart_channel_performance;

-- Gold table row-count smoke tests

SELECT 'dim_media' AS table_name, COUNT(*) AS row_count
FROM wistia_gold.dim_media

UNION ALL

SELECT 'dim_visitor' AS table_name, COUNT(*) AS row_count
FROM wistia_gold.dim_visitor

UNION ALL

SELECT 'fact_media_daily_stats' AS table_name, COUNT(*) AS row_count
FROM wistia_gold.fact_media_daily_stats

UNION ALL

SELECT 'fact_media_engagement' AS table_name, COUNT(*) AS row_count
FROM wistia_gold.fact_media_engagement;

-- Mart table-row smoke tests

SELECT 'mart_channel_performance' AS table_name, COUNT(*) AS row_count
FROM wistia_marts.mart_channel_performance

UNION ALL

SELECT 'mart_geo_engagement' AS table_name, COUNT(*) AS row_count
FROM wistia_marts.mart_geo_engagement

UNION ALL

SELECT 'mart_device_browser' AS table_name, COUNT(*) AS row_count
FROM wistia_marts.mart_device_browser

UNION ALL

SELECT 'mart_visitor_engagement' AS table_name, COUNT(*) AS row_count
FROM wistia_marts.mart_visitor_engagement;

-- IMPORTANT
-- mart_channel_performance is snapshot-based and contains one row per channel per snapshot_date. 
-- Because Wistia media stats are cumulative as of ingestion time, dashboard queries should filter to a single snapshot_date, 
-- typically the latest available snapshot, unless intentionally analyzing snapshot history over time.

-- Given the fact that we can have multiple ingest_dates
-- this query is a more refined check for mart_channel_performance

SELECT
    snapshot_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT channel) AS channel_count
FROM wistia_marts.mart_channel_performance
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- Latest snapshot-only row count

WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM wistia_marts.mart_channel_performance
)
SELECT
    'mart_channel_performance_latest' AS table_name,
    COUNT(*) AS row_count
FROM wistia_marts.mart_channel_performance m
JOIN latest_snapshot l
    ON m.snapshot_date = l.snapshot_date;

-- Latest channel performance query

WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM wistia_marts.mart_channel_performance
)
SELECT
    m.snapshot_date,
    m.channel,
    m.media_count,
    m.total_load_count,
    m.total_play_count,
    m.avg_api_play_rate,
    m.total_hours_watched,
    m.avg_engagement,
    m.engagement_event_count,
    m.unique_event_visitors,
    m.avg_percent_viewed,
    m.completed_view_count,
    m.completion_rate,
    m.mobile_event_rate
FROM wistia_marts.mart_channel_performance m
JOIN latest_snapshot l
    ON m.snapshot_date = l.snapshot_date
ORDER BY m.channel;

-- Confirm gold media/channel mapping

SELECT
    media_hashed_id,
    channel,
    media_name,
    duration_seconds,
    status,
    ingest_date
FROM wistia_gold.dim_media
ORDER BY channel;

-- check fact engagement has no duplicate event keys

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_key) AS distinct_event_keys,
    COUNT(*) - COUNT(DISTINCT event_key) AS duplicate_event_key_count
FROM wistia_gold.fact_media_engagement;

- check for missing critical keys

SELECT
    SUM(CASE WHEN event_key IS NULL THEN 1 ELSE 0 END) AS null_event_keys,
    SUM(CASE WHEN media_hashed_id IS NULL THEN 1 ELSE 0 END) AS null_media_ids,
    SUM(CASE WHEN channel IS NULL THEN 1 ELSE 0 END) AS null_channels,
    SUM(CASE WHEN visitor_key IS NULL THEN 1 ELSE 0 END) AS null_visitor_keys,
    SUM(CASE WHEN percent_viewed IS NULL THEN 1 ELSE 0 END) AS null_percent_viewed
FROM wistia_gold.fact_media_engagement;

-- confirm channel distribution in event fact

SELECT
    channel,
    media_hashed_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT visitor_key) AS unique_visitors,
    AVG(percent_viewed) AS avg_percent_viewed,
    MAX(percent_viewed) AS max_percent_viewed
FROM wistia_gold.fact_media_engagement
GROUP BY channel, media_hashed_id
ORDER BY channel;

-- validate daily stats fact

SELECT
    snapshot_date,
    channel,
    media_hashed_id,
    load_count,
    play_count,
    api_play_rate,
    calculated_play_rate,
    hours_watched,
    engagement,
    visitor_count
FROM wistia_gold.fact_media_daily_stats
ORDER BY channel;

-- compare FB vs YouTube performance from mart

SELECT
    snapshot_date,
    channel,
    media_count,
    total_load_count,
    total_play_count,
    avg_api_play_rate,
    total_hours_watched,
    avg_engagement,
    engagement_event_count,
    unique_event_visitors,
    avg_percent_viewed,
    completed_view_count,
    completion_rate,
    mobile_event_rate
FROM wistia_marts.mart_channel_performance
ORDER BY channel;

-- top geographies by engagement events

SELECT
    country,
    region,
    city,
    channel,
    engagement_event_count,
    unique_visitors,
    avg_percent_viewed,
    completed_view_count,
    completion_rate
FROM wistia_marts.mart_geo_engagement
ORDER BY engagement_event_count DESC, unique_visitors DESC
LIMIT 20;

-- device/browser breakdown

SELECT
    platform,
    browser,
    browser_version,
    is_mobile,
    channel,
    engagement_event_count,
    unique_visitors,
    avg_percent_viewed,
    completed_view_count,
    completion_rate
FROM wistia_marts.mart_device_browser
ORDER BY engagement_event_count DESC
LIMIT 20;

-- visitor engagement segments

SELECT
    engagement_segment,
    COUNT(*) AS visitor_count,
    AVG(event_count) AS avg_events_per_visitor,
    AVG(avg_percent_viewed) AS avg_percent_viewed,
    AVG(completion_rate) AS avg_completion_rate
FROM wistia_marts.mart_visitor_engagement
GROUP BY engagement_segment
ORDER BY engagement_segment;

-- most engaged visitors

SELECT
    visitor_key,
    country,
    region,
    city,
    platform,
    browser,
    event_count,
    avg_percent_viewed,
    max_percent_viewed,
    completed_event_count,
    completion_rate,
    channels_seen,
    media_seen
FROM wistia_marts.mart_visitor_engagement
ORDER BY avg_percent_viewed DESC, event_count DESC
LIMIT 25;

-- simple final validation query

SELECT
    'Gold event fact validation' AS check_name,
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_key) AS unique_events,
    COUNT(DISTINCT visitor_key) AS unique_visitors,
    COUNT(DISTINCT media_hashed_id) AS media_count,
    COUNT(DISTINCT channel) AS channel_count,
    MIN(received_at) AS first_event_at,
    MAX(received_at) AS last_event_at
FROM wistia_gold.fact_media_engagement;

-- misc diagnostic queries

SELECT
    snapshot_date,
    channel,
    media_hashed_id,
    load_count,
    play_count,
    api_play_rate,
    hours_watched,
    engagement,
    visitor_count
FROM wistia_gold.fact_media_daily_stats
ORDER BY snapshot_date, channel;

SELECT
    snapshot_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT media_hashed_id) AS media_count
FROM wistia_gold.fact_media_daily_stats
GROUP BY snapshot_date
ORDER BY snapshot_date;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_key) AS distinct_event_keys,
    COUNT(*) - COUNT(DISTINCT event_key) AS duplicate_event_key_count
FROM wistia_gold.fact_media_engagement;

SELECT
    channel,
    media_hashed_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT visitor_key) AS unique_visitors,
    AVG(percent_viewed) AS avg_percent_viewed,
    MAX(percent_viewed) AS max_percent_viewed
FROM wistia_gold.fact_media_engagement
GROUP BY channel, media_hashed_id
ORDER BY channel;

SELECT
    snapshot_date,
    channel,
    media_count,
    total_load_count,
    total_play_count,
    avg_api_play_rate,
    total_hours_watched,
    avg_engagement,
    engagement_event_count,
    unique_event_visitors,
    avg_percent_viewed,
    completed_view_count,
    completion_rate,
    mobile_event_rate
FROM wistia_marts.mart_channel_performance
ORDER BY snapshot_date, channel;