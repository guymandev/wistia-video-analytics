# Wistia Video Analytics Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline for Wistia video analytics data.

The pipeline ingests video metadata, aggregate media statistics, visitor summaries, and engagement events from the Wistia API. The data is stored in an S3-based data lake, transformed through Raw, Silver, Gold, and Mart layers using AWS Glue jobs, cataloged with Glue Crawlers, and exposed for SQL analysis through Amazon Athena.

The primary analytical goal is to compare performance between two Wistia-hosted versions of the same video asset:

| Channel | Wistia Media ID | Description |
|---|---|---|
| YouTube | `gskhw4w4lm` | YouTube paid ads version |
| Facebook | `v08dlrgr7v` | Facebook paid ads version |

The final deliverable exposes both dimensional Gold tables and business-friendly Mart tables through Athena.

## Project Artifacts

- [Architecture diagram](docs/architecture.md)
- [Video walkthrough](docs/walkthrough.md)
- [Athena smoke and business queries](sql/athena/smoke_and_business_queries.sql)

## Architecture Summary

The pipeline follows this architecture:

~~~text
Wistia API
  ↓
AWS Glue Python Shell ingestion job
  ↓
S3 Raw JSON
  ↓
AWS Glue Spark Silver transform
  ↓
S3 Silver Parquet
  ↓
AWS Glue Spark Gold transform
  ↓
S3 Gold Parquet
  ↓
AWS Glue Spark Mart transform
  ↓
S3 Mart Parquet
  ↓
Glue Crawlers / Glue Data Catalog
  ↓
Athena SQL analytics
~~~

The pipeline is orchestrated with an AWS Glue Workflow.

## AWS Services Used

| Service | Purpose |
|---|---|
| AWS Glue Python Shell | Wistia API ingestion |
| AWS Glue Spark Jobs | Silver, Gold, and Mart transformations |
| AWS Glue Workflow | End-to-end orchestration |
| AWS Glue Crawlers | Catalog Gold and Mart Parquet datasets |
| AWS Glue Data Catalog | Metadata layer for Athena |
| Amazon S3 | Data lake storage |
| Amazon Athena | SQL query layer |
| AWS Secrets Manager | Stores Wistia API token |
| IAM | Glue and local development permissions |
| CloudWatch Logs | Job execution logs and troubleshooting |

## Wistia API Endpoints

The ingestion job calls the following Wistia API endpoints:

| Endpoint | Purpose |
|---|---|
| `/v1/medias/{media_id}.json` | Media metadata |
| `/v1/stats/medias/{media_id}.json` | Aggregate media statistics |
| `/v1/stats/visitors.json` | Visitor summary data |
| `/v1/stats/events.json?media_id={media_id}` | Event-level engagement data |

Authentication uses a Bearer token stored in AWS Secrets Manager.

## Important API Notes

During API exploration, the Wistia visitors endpoint appeared to behave as an account-level visitor summary endpoint rather than a media-specific endpoint. Testing with valid media IDs, numeric IDs, and invalid IDs returned the same visitor list.

Because of this, media-specific visitor engagement is modeled primarily from the events endpoint.

The events endpoint is treated as the authoritative source for:

- media-specific engagement
- visitor/media relationship
- percent viewed
- geography
- device/browser details
- event-level deduplication

## Repository Structure

~~~text
.
├── config/
│   └── media_config.yaml
├── docs/
│   ├── api_exploration.md
│   └── architecture.md
├── sql/
│   └── athena/
│       └── smoke_and_business_queries.sql
├── src/
│   ├── ingest/
│   │   └── wistia_ingest.py
│   ├── pipeline/
│   │   └── run_local_pipeline.py
│   ├── quality/
│   │   └── validate_outputs.py
│   └── transform/
│       ├── silver_transform.py
│       ├── gold_transform.py
│       ├── mart_transform.py
│       ├── silver_transform_spark.py
│       ├── gold_transform_spark.py
│       └── mart_transform_spark.py
├── tests/
│   └── test_wistia_ingest.py
├── requirements.txt
├── .gitignore
└── README.md
~~~

## Data Lake Layout

The S3 data lake uses the following structure:

~~~text
s3://wistia-video-analytics-guy/
├── scripts/
│   ├── ingest/
│   ├── transform/
│   └── config/
├── raw/
│   └── wistia/
├── silver/
│   └── wistia/
├── gold/
│   └── wistia/
├── marts/
│   └── wistia/
├── logs/
└── temp/
~~~

## Data Layers

### Raw Layer

The Raw layer stores original Wistia API responses as JSON.

~~~text
raw/wistia/
├── media_metadata/
├── media_stats/
├── visitors/
└── events/
~~~

Example event path:

~~~text
raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-07-06/page=1.json
~~~

Raw files are append-only and preserve original API payloads for replay and debugging.

### Silver Layer

The Silver layer stores normalized, flattened Parquet datasets.

~~~text
silver/wistia/
├── media_metadata/
├── media_stats/
├── visitors/
└── events/
~~~

Silver transformations include:

- flattening nested JSON
- normalizing column names
- casting data types
- extracting ingest metadata from S3 paths
- deduplicating events by `event_key`
- writing optimized Parquet outputs

### Gold Layer

The Gold layer contains dimensional model tables.

~~~text
gold/wistia/
├── dim_media/
├── dim_visitor/
├── fact_media_daily_stats/
└── fact_media_engagement/
~~~

| Table | Grain | Description |
|---|---|---|
| `dim_media` | One row per media asset | Media metadata and channel mapping |
| `dim_visitor` | One row per visitor | Visitor profile and engagement summary |
| `fact_media_daily_stats` | One row per media per snapshot date | Wistia aggregate media statistics |
| `fact_media_engagement` | One row per engagement event | Event-level media engagement |

Gold tables are exposed directly in Athena for flexible analyst querying.

### Mart Layer

The Mart layer contains reporting-friendly aggregate tables.

~~~text
marts/wistia/
├── mart_channel_performance/
├── mart_geo_engagement/
├── mart_device_browser/
└── mart_visitor_engagement/
~~~

| Mart | Purpose |
|---|---|
| `mart_channel_performance` | Compare YouTube vs Facebook performance |
| `mart_geo_engagement` | Analyze engagement by geography |
| `mart_device_browser` | Analyze engagement by device/browser |
| `mart_visitor_engagement` | Analyze visitor-level engagement patterns |

## Snapshot Fact Note

`fact_media_daily_stats` stores cumulative Wistia media statistics as daily snapshots.

Because Wistia media statistics are cumulative as of ingestion time, downstream reporting should filter to a single `snapshot_date`, usually the latest available snapshot, unless intentionally analyzing changes between snapshots over time.

For example:

~~~sql
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM wistia_marts.mart_channel_performance
)
SELECT
    m.*
FROM wistia_marts.mart_channel_performance m
JOIN latest_snapshot l
    ON m.snapshot_date = l.snapshot_date;
~~~

This prevents dashboard or reporting queries from double-counting cumulative media statistics across multiple snapshot dates.

## Glue Workflow

The AWS Glue Workflow orchestrates the full pipeline:

~~~text
wistia-api-ingestion-job
  ↓
wistia-silver-transform-job
  ↓
wistia-gold-transform-job
  ↓
wistia-mart-transform-job
  ↓
wistia-gold-crawler
wistia-marts-crawler
~~~

The workflow uses conditional triggers so downstream jobs only run when the previous job succeeds.

If ingestion fails, Silver, Gold, Mart, and crawler jobs do not run. This prevents incomplete data from being transformed and cataloged.

## Glue Jobs

### `wistia-api-ingestion-job`

Type: AWS Glue Python Shell

Purpose:

- reads Wistia API token from Secrets Manager
- reads media configuration from S3
- calls Wistia metadata, stats, visitors, and events endpoints
- handles event pagination
- writes raw JSON payloads to S3

Key arguments:

~~~text
--config       s3://wistia-video-analytics-guy/scripts/config/media_config.yaml
--output-dir   s3://wistia-video-analytics-guy/raw/wistia
--secret-name  wistia-api-key
--aws-region   us-east-1
--max-pages    1
~~~

The ingestion job includes retry/backoff handling for transient Wistia API failures such as `429`, `500`, `502`, `503`, and `504` responses.

### `wistia-silver-transform-job`

Type: AWS Glue Spark

Purpose:

- reads Raw JSON from S3
- flattens and normalizes Wistia API data
- writes Silver Parquet tables

Key arguments:

~~~text
--raw-dir      s3://wistia-video-analytics-guy/raw/wistia
--silver-dir   s3://wistia-video-analytics-guy/silver/wistia
~~~

### `wistia-gold-transform-job`

Type: AWS Glue Spark

Purpose:

- reads Silver Parquet data
- applies media/channel mapping
- builds Gold dimensional and fact tables
- writes Gold Parquet outputs

Key arguments:

~~~text
--silver-dir   s3://wistia-video-analytics-guy/silver/wistia
--gold-dir     s3://wistia-video-analytics-guy/gold/wistia
--config       s3://wistia-video-analytics-guy/scripts/config/media_config.yaml
~~~

### `wistia-mart-transform-job`

Type: AWS Glue Spark

Purpose:

- reads Gold dimensional model tables
- builds business-friendly reporting marts
- writes Mart Parquet outputs

Key arguments:

~~~text
--gold-dir     s3://wistia-video-analytics-guy/gold/wistia
--mart-dir     s3://wistia-video-analytics-guy/marts/wistia
~~~

## Athena Databases

Glue Crawlers expose Gold and Mart tables through Athena.

### Gold Database

~~~text
wistia_gold
~~~

Tables:

~~~text
dim_media
dim_visitor
fact_media_daily_stats
fact_media_engagement
~~~

### Mart Database

~~~text
wistia_marts
~~~

Tables:

~~~text
mart_channel_performance
mart_geo_engagement
mart_device_browser
mart_visitor_engagement
~~~

## Athena Smoke Tests

Example Gold row-count smoke test:

~~~sql
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
~~~

Example event deduplication test:

~~~sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_key) AS distinct_event_keys,
    COUNT(*) - COUNT(DISTINCT event_key) AS duplicate_event_key_count
FROM wistia_gold.fact_media_engagement;
~~~

Expected result:

~~~text
duplicate_event_key_count = 0
~~~

Additional smoke and business analysis queries are stored in:

~~~text
sql/athena/smoke_and_business_queries.sql
~~~

## Local Development Setup

Create and activate a virtual environment:

~~~bash
python3 -m venv py3_12
source py3_12/bin/activate
~~~

Install dependencies:

~~~bash
pip install -r requirements.txt
~~~

Create a `.env` file for local API testing:

~~~text
WISTIA_API_TOKEN=<your_token_here>
~~~

The `.env` file is ignored by Git.

## Local Pipeline Run

Run the full local pipeline:

~~~bash
python -m src.pipeline.run_local_pipeline \
  --ingest-date 2026-06-09 \
  --max-pages 1
~~~

Skip ingestion and reuse existing Raw files:

~~~bash
python -m src.pipeline.run_local_pipeline --skip-ingestion
~~~

Skip validation:

~~~bash
python -m src.pipeline.run_local_pipeline --skip-validation
~~~

View CLI options:

~~~bash
python -m src.pipeline.run_local_pipeline --help
~~~

## Local PySpark Transforms

Run Silver Spark transform locally:

~~~bash
python -m src.transform.silver_transform_spark \
  --raw-dir data/raw/wistia \
  --silver-dir data/silver_spark/wistia
~~~

Run Gold Spark transform locally:

~~~bash
python -m src.transform.gold_transform_spark \
  --silver-dir data/silver_spark/wistia \
  --gold-dir data/gold_spark/wistia \
  --config config/media_config.yaml
~~~

Run Mart Spark transform locally:

~~~bash
python -m src.transform.mart_transform_spark \
  --gold-dir data/gold_spark/wistia \
  --mart-dir data/marts_spark/wistia
~~~

## Data Quality Validation

The local pipeline includes a validation module:

~~~bash
python -m src.quality.validate_outputs
~~~

Validation checks include:

- expected Gold and Mart outputs exist
- required columns are present
- key fields are non-null
- event keys are unique
- expected channel values are present
- percent-viewed and completion-rate fields are within valid ranges
- marts are non-empty

## Dashboard Scope

The dashboard layer was treated as optional for this capstone.

Instead, the project exposes both Gold dimensional tables and Mart reporting tables through Athena, enabling SQL-based analysis of:

- channel performance
- visitor engagement
- geography
- device/browser behavior
- media-level aggregate statistics
- event-level engagement

This keeps the final consumption layer focused on Athena-backed analytical access rather than a separate Streamlit deployment.

## Current Pipeline Status

Completed:

- Wistia API exploration
- Bearer-token authentication
- Raw JSON ingestion
- S3 data lake layout
- Silver PySpark transform
- Gold PySpark transform
- Mart PySpark transform
- AWS Glue jobs
- AWS Glue workflow
- Glue Crawlers
- Athena databases and tables
- Athena smoke/business queries
- Retry handling for transient API errors

## Notes and Assumptions

- The pipeline currently processes two configured Wistia media IDs.
- `media_config.yaml` is the source of truth for mapping Wistia media IDs to business channels.
- The visitors endpoint is treated as supplemental because it did not behave as media-specific during API testing.
- Engagement events are deduplicated by `event_key`.
- Raw data is retained as original JSON.
- Silver, Gold, and Mart layers are written as Parquet.
- Gold and Mart layers are rebuilt using overwrite mode during each workflow run.
- Snapshot-based metrics should be filtered by `snapshot_date` for dashboard-style reporting.
