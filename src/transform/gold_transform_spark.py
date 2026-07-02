"""
PySpark Gold transform for Wistia Video Analytics.

Responsibilities:
- Read Silver Parquet datasets from local storage or S3.
- Build Gold dimensional/fact tables.
- Enrich records with configured channel mapping.
- Write Gold datasets as Parquet.

Expected Silver input layout:

silver/wistia/
├── media_metadata/
├── media_stats/
├── visitors/
└── events/

Expected Gold output layout:

gold/wistia/
├── dim_media/
├── fact_media_daily_stats/
├── fact_media_engagement/
└── dim_visitor/
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def build_spark_session(app_name: str = "WistiaGoldTransform") -> SparkSession:
    """
    Build a SparkSession.

    In AWS Glue, SparkSession is provided by the Glue runtime.
    Locally, this creates a normal SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def normalize_base_path(path: str) -> str:
    """
    Remove trailing slash from a path.
    """
    return path.rstrip("/")


def input_path(base_dir: str, table_name: str) -> str:
    """
    Build input table path.
    """
    return f"{normalize_base_path(base_dir)}/{table_name}"


def output_path(base_dir: str, table_name: str) -> str:
    """
    Build output table path.
    """
    return f"{normalize_base_path(base_dir)}/{table_name}"


def is_s3_path(path: str) -> bool:
    """
    Return True if path is an S3 URI.
    """
    return str(path).startswith("s3://")


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """
    Parse an S3 URI into bucket and key.
    """
    parsed = urlparse(str(s3_uri))

    if parsed.scheme != "s3":
        raise ValueError(f"Not a valid S3 URI: {s3_uri}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not bucket or not key:
        raise ValueError(f"S3 URI must include bucket and key: {s3_uri}")

    return bucket, key


def read_text_file(path: str) -> str:
    """
    Read text from local file or S3.
    """
    if is_s3_path(path):
        bucket, key = parse_s3_uri(path)
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    return Path(path).read_text(encoding="utf-8")


def parse_media_config_without_yaml(config_text: str) -> List[Dict[str, Any]]:
    """
    Parse the small media_config.yaml file without requiring PyYAML.

    This is intentionally simple because our config shape is simple:

    media_ids:
      - media_id: gskhw4w4lm
        channel: YouTube
        name: Some Name
    """
    records: List[Dict[str, Any]] = []
    current_record: Dict[str, Any] = {}

    for raw_line in config_text.splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or line == "media_ids:":
            continue

        if line.startswith("- "):
            if current_record:
                records.append(current_record)

            current_record = {}

            line = line[2:].strip()
            if ":" in line:
                key, value = line.split(":", 1)
                current_record[key.strip()] = value.strip().strip("'\"")

        elif ":" in line and current_record is not None:
            key, value = line.split(":", 1)
            current_record[key.strip()] = value.strip().strip("'\"")

    if current_record:
        records.append(current_record)

    if not records:
        raise ValueError("No media records found in media config.")

    return records


def load_media_config_records(config_path: str) -> List[Dict[str, Any]]:
    """
    Load media/channel mapping from local file or S3.

    Tries PyYAML if available. Falls back to a simple parser so Glue Spark
    does not require an extra dependency for this small config file.
    """
    config_text = read_text_file(config_path)

    try:
        import yaml  # type: ignore

        config = yaml.safe_load(config_text) or {}
        media_items = config.get("media_ids", [])

        if not media_items:
            raise ValueError("media_config.yaml must contain a non-empty media_ids list.")

        return [
            {
                "media_hashed_id": item.get("media_id"),
                "channel": item.get("channel"),
                "configured_media_name": item.get("name"),
            }
            for item in media_items
        ]

    except ImportError:
        parsed_items = parse_media_config_without_yaml(config_text)

        return [
            {
                "media_hashed_id": item.get("media_id"),
                "channel": item.get("channel"),
                "configured_media_name": item.get("name"),
            }
            for item in parsed_items
        ]


def build_media_config_df(spark: SparkSession, config_path: str) -> DataFrame:
    """
    Build Spark DataFrame with media_id/channel mapping.
    """
    records = load_media_config_records(config_path)

    schema = StructType(
        [
            StructField("media_hashed_id", StringType(), False),
            StructField("channel", StringType(), True),
            StructField("configured_media_name", StringType(), True),
        ]
    )

    return spark.createDataFrame(records, schema=schema)


def read_parquet_table(spark: SparkSession, base_dir: str, table_name: str) -> DataFrame:
    """
    Read a Silver/Gold Parquet table.
    """
    path = input_path(base_dir, table_name)
    print(f"Reading {table_name} from: {path}")
    return spark.read.parquet(path)


def write_parquet_table(df: DataFrame, base_dir: str, table_name: str, mode: str = "overwrite") -> None:
    """
    Write a Gold Parquet table.
    """
    path = output_path(base_dir, table_name)
    print(f"Writing {table_name} to: {path}")
    df.write.mode(mode).parquet(path)
    print(f"Wrote {table_name} rows: {df.count()}")


def build_dim_media(
    silver_media_metadata_df: DataFrame,
    media_config_df: DataFrame,
) -> DataFrame:
    """
    Build Gold dim_media.

    Grain:
    - One row per Wistia media/video.
    """
    df = silver_media_metadata_df.alias("m").join(
        media_config_df.alias("c"),
        on="media_hashed_id",
        how="left",
    )

    df = df.withColumn(
        "media_name",
        F.coalesce(F.col("media_name"), F.col("configured_media_name")),
    )

    df = df.withColumn("media_key", F.col("media_hashed_id"))

    # If multiple ingest dates exist, keep the latest row per media_hashed_id.
    window = (
        F.row_number()
        .over(
            __import__("pyspark.sql.window")
            .sql.window.Window.partitionBy("media_hashed_id")
            .orderBy(F.col("updated_at").desc_nulls_last(), F.col("ingest_date").desc_nulls_last())
        )
    )

    df = df.withColumn("row_num", window).filter(F.col("row_num") == 1)

    return df.select(
        "media_key",
        "media_hashed_id",
        "wistia_numeric_id",
        "channel",
        "media_name",
        "media_type",
        "duration_seconds",
        "created_at",
        "updated_at",
        "status",
        "archived",
        "description",
        "project_id",
        "project_hashed_id",
        "project_name",
        "section_id",
        "section_name",
        "share_url",
        "thumbnail_url",
        "thumbnail_width",
        "thumbnail_height",
        "ingest_date",
    )


def build_fact_media_daily_stats(
    silver_media_stats_df: DataFrame,
    media_config_df: DataFrame,
) -> DataFrame:
    """
    Build Gold fact_media_daily_stats.

    Grain:
    - One row per media_hashed_id per ingest_date.
    """
    df = silver_media_stats_df.join(
        media_config_df.select("media_hashed_id", "channel"),
        on="media_hashed_id",
        how="left",
    )

    df = df.withColumn("snapshot_date", F.col("ingest_date"))

    df = df.withColumn(
        "media_daily_stats_key",
        F.concat_ws("_", F.col("media_hashed_id"), F.col("snapshot_date")),
    )

    df = df.dropDuplicates(["media_hashed_id", "snapshot_date"])

    return df.select(
        "media_daily_stats_key",
        "media_hashed_id",
        "channel",
        "snapshot_date",
        "load_count",
        "play_count",
        "api_play_rate",
        "calculated_play_rate",
        "hours_watched",
        "engagement",
        "visitor_count",
        "ingest_date",
    )


def build_fact_media_engagement(
    silver_events_df: DataFrame,
    media_config_df: DataFrame,
) -> DataFrame:
    """
    Build Gold fact_media_engagement.

    Grain:
    - One row per Wistia engagement event.
    """
    df = silver_events_df.join(
        media_config_df.select("media_hashed_id", "channel"),
        on="media_hashed_id",
        how="left",
    )

    df = df.withColumn("event_date", F.to_date(F.col("received_at")))
    df = df.withColumn("media_engagement_key", F.col("event_key"))

    before_count = df.count()
    df = df.dropDuplicates(["event_key"])
    after_count = df.count()

    print(
        f"Deduplicated fact_media_engagement: "
        f"{before_count} records before, {after_count} records after."
    )

    return df.select(
        "media_engagement_key",
        "event_key",
        "received_at",
        "event_date",
        "media_hashed_id",
        "channel",
        "visitor_key",
        "percent_viewed",
        "ip_address",
        "country",
        "region",
        "city",
        "latitude",
        "longitude",
        "organization",
        "email",
        "browser",
        "browser_version",
        "platform",
        "is_mobile",
        "embed_url",
        "media_url",
        "conversion_type",
        "conversion_data_json",
        "iframe_heatmap_url",
        "thumbnail_url",
        "thumbnail_width",
        "thumbnail_height",
        "ingest_date",
        "source_page",
    )


def build_dim_visitor(
    silver_events_df: DataFrame,
    silver_visitors_df: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Build Gold dim_visitor.

    Grain:
    - One row per visitor_key.

    Primary source:
    - Silver events

    Optional supplemental source:
    - Silver visitors
    """
    events_df = silver_events_df

    base_df = events_df.groupBy("visitor_key").agg(
        F.min("received_at").alias("first_seen_at"),
        F.max("received_at").alias("last_seen_at"),
        F.count("event_key").alias("event_count"),
        F.avg("percent_viewed").alias("avg_percent_viewed"),
        F.max("percent_viewed").alias("max_percent_viewed"),
        F.countDistinct("media_hashed_id").alias("media_count"),
        F.last("ip_address", ignorenulls=True).alias("ip_address"),
        F.last("country", ignorenulls=True).alias("country"),
        F.last("region", ignorenulls=True).alias("region"),
        F.last("city", ignorenulls=True).alias("city"),
        F.last("latitude", ignorenulls=True).alias("latitude"),
        F.last("longitude", ignorenulls=True).alias("longitude"),
        F.last("organization", ignorenulls=True).alias("organization"),
        F.last("email", ignorenulls=True).alias("email"),
        F.last("browser", ignorenulls=True).alias("browser"),
        F.last("browser_version", ignorenulls=True).alias("browser_version"),
        F.last("platform", ignorenulls=True).alias("platform"),
        F.last("is_mobile", ignorenulls=True).alias("is_mobile"),
        F.last("ingest_date", ignorenulls=True).alias("ingest_date"),
    )

    if silver_visitors_df is not None:
        visitors_df = silver_visitors_df.dropDuplicates(["visitor_key"]).select(
            "visitor_key",
            F.col("visitor_email").alias("visitor_summary_email"),
            F.col("visitor_org_name").alias("visitor_summary_org_name"),
            F.col("browser").alias("visitor_summary_browser"),
            F.col("browser_version").alias("visitor_summary_browser_version"),
            F.col("platform").alias("visitor_summary_platform"),
            F.col("is_mobile").alias("visitor_summary_is_mobile"),
            F.col("created_at").alias("visitor_summary_created_at"),
            F.col("last_active_at").alias("visitor_summary_last_active_at"),
        )

        base_df = base_df.join(visitors_df, on="visitor_key", how="left")

        base_df = (
            base_df
            .withColumn("email", F.coalesce(F.col("email"), F.col("visitor_summary_email")))
            .withColumn("organization", F.coalesce(F.col("organization"), F.col("visitor_summary_org_name")))
            .withColumn("browser", F.coalesce(F.col("browser"), F.col("visitor_summary_browser")))
            .withColumn("browser_version", F.coalesce(F.col("browser_version"), F.col("visitor_summary_browser_version")))
            .withColumn("platform", F.coalesce(F.col("platform"), F.col("visitor_summary_platform")))
            .withColumn("is_mobile", F.coalesce(F.col("is_mobile"), F.col("visitor_summary_is_mobile")))
            .withColumn("first_seen_at", F.coalesce(F.col("first_seen_at"), F.col("visitor_summary_created_at")))
            .withColumn("last_seen_at", F.coalesce(F.col("last_seen_at"), F.col("visitor_summary_last_active_at")))
        )

    base_df = base_df.withColumn("visitor_key_dim", F.col("visitor_key"))

    return base_df.select(
        "visitor_key_dim",
        "visitor_key",
        "email",
        "ip_address",
        "country",
        "region",
        "city",
        "latitude",
        "longitude",
        "organization",
        "browser",
        "browser_version",
        "platform",
        "is_mobile",
        "first_seen_at",
        "last_seen_at",
        "event_count",
        "avg_percent_viewed",
        "max_percent_viewed",
        "media_count",
        "ingest_date",
    )


def run_gold_transform_spark(
    silver_base_dir: str,
    gold_base_dir: str,
    config_path: str,
    tables: Optional[List[str]] = None,
) -> None:
    """
    Run selected Gold transforms.
    """
    spark = build_spark_session()

    tables = tables or [
        "dim_media",
        "fact_media_daily_stats",
        "fact_media_engagement",
        "dim_visitor",
    ]

    print("Starting Wistia PySpark Gold transform")
    print(f"Silver base dir: {silver_base_dir}")
    print(f"Gold base dir: {gold_base_dir}")
    print(f"Config path: {config_path}")
    print(f"Tables: {tables}")

    media_config_df = build_media_config_df(spark, config_path)

    silver_media_metadata_df = read_parquet_table(spark, silver_base_dir, "media_metadata")
    silver_media_stats_df = read_parquet_table(spark, silver_base_dir, "media_stats")
    silver_events_df = read_parquet_table(spark, silver_base_dir, "events")
    silver_visitors_df = read_parquet_table(spark, silver_base_dir, "visitors")

    if "dim_media" in tables:
        dim_media_df = build_dim_media(
            silver_media_metadata_df=silver_media_metadata_df,
            media_config_df=media_config_df,
        )
        write_parquet_table(dim_media_df, gold_base_dir, "dim_media")

    if "fact_media_daily_stats" in tables:
        fact_media_daily_stats_df = build_fact_media_daily_stats(
            silver_media_stats_df=silver_media_stats_df,
            media_config_df=media_config_df,
        )
        write_parquet_table(
            fact_media_daily_stats_df,
            gold_base_dir,
            "fact_media_daily_stats",
        )

    if "fact_media_engagement" in tables:
        fact_media_engagement_df = build_fact_media_engagement(
            silver_events_df=silver_events_df,
            media_config_df=media_config_df,
        )
        write_parquet_table(
            fact_media_engagement_df,
            gold_base_dir,
            "fact_media_engagement",
        )

    if "dim_visitor" in tables:
        dim_visitor_df = build_dim_visitor(
            silver_events_df=silver_events_df,
            silver_visitors_df=silver_visitors_df,
        )
        write_parquet_table(dim_visitor_df, gold_base_dir, "dim_visitor")

    print("Completed Wistia PySpark Gold transform")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Uses parse_known_args() so AWS Glue runtime arguments do not crash the script.
    """
    parser = argparse.ArgumentParser(
        description="Transform Silver Wistia Parquet to Gold Parquet using PySpark."
    )

    parser.add_argument(
        "--silver-dir",
        default="data/silver_spark/wistia",
        help="Base Silver Wistia data directory.",
    )

    parser.add_argument(
        "--gold-dir",
        default="data/gold_spark/wistia",
        help="Base Gold Wistia output directory.",
    )

    parser.add_argument(
        "--config",
        default="config/media_config.yaml",
        help="Path to media config YAML file. Supports local or s3:// paths.",
    )

    parser.add_argument(
        "--table",
        action="append",
        choices=[
            "dim_media",
            "fact_media_daily_stats",
            "fact_media_engagement",
            "dim_visitor",
        ],
        help=(
            "Gold table to build. Can be supplied multiple times. "
            "Defaults to all tables."
        ),
    )

    args, unknown_args = parser.parse_known_args()

    if unknown_args:
        print(f"Ignoring unknown Glue/job arguments: {unknown_args}")

    return args


def main() -> None:
    """
    CLI entrypoint.
    """
    args = parse_args()

    run_gold_transform_spark(
        silver_base_dir=args.silver_dir,
        gold_base_dir=args.gold_dir,
        config_path=args.config,
        tables=args.table,
    )


if __name__ == "__main__":
    main()