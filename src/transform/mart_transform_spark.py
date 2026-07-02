"""
PySpark Mart transform for Wistia Video Analytics.

Responsibilities:
- Read Gold Parquet datasets from local storage or S3.
- Build dashboard/query-friendly analytics marts.
- Write Mart datasets as Parquet.

Expected Gold input layout:

gold/wistia/
├── dim_media/
├── dim_visitor/
├── fact_media_daily_stats/
└── fact_media_engagement/

Expected Mart output layout:

marts/wistia/
├── mart_channel_performance/
├── mart_geo_engagement/
├── mart_device_browser/
└── mart_visitor_engagement/
"""

from __future__ import annotations

import argparse
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark_session(app_name: str = "WistiaMartTransform") -> SparkSession:
    """
    Build a SparkSession.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def normalize_base_path(path: str) -> str:
    """
    Remove trailing slash from a path.
    """
    return path.rstrip("/")


def table_path(base_dir: str, table_name: str) -> str:
    """
    Build table path.
    """
    return f"{normalize_base_path(base_dir)}/{table_name}"


def read_parquet_table(spark: SparkSession, base_dir: str, table_name: str) -> DataFrame:
    """
    Read a Parquet table from local storage or S3.
    """
    path = table_path(base_dir, table_name)
    print(f"Reading {table_name} from: {path}")
    return spark.read.parquet(path)


def write_parquet_table(
    df: DataFrame,
    base_dir: str,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame as Parquet.
    """
    path = table_path(base_dir, table_name)
    print(f"Writing {table_name} to: {path}")
    df.write.mode(mode).parquet(path)
    print(f"Wrote {table_name} rows: {df.count()}")


def safe_divide(numerator_col: F.Column, denominator_col: F.Column) -> F.Column:
    """
    Safely divide two Spark columns.

    Returns 0.0 when denominator is null or zero.
    """
    return (
        F.when(
            denominator_col.isNull() | (denominator_col == 0),
            F.lit(0.0),
        )
        .otherwise(numerator_col.cast("double") / denominator_col.cast("double"))
    )


def build_mart_channel_performance(
    fact_media_daily_stats_df: DataFrame,
    fact_media_engagement_df: DataFrame,
    dim_media_df: DataFrame,
) -> DataFrame:
    """
    Build mart_channel_performance.

    Grain:
    - One row per snapshot_date/channel.

    Business questions:
    - How does YouTube perform vs Facebook?
    - Which channel has better play rate?
    - Which channel has higher average percent viewed?
    - Which channel has more completed views?
    """
    stats_df = fact_media_daily_stats_df
    engagement_df = fact_media_engagement_df
    media_df = dim_media_df

    stats_agg_df = (
        stats_df
        .groupBy("snapshot_date", "channel")
        .agg(
            F.countDistinct("media_hashed_id").alias("media_count"),
            F.sum("load_count").alias("total_load_count"),
            F.sum("play_count").alias("total_play_count"),
            F.avg("api_play_rate").alias("avg_api_play_rate"),
            F.avg("calculated_play_rate").alias("avg_calculated_play_rate"),
            F.sum("hours_watched").alias("total_hours_watched"),
            F.avg("engagement").alias("avg_engagement"),
            F.sum("visitor_count").alias("total_visitor_count"),
        )
    )

    engagement_agg_df = (
        engagement_df
        .groupBy("channel")
        .agg(
            F.count("event_key").alias("engagement_event_count"),
            F.countDistinct("visitor_key").alias("unique_event_visitors"),
            F.avg("percent_viewed").alias("avg_percent_viewed"),
            F.max("percent_viewed").alias("max_percent_viewed"),
            F.sum(
                F.when(F.col("percent_viewed") >= 0.95, F.lit(1)).otherwise(F.lit(0))
            ).alias("completed_view_count"),
            F.sum(
                F.when(F.col("is_mobile") == True, F.lit(1)).otherwise(F.lit(0))
            ).alias("mobile_event_count"),
        )
        .withColumn(
            "completion_rate",
            safe_divide(F.col("completed_view_count"), F.col("engagement_event_count")),
        )
        .withColumn(
            "mobile_event_rate",
            safe_divide(F.col("mobile_event_count"), F.col("engagement_event_count")),
        )
    )

    media_names_df = (
        media_df
        .groupBy("channel")
        .agg(
            F.concat_ws(
                " | ",
                F.sort_array(F.collect_set("media_name")),
            ).alias("media_names")
        )
    )

    return (
        stats_agg_df
        .join(engagement_agg_df, on="channel", how="left")
        .join(media_names_df, on="channel", how="left")
        .select(
            "snapshot_date",
            "channel",
            "media_count",
            "media_names",
            "total_load_count",
            "total_play_count",
            "avg_api_play_rate",
            "avg_calculated_play_rate",
            "total_hours_watched",
            "avg_engagement",
            "total_visitor_count",
            "engagement_event_count",
            "unique_event_visitors",
            "avg_percent_viewed",
            "max_percent_viewed",
            "completed_view_count",
            "completion_rate",
            "mobile_event_count",
            "mobile_event_rate",
        )
    )


def build_mart_geo_engagement(
    fact_media_engagement_df: DataFrame,
) -> DataFrame:
    """
    Build mart_geo_engagement.

    Grain:
    - One row per country/region/city/channel.
    """
    df = fact_media_engagement_df

    mart_df = (
        df
        .groupBy("country", "region", "city", "channel")
        .agg(
            F.count("event_key").alias("engagement_event_count"),
            F.countDistinct("visitor_key").alias("unique_visitors"),
            F.avg("percent_viewed").alias("avg_percent_viewed"),
            F.max("percent_viewed").alias("max_percent_viewed"),
            F.sum(
                F.when(F.col("percent_viewed") >= 0.95, F.lit(1)).otherwise(F.lit(0))
            ).alias("completed_view_count"),
            F.min("received_at").alias("first_event_at"),
            F.max("received_at").alias("last_event_at"),
        )
        .withColumn(
            "completion_rate",
            safe_divide(F.col("completed_view_count"), F.col("engagement_event_count")),
        )
    )

    return mart_df.select(
        "country",
        "region",
        "city",
        "channel",
        "engagement_event_count",
        "unique_visitors",
        "avg_percent_viewed",
        "max_percent_viewed",
        "completed_view_count",
        "completion_rate",
        "first_event_at",
        "last_event_at",
    )


def build_mart_device_browser(
    fact_media_engagement_df: DataFrame,
) -> DataFrame:
    """
    Build mart_device_browser.

    Grain:
    - One row per platform/browser/browser_version/is_mobile/channel.
    """
    df = fact_media_engagement_df

    mart_df = (
        df
        .groupBy("platform", "browser", "browser_version", "is_mobile", "channel")
        .agg(
            F.count("event_key").alias("engagement_event_count"),
            F.countDistinct("visitor_key").alias("unique_visitors"),
            F.avg("percent_viewed").alias("avg_percent_viewed"),
            F.max("percent_viewed").alias("max_percent_viewed"),
            F.sum(
                F.when(F.col("percent_viewed") >= 0.95, F.lit(1)).otherwise(F.lit(0))
            ).alias("completed_view_count"),
            F.min("received_at").alias("first_event_at"),
            F.max("received_at").alias("last_event_at"),
        )
        .withColumn(
            "completion_rate",
            safe_divide(F.col("completed_view_count"), F.col("engagement_event_count")),
        )
    )

    return mart_df.select(
        "platform",
        "browser",
        "browser_version",
        "is_mobile",
        "channel",
        "engagement_event_count",
        "unique_visitors",
        "avg_percent_viewed",
        "max_percent_viewed",
        "completed_view_count",
        "completion_rate",
        "first_event_at",
        "last_event_at",
    )


def build_mart_visitor_engagement(
    dim_visitor_df: DataFrame,
    fact_media_engagement_df: DataFrame,
) -> DataFrame:
    """
    Build mart_visitor_engagement.

    Grain:
    - One row per visitor_key.

    Business questions:
    - Which visitors are most engaged?
    - Which visitors completed the video?
    - Which channels/media did each visitor see?
    """
    visitors_df = dim_visitor_df
    engagement_df = fact_media_engagement_df

    channel_summary_df = (
        engagement_df
        .groupBy("visitor_key")
        .agg(
            F.concat_ws(
                " | ",
                F.sort_array(F.collect_set("channel")),
            ).alias("channels_seen"),
            F.concat_ws(
                " | ",
                F.sort_array(F.collect_set("media_hashed_id")),
            ).alias("media_seen"),
            F.sum(
                F.when(F.col("percent_viewed") >= 0.95, F.lit(1)).otherwise(F.lit(0))
            ).alias("completed_event_count"),
        )
    )

    mart_df = (
        visitors_df
        .join(channel_summary_df, on="visitor_key", how="left")
        .withColumn(
            "completed_event_count",
            F.coalesce(F.col("completed_event_count"), F.lit(0)),
        )
        .withColumn(
            "completion_rate",
            safe_divide(F.col("completed_event_count"), F.col("event_count")),
        )
        .withColumn(
            "engagement_segment",
            F.when(F.col("avg_percent_viewed") <= 0.25, F.lit("0-25%"))
            .when(F.col("avg_percent_viewed") <= 0.50, F.lit("26-50%"))
            .when(F.col("avg_percent_viewed") <= 0.75, F.lit("51-75%"))
            .otherwise(F.lit("76-100%")),
        )
    )

    return mart_df.select(
        "visitor_key",
        "email",
        "country",
        "region",
        "city",
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
        "completed_event_count",
        "completion_rate",
        "engagement_segment",
        "media_count",
        "channels_seen",
        "media_seen",
        "ingest_date",
    )


def run_mart_transform_spark(
    gold_base_dir: str,
    mart_base_dir: str,
    marts: Optional[List[str]] = None,
) -> None:
    """
    Run selected Mart transforms.
    """
    spark = build_spark_session()

    marts = marts or [
        "mart_channel_performance",
        "mart_geo_engagement",
        "mart_device_browser",
        "mart_visitor_engagement",
    ]

    print("Starting Wistia PySpark Mart transform")
    print(f"Gold base dir: {gold_base_dir}")
    print(f"Mart base dir: {mart_base_dir}")
    print(f"Marts: {marts}")

    dim_media_df = read_parquet_table(spark, gold_base_dir, "dim_media")
    dim_visitor_df = read_parquet_table(spark, gold_base_dir, "dim_visitor")
    fact_media_daily_stats_df = read_parquet_table(
        spark,
        gold_base_dir,
        "fact_media_daily_stats",
    )
    fact_media_engagement_df = read_parquet_table(
        spark,
        gold_base_dir,
        "fact_media_engagement",
    )

    if "mart_channel_performance" in marts:
        mart_channel_performance_df = build_mart_channel_performance(
            fact_media_daily_stats_df=fact_media_daily_stats_df,
            fact_media_engagement_df=fact_media_engagement_df,
            dim_media_df=dim_media_df,
        )
        write_parquet_table(
            mart_channel_performance_df,
            mart_base_dir,
            "mart_channel_performance",
        )

    if "mart_geo_engagement" in marts:
        mart_geo_engagement_df = build_mart_geo_engagement(
            fact_media_engagement_df=fact_media_engagement_df,
        )
        write_parquet_table(
            mart_geo_engagement_df,
            mart_base_dir,
            "mart_geo_engagement",
        )

    if "mart_device_browser" in marts:
        mart_device_browser_df = build_mart_device_browser(
            fact_media_engagement_df=fact_media_engagement_df,
        )
        write_parquet_table(
            mart_device_browser_df,
            mart_base_dir,
            "mart_device_browser",
        )

    if "mart_visitor_engagement" in marts:
        mart_visitor_engagement_df = build_mart_visitor_engagement(
            dim_visitor_df=dim_visitor_df,
            fact_media_engagement_df=fact_media_engagement_df,
        )
        write_parquet_table(
            mart_visitor_engagement_df,
            mart_base_dir,
            "mart_visitor_engagement",
        )

    print("Completed Wistia PySpark Mart transform")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Uses parse_known_args() so AWS Glue runtime arguments do not crash the script.
    """
    parser = argparse.ArgumentParser(
        description="Transform Gold Wistia Parquet to Mart Parquet using PySpark."
    )

    parser.add_argument(
        "--gold-dir",
        default="data/gold_spark/wistia",
        help="Base Gold Wistia data directory.",
    )

    parser.add_argument(
        "--mart-dir",
        default="data/marts_spark/wistia",
        help="Base Mart Wistia output directory.",
    )

    parser.add_argument(
        "--mart",
        action="append",
        choices=[
            "mart_channel_performance",
            "mart_geo_engagement",
            "mart_device_browser",
            "mart_visitor_engagement",
        ],
        help=(
            "Mart table to build. Can be supplied multiple times. "
            "Defaults to all marts."
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

    run_mart_transform_spark(
        gold_base_dir=args.gold_dir,
        mart_base_dir=args.mart_dir,
        marts=args.mart,
    )


if __name__ == "__main__":
    main()