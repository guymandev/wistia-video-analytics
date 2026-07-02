"""
PySpark Silver transform for Wistia Video Analytics.

Responsibilities:
- Read raw Wistia JSON files from local storage or S3.
- Flatten nested JSON structures.
- Normalize and select Silver-layer columns.
- Add path-derived ingest metadata.
- Deduplicate event records by event_key.
- Write Silver datasets as Parquet.

Expected raw input layout:

raw/wistia/
├── media_metadata/
│   └── media_id=<media_id>/
│       └── ingest_date=<YYYY-MM-DD>/
│           └── response.json
├── media_stats/
│   └── media_id=<media_id>/
│       └── ingest_date=<YYYY-MM-DD>/
│           └── response.json
├── visitors/
│   └── ingest_date=<YYYY-MM-DD>/
│       └── response.json
└── events/
    └── media_id=<media_id>/
        └── ingest_date=<YYYY-MM-DD>/
            └── page=<page_number>.json

Expected Silver output layout:

silver/wistia/
├── media_metadata/
├── media_stats/
├── visitors/
└── events/
"""

from __future__ import annotations

import argparse
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    regexp_extract,
    to_date,
    to_timestamp,
    when,
)

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)


def build_spark_session(app_name: str = "WistiaSilverTransform") -> SparkSession:
    """
    Build a SparkSession.

    In AWS Glue, SparkSession is provided by the Glue runtime.
    Locally, this creates a normal SparkSession.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def normalize_base_path(path: str) -> str:
    """
    Remove trailing slash from a path.
    """
    return path.rstrip("/")


def endpoint_path(raw_base_dir: str, endpoint_name: str) -> str:
    """
    Build endpoint input path.
    """
    return f"{normalize_base_path(raw_base_dir)}/{endpoint_name}"


def output_path(silver_base_dir: str, endpoint_name: str) -> str:
    """
    Build endpoint output path.
    """
    return f"{normalize_base_path(silver_base_dir)}/{endpoint_name}"


def read_json_endpoint(spark: SparkSession, path: str) -> DataFrame:
    """
    Read JSON files recursively.

    multiLine=true is important because Wistia event files are JSON arrays
    spread across many lines.

    recursiveFileLookup=true allows Spark to find files nested under
    media_id=.../ingest_date=... folders.
    """
    return (
        spark.read
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(path)
        .withColumn("source_file", input_file_name())
    )


def add_path_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata extracted from the source file path.

    Examples extracted from path:
    - source_media_id from media_id=<value>
    - ingest_date from ingest_date=<value>
    - source_page from page=<number>.json
    """
    return (
        df
        .withColumn(
            "source_media_id",
            regexp_extract(col("source_file"), r"media_id=([^/]+)", 1),
        )
        .withColumn(
            "ingest_date",
            regexp_extract(col("source_file"), r"ingest_date=([^/]+)", 1),
        )
        .withColumn(
            "source_page",
            regexp_extract(col("source_file"), r"page=(\d+)\.json", 1),
        )
        .withColumn(
            "source_media_id",
            when(col("source_media_id") == "", lit(None)).otherwise(col("source_media_id")),
        )
        .withColumn(
            "ingest_date",
            when(col("ingest_date") == "", lit(None)).otherwise(col("ingest_date")),
        )
        .withColumn(
            "source_page",
            when(col("source_page") == "", lit(None)).otherwise(col("source_page").cast(IntegerType())),
        )
        .withColumn("silver_processed_at", current_timestamp())
    )


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Write a DataFrame as Parquet.
    """
    (
        df.write
        .mode(mode)
        .parquet(path)
    )


def safe_col(df: DataFrame, column_name: str, data_type: DataType = StringType()):
    """
    Return a column if it exists, otherwise return a typed null literal.
    """
    if column_name in df.columns:
        return col(column_name).cast(data_type)

    return lit(None).cast(data_type)


def safe_nested_col(df: DataFrame, column_path: str, data_type: DataType = StringType()):
    """
    Return a nested column if its top-level parent exists, otherwise null.
    """
    top_level_column = column_path.split(".", 1)[0]

    if top_level_column in df.columns:
        return col(column_path).cast(data_type)

    return lit(None).cast(data_type)


def column_exists(df: DataFrame, column_name: str) -> bool:
    """
    Return True if a column exists in a DataFrame.
    """
    return column_name in df.columns


def select_existing_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Select only columns that exist in the DataFrame.
    """
    existing_columns = [column for column in columns if column in df.columns]
    return df.select(*existing_columns)


def transform_media_metadata(spark: SparkSession, raw_base_dir: str, silver_base_dir: str) -> None:
    """
    Transform Wistia media metadata JSON to Silver Parquet.

    Grain:
    - One row per media metadata response.
    """
    input_path = endpoint_path(raw_base_dir, "media_metadata")
    output = output_path(silver_base_dir, "media_metadata")

    print(f"Reading media_metadata from: {input_path}")

    df = read_json_endpoint(spark, input_path)
    df = add_path_metadata(df)

    # Wistia section may be a string in the actual API response.
    # If it ever arrives as a struct, this first draft keeps the string-style behavior.
    section_name_expr = (
        col("section").cast(StringType())
        if column_exists(df, "section")
        else lit(None).cast(StringType())
    )

    transformed_df = df.select(
        col("id").cast(LongType()).alias("wistia_numeric_id"),
        col("hashed_id").cast(StringType()).alias("media_hashed_id"),
        col("type").cast(StringType()).alias("media_type"),
        col("name").cast(StringType()).alias("media_name"),
        col("duration").cast(DoubleType()).alias("duration_seconds"),
        to_timestamp(col("created")).alias("created_at"),
        to_timestamp(col("updated")).alias("updated_at"),
        col("status").cast(StringType()).alias("status"),
        col("archived").cast(BooleanType()).alias("archived"),
        col("description").cast(StringType()).alias("description"),
        col("project.id").cast(LongType()).alias("project_id"),
        col("project.hashed_id").cast(StringType()).alias("project_hashed_id"),
        col("project.name").cast(StringType()).alias("project_name"),
        lit(None).cast(LongType()).alias("section_id"),
        section_name_expr.alias("section_name"),
        col("share_link.url").cast(StringType()).alias("share_url"),
        col("thumbnail.url").cast(StringType()).alias("thumbnail_url"),
        col("thumbnail.width").cast(IntegerType()).alias("thumbnail_width"),
        col("thumbnail.height").cast(IntegerType()).alias("thumbnail_height"),
        col("source_file"),
        col("ingest_date"),
        col("source_media_id"),
        col("silver_processed_at"),
    )

    print(f"Writing Silver media_metadata to: {output}")
    write_parquet(transformed_df, output)
    print(f"Wrote Silver media_metadata rows: {transformed_df.count()}")


def transform_media_stats(spark: SparkSession, raw_base_dir: str, silver_base_dir: str) -> None:
    """
    Transform Wistia media stats JSON to Silver Parquet.

    Grain:
    - One row per media stats response.
    """
    input_path = endpoint_path(raw_base_dir, "media_stats")
    output = output_path(silver_base_dir, "media_stats")

    print(f"Reading media_stats from: {input_path}")

    df = read_json_endpoint(spark, input_path)
    df = add_path_metadata(df)

    transformed_df = df.select(
        col("source_media_id").cast(StringType()).alias("media_hashed_id"),
        col("load_count").cast(LongType()).alias("load_count"),
        col("play_count").cast(LongType()).alias("play_count"),
        col("play_rate").cast(DoubleType()).alias("api_play_rate"),
        when(
            col("load_count").isNotNull() & (col("load_count") != 0),
            col("play_count").cast(DoubleType()) / col("load_count").cast(DoubleType()),
        ).otherwise(lit(None).cast(DoubleType())).alias("calculated_play_rate"),
        col("hours_watched").cast(DoubleType()).alias("hours_watched"),
        col("engagement").cast(DoubleType()).alias("engagement"),
        col("visitors").cast(LongType()).alias("visitor_count"),
        col("source_file"),
        col("ingest_date"),
        col("source_media_id"),
        col("silver_processed_at"),
    )

    print(f"Writing Silver media_stats to: {output}")
    write_parquet(transformed_df, output)
    print(f"Wrote Silver media_stats rows: {transformed_df.count()}")


def transform_visitors(spark: SparkSession, raw_base_dir: str, silver_base_dir: str) -> None:
    """
    Transform Wistia visitors JSON to Silver Parquet.

    During API exploration, the visitors endpoint behaved like an account-level
    visitor summary endpoint rather than media-specific visitor data.
    """
    input_path = endpoint_path(raw_base_dir, "visitors")
    output = output_path(silver_base_dir, "visitors")

    print(f"Reading visitors from: {input_path}")

    df = read_json_endpoint(spark, input_path)
    df = add_path_metadata(df)

    transformed_df = df.select(
        col("visitor_key").cast(StringType()).alias("visitor_key"),
        to_timestamp(col("created_at")).alias("created_at"),
        to_timestamp(col("last_active_at")).alias("last_active_at"),
        col("last_event_key").cast(StringType()).alias("last_event_key"),
        col("load_count").cast(LongType()).alias("load_count"),
        col("play_count").cast(LongType()).alias("play_count"),
        col("identifying_event_key").cast(StringType()).alias("identifying_event_key"),
        col("visitor_identity.name").cast(StringType()).alias("visitor_name"),
        col("visitor_identity.email").cast(StringType()).alias("visitor_email"),
        col("visitor_identity.org.name").cast(StringType()).alias("visitor_org_name"),
        col("visitor_identity.org.title").cast(StringType()).alias("visitor_org_title"),
        col("user_agent_details.browser").cast(StringType()).alias("browser"),
        col("user_agent_details.browser_version").cast(StringType()).alias("browser_version"),
        col("user_agent_details.platform").cast(StringType()).alias("platform"),
        col("user_agent_details.mobile").cast(BooleanType()).alias("is_mobile"),
        col("source_file"),
        col("ingest_date"),
        col("silver_processed_at"),
    )

    print(f"Writing Silver visitors to: {output}")
    write_parquet(transformed_df, output)
    print(f"Wrote Silver visitors rows: {transformed_df.count()}")


def transform_events(spark: SparkSession, raw_base_dir: str, silver_base_dir: str) -> None:
    """
    Transform Wistia events JSON to Silver Parquet.

    Grain:
    - One row per Wistia event record.

    Natural key:
    - event_key
    """
    input_path = endpoint_path(raw_base_dir, "events")
    output = output_path(silver_base_dir, "events")

    print(f"Reading events from: {input_path}")

    df = read_json_endpoint(spark, input_path)
    df = add_path_metadata(df)

    transformed_df = df.select(
        safe_col(df, "event_key", StringType()).alias("event_key"),
        to_timestamp(safe_col(df, "received_at", StringType())).alias("received_at"),
        safe_col(df, "visitor_key", StringType()).alias("visitor_key"),
        safe_col(df, "media_id", StringType()).alias("media_hashed_id"),
        safe_col(df, "media_name", StringType()).alias("media_name"),
        safe_col(df, "media_url", StringType()).alias("media_url"),
        safe_col(df, "percent_viewed", DoubleType()).alias("percent_viewed"),
        safe_col(df, "ip", StringType()).alias("ip_address"),
        safe_col(df, "country", StringType()).alias("country"),
        safe_col(df, "region", StringType()).alias("region"),
        safe_col(df, "city", StringType()).alias("city"),
        safe_col(df, "lat", DoubleType()).alias("latitude"),
        safe_col(df, "lon", DoubleType()).alias("longitude"),
        safe_col(df, "org", StringType()).alias("organization"),
        safe_col(df, "email", StringType()).alias("email"),
        safe_col(df, "embed_url", StringType()).alias("embed_url"),
        safe_col(df, "conversion_type", StringType()).alias("conversion_type"),
        safe_col(df, "conversion_data", StringType()).alias("conversion_data_json"),
        safe_col(df, "iframe_heatmap_url", StringType()).alias("iframe_heatmap_url"),
        safe_nested_col(df, "user_agent_details.browser", StringType()).alias("browser"),
        safe_nested_col(df, "user_agent_details.browser_version", StringType()).alias("browser_version"),
        safe_nested_col(df, "user_agent_details.platform", StringType()).alias("platform"),
        safe_nested_col(df, "user_agent_details.mobile", BooleanType()).alias("is_mobile"),
        safe_nested_col(df, "thumbnail.url", StringType()).alias("thumbnail_url"),
        safe_nested_col(df, "thumbnail.width", IntegerType()).alias("thumbnail_width"),
        safe_nested_col(df, "thumbnail.height", IntegerType()).alias("thumbnail_height"),
        col("source_file"),
        col("ingest_date"),
        col("source_media_id"),
        col("source_page"),
        col("silver_processed_at"),
    )

    before_count = transformed_df.count()
    transformed_df = transformed_df.dropDuplicates(["event_key"])
    after_count = transformed_df.count()

    print(f"Deduplicated events: {before_count} records before, {after_count} records after.")

    print(f"Writing Silver events to: {output}")
    write_parquet(transformed_df, output)
    print(f"Wrote Silver events rows: {after_count}")


def run_silver_transform_spark(
    raw_base_dir: str,
    silver_base_dir: str,
    endpoints: Optional[List[str]] = None,
) -> None:
    """
    Run selected Silver transforms.
    """
    spark = build_spark_session()

    endpoints = endpoints or [
        "media_metadata",
        "media_stats",
        "visitors",
        "events",
    ]

    print("Starting Wistia PySpark Silver transform")
    print(f"Raw base dir: {raw_base_dir}")
    print(f"Silver base dir: {silver_base_dir}")
    print(f"Endpoints: {endpoints}")

    if "media_metadata" in endpoints:
        transform_media_metadata(spark, raw_base_dir, silver_base_dir)

    if "media_stats" in endpoints:
        transform_media_stats(spark, raw_base_dir, silver_base_dir)

    if "visitors" in endpoints:
        transform_visitors(spark, raw_base_dir, silver_base_dir)

    if "events" in endpoints:
        transform_events(spark, raw_base_dir, silver_base_dir)

    print("Completed Wistia PySpark Silver transform")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Uses parse_known_args() so AWS Glue runtime arguments do not crash the script.
    """
    parser = argparse.ArgumentParser(
        description="Transform raw Wistia JSON to Silver Parquet using PySpark."
    )

    parser.add_argument(
        "--raw-dir",
        default="data/raw/wistia",
        help="Base raw Wistia data directory.",
    )

    parser.add_argument(
        "--silver-dir",
        default="data/silver_spark/wistia",
        help="Base Silver Wistia output directory.",
    )

    parser.add_argument(
        "--endpoint",
        action="append",
        choices=["media_metadata", "media_stats", "visitors", "events"],
        help=(
            "Endpoint to transform. Can be supplied multiple times. "
            "Defaults to all endpoints."
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

    run_silver_transform_spark(
        raw_base_dir=args.raw_dir,
        silver_base_dir=args.silver_dir,
        endpoints=args.endpoint,
    )


if __name__ == "__main__":
    main()