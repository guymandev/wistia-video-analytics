"""
Data quality validation for Wistia Video Analytics outputs.

Responsibilities:
- Validate that expected Gold and Mart Parquet outputs exist.
- Validate basic table-level row counts.
- Validate key constraints and important non-null fields.
- Validate expected dimensional-model assumptions.

This script is intended for local development first, and can later be adapted
for AWS/S3-based validation after Glue jobs run.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List

import pandas as pd


@dataclass
class ValidationResult:
    """
    Represents the result of a single validation check.
    """

    check_name: str
    passed: bool
    message: str


def read_parquet(path: str | Path) -> pd.DataFrame:
    """
    Read a Parquet file into a DataFrame.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    return pd.read_parquet(path)


def add_result(
    results: List[ValidationResult],
    check_name: str,
    passed: bool,
    message: str,
) -> None:
    """
    Append a validation result.
    """
    results.append(
        ValidationResult(
            check_name=check_name,
            passed=passed,
            message=message,
        )
    )


def validate_file_exists(
    results: List[ValidationResult],
    path: str | Path,
    check_name: str,
) -> None:
    """
    Validate that an expected output file exists.
    """
    path = Path(path)

    add_result(
        results=results,
        check_name=check_name,
        passed=path.exists(),
        message=f"Expected file: {path}",
    )


def validate_non_empty(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Validate that a DataFrame is not empty.
    """
    add_result(
        results=results,
        check_name=f"{table_name}: non-empty",
        passed=not df.empty,
        message=f"{table_name} row count: {len(df)}",
    )


def validate_expected_row_count(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    expected_count: int,
) -> None:
    """
    Validate exact expected row count.
    """
    actual_count = len(df)

    add_result(
        results=results,
        check_name=f"{table_name}: expected row count",
        passed=actual_count == expected_count,
        message=f"Expected {expected_count}, found {actual_count}",
    )


def validate_column_exists(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    column_name: str,
) -> None:
    """
    Validate that a required column exists.
    """
    add_result(
        results=results,
        check_name=f"{table_name}: column exists: {column_name}",
        passed=column_name in df.columns,
        message=f"Columns available: {list(df.columns)}",
    )


def validate_required_columns(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    required_columns: list[str],
) -> None:
    """
    Validate that required columns exist.
    """
    missing_columns = [column for column in required_columns if column not in df.columns]

    add_result(
        results=results,
        check_name=f"{table_name}: required columns",
        passed=not missing_columns,
        message=(
            "All required columns found."
            if not missing_columns
            else f"Missing columns: {missing_columns}"
        ),
    )


def validate_no_nulls(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    column_name: str,
) -> None:
    """
    Validate that a column has no null values.
    """
    if column_name not in df.columns:
        add_result(
            results=results,
            check_name=f"{table_name}: no nulls: {column_name}",
            passed=False,
            message=f"Column does not exist: {column_name}",
        )
        return

    null_count = int(df[column_name].isna().sum())

    add_result(
        results=results,
        check_name=f"{table_name}: no nulls: {column_name}",
        passed=null_count == 0,
        message=f"Null count for {column_name}: {null_count}",
    )


def validate_unique(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    column_name: str,
) -> None:
    """
    Validate that a column is unique.
    """
    if column_name not in df.columns:
        add_result(
            results=results,
            check_name=f"{table_name}: unique: {column_name}",
            passed=False,
            message=f"Column does not exist: {column_name}",
        )
        return

    duplicate_count = int(df[column_name].duplicated().sum())

    add_result(
        results=results,
        check_name=f"{table_name}: unique: {column_name}",
        passed=duplicate_count == 0,
        message=f"Duplicate count for {column_name}: {duplicate_count}",
    )


def validate_allowed_values(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    column_name: str,
    allowed_values: set[str],
) -> None:
    """
    Validate that a column only contains expected values.

    Null values are treated as invalid for this check.
    """
    if column_name not in df.columns:
        add_result(
            results=results,
            check_name=f"{table_name}: allowed values: {column_name}",
            passed=False,
            message=f"Column does not exist: {column_name}",
        )
        return

    actual_values = set(df[column_name].dropna().astype(str).unique())
    null_count = int(df[column_name].isna().sum())
    unexpected_values = actual_values - allowed_values

    passed = not unexpected_values and null_count == 0

    add_result(
        results=results,
        check_name=f"{table_name}: allowed values: {column_name}",
        passed=passed,
        message=(
            f"Allowed values: {sorted(allowed_values)}; "
            f"actual values: {sorted(actual_values)}; "
            f"null count: {null_count}; "
            f"unexpected values: {sorted(unexpected_values)}"
        ),
    )


def validate_numeric_range(
    results: List[ValidationResult],
    df: pd.DataFrame,
    table_name: str,
    column_name: str,
    min_value: float,
    max_value: float,
) -> None:
    """
    Validate that numeric column values fall within an inclusive range.

    Null values are ignored for this check.
    """
    if column_name not in df.columns:
        add_result(
            results=results,
            check_name=f"{table_name}: numeric range: {column_name}",
            passed=False,
            message=f"Column does not exist: {column_name}",
        )
        return

    series = pd.to_numeric(df[column_name], errors="coerce").dropna()

    if series.empty:
        add_result(
            results=results,
            check_name=f"{table_name}: numeric range: {column_name}",
            passed=False,
            message=f"No numeric values found in {column_name}",
        )
        return

    below_count = int((series < min_value).sum())
    above_count = int((series > max_value).sum())

    add_result(
        results=results,
        check_name=f"{table_name}: numeric range: {column_name}",
        passed=below_count == 0 and above_count == 0,
        message=(
            f"{column_name} range observed: {series.min()} to {series.max()}; "
            f"below min count: {below_count}; above max count: {above_count}"
        ),
    )


def validate_gold_outputs(gold_base_dir: str | Path) -> List[ValidationResult]:
    """
    Validate Gold output tables.
    """
    gold_base_dir = Path(gold_base_dir)
    results: List[ValidationResult] = []

    expected_paths = {
        "dim_media": gold_base_dir / "dim_media" / "data.parquet",
        "dim_visitor": gold_base_dir / "dim_visitor" / "data.parquet",
        "fact_media_daily_stats": gold_base_dir
        / "fact_media_daily_stats"
        / "data.parquet",
        "fact_media_engagement": gold_base_dir
        / "fact_media_engagement"
        / "data.parquet",
    }

    for table_name, path in expected_paths.items():
        validate_file_exists(
            results=results,
            path=path,
            check_name=f"Gold file exists: {table_name}",
        )

    # If files do not exist, stop before reading.
    if not all(path.exists() for path in expected_paths.values()):
        return results

    dim_media_df = read_parquet(expected_paths["dim_media"])
    dim_visitor_df = read_parquet(expected_paths["dim_visitor"])
    fact_media_daily_stats_df = read_parquet(expected_paths["fact_media_daily_stats"])
    fact_media_engagement_df = read_parquet(expected_paths["fact_media_engagement"])

    # Basic non-empty checks.
    for table_name, df in [
        ("dim_media", dim_media_df),
        ("dim_visitor", dim_visitor_df),
        ("fact_media_daily_stats", fact_media_daily_stats_df),
        ("fact_media_engagement", fact_media_engagement_df),
    ]:
        validate_non_empty(results, df, table_name)

    # Expected two media IDs for this project.
    validate_expected_row_count(results, dim_media_df, "dim_media", expected_count=2)
    validate_expected_row_count(
        results,
        fact_media_daily_stats_df,
        "fact_media_daily_stats",
        expected_count=2,
    )

    # Required columns.
    validate_required_columns(
        results,
        dim_media_df,
        "dim_media",
        [
            "media_key",
            "media_hashed_id",
            "channel",
            "media_name",
            "duration_seconds",
            "status",
            "ingest_date",
        ],
    )

    validate_required_columns(
        results,
        dim_visitor_df,
        "dim_visitor",
        [
            "visitor_key",
            "first_seen_at",
            "last_seen_at",
            "event_count",
            "avg_percent_viewed",
            "media_count",
            "ingest_date",
        ],
    )

    validate_required_columns(
        results,
        fact_media_daily_stats_df,
        "fact_media_daily_stats",
        [
            "media_daily_stats_key",
            "media_hashed_id",
            "channel",
            "snapshot_date",
            "load_count",
            "play_count",
            "hours_watched",
            "engagement",
            "visitor_count",
        ],
    )

    validate_required_columns(
        results,
        fact_media_engagement_df,
        "fact_media_engagement",
        [
            "media_engagement_key",
            "event_key",
            "received_at",
            "event_date",
            "media_hashed_id",
            "channel",
            "visitor_key",
            "percent_viewed",
            "ingest_date",
        ],
    )

    # Key uniqueness / not-null checks.
    validate_no_nulls(results, dim_media_df, "dim_media", "media_hashed_id")
    validate_unique(results, dim_media_df, "dim_media", "media_hashed_id")

    validate_no_nulls(results, dim_visitor_df, "dim_visitor", "visitor_key")
    validate_unique(results, dim_visitor_df, "dim_visitor", "visitor_key")

    validate_no_nulls(
        results,
        fact_media_daily_stats_df,
        "fact_media_daily_stats",
        "media_daily_stats_key",
    )
    validate_unique(
        results,
        fact_media_daily_stats_df,
        "fact_media_daily_stats",
        "media_daily_stats_key",
    )

    validate_no_nulls(
        results,
        fact_media_engagement_df,
        "fact_media_engagement",
        "event_key",
    )
    validate_unique(
        results,
        fact_media_engagement_df,
        "fact_media_engagement",
        "event_key",
    )

    # Important relationship fields.
    validate_no_nulls(results, fact_media_engagement_df, "fact_media_engagement", "media_hashed_id")
    validate_no_nulls(results, fact_media_engagement_df, "fact_media_engagement", "channel")
    validate_no_nulls(results, fact_media_engagement_df, "fact_media_engagement", "visitor_key")

    # Project-specific allowed channels.
    allowed_channels = {"YouTube", "Facebook"}
    validate_allowed_values(results, dim_media_df, "dim_media", "channel", allowed_channels)
    validate_allowed_values(
        results,
        fact_media_daily_stats_df,
        "fact_media_daily_stats",
        "channel",
        allowed_channels,
    )
    validate_allowed_values(
        results,
        fact_media_engagement_df,
        "fact_media_engagement",
        "channel",
        allowed_channels,
    )

    # Expected percent_viewed range.
    validate_numeric_range(
        results,
        fact_media_engagement_df,
        "fact_media_engagement",
        "percent_viewed",
        min_value=0.0,
        max_value=1.0,
    )

    validate_numeric_range(
        results,
        dim_visitor_df,
        "dim_visitor",
        "avg_percent_viewed",
        min_value=0.0,
        max_value=1.0,
    )

    validate_numeric_range(
        results,
        dim_visitor_df,
        "dim_visitor",
        "max_percent_viewed",
        min_value=0.0,
        max_value=1.0,
    )

    return results


def validate_mart_outputs(mart_base_dir: str | Path) -> List[ValidationResult]:
    """
    Validate Mart output tables.
    """
    mart_base_dir = Path(mart_base_dir)
    results: List[ValidationResult] = []

    expected_paths = {
        "mart_channel_performance": mart_base_dir
        / "mart_channel_performance"
        / "data.parquet",
        "mart_geo_engagement": mart_base_dir
        / "mart_geo_engagement"
        / "data.parquet",
        "mart_device_browser": mart_base_dir
        / "mart_device_browser"
        / "data.parquet",
        "mart_visitor_engagement": mart_base_dir
        / "mart_visitor_engagement"
        / "data.parquet",
    }

    for table_name, path in expected_paths.items():
        validate_file_exists(
            results=results,
            path=path,
            check_name=f"Mart file exists: {table_name}",
        )

    # If files do not exist, stop before reading.
    if not all(path.exists() for path in expected_paths.values()):
        return results

    mart_channel_performance_df = read_parquet(expected_paths["mart_channel_performance"])
    mart_geo_engagement_df = read_parquet(expected_paths["mart_geo_engagement"])
    mart_device_browser_df = read_parquet(expected_paths["mart_device_browser"])
    mart_visitor_engagement_df = read_parquet(expected_paths["mart_visitor_engagement"])

    # Basic non-empty checks.
    for table_name, df in [
        ("mart_channel_performance", mart_channel_performance_df),
        ("mart_geo_engagement", mart_geo_engagement_df),
        ("mart_device_browser", mart_device_browser_df),
        ("mart_visitor_engagement", mart_visitor_engagement_df),
    ]:
        validate_non_empty(results, df, table_name)

    # Channel performance should have one row per channel for the current local run.
    validate_expected_row_count(
        results,
        mart_channel_performance_df,
        "mart_channel_performance",
        expected_count=2,
    )

    validate_required_columns(
        results,
        mart_channel_performance_df,
        "mart_channel_performance",
        [
            "snapshot_date",
            "channel",
            "media_count",
            "total_load_count",
            "total_play_count",
            "avg_engagement",
            "engagement_event_count",
            "unique_event_visitors",
            "completion_rate",
            "mobile_event_rate",
        ],
    )

    validate_required_columns(
        results,
        mart_geo_engagement_df,
        "mart_geo_engagement",
        [
            "country",
            "region",
            "city",
            "channel",
            "engagement_event_count",
            "unique_visitors",
            "avg_percent_viewed",
            "completion_rate",
        ],
    )

    validate_required_columns(
        results,
        mart_device_browser_df,
        "mart_device_browser",
        [
            "platform",
            "browser",
            "is_mobile",
            "channel",
            "engagement_event_count",
            "unique_visitors",
            "avg_percent_viewed",
            "completion_rate",
        ],
    )

    validate_required_columns(
        results,
        mart_visitor_engagement_df,
        "mart_visitor_engagement",
        [
            "visitor_key",
            "event_count",
            "avg_percent_viewed",
            "max_percent_viewed",
            "completed_event_count",
            "completion_rate",
            "engagement_segment",
            "channels_seen",
            "media_seen",
        ],
    )

    allowed_channels = {"YouTube", "Facebook"}

    validate_allowed_values(
        results,
        mart_channel_performance_df,
        "mart_channel_performance",
        "channel",
        allowed_channels,
    )

    validate_allowed_values(
        results,
        mart_geo_engagement_df,
        "mart_geo_engagement",
        "channel",
        allowed_channels,
    )

    validate_allowed_values(
        results,
        mart_device_browser_df,
        "mart_device_browser",
        "channel",
        allowed_channels,
    )

    # Important ranges.
    for table_name, df in [
        ("mart_channel_performance", mart_channel_performance_df),
        ("mart_geo_engagement", mart_geo_engagement_df),
        ("mart_device_browser", mart_device_browser_df),
        ("mart_visitor_engagement", mart_visitor_engagement_df),
    ]:
        if "completion_rate" in df.columns:
            validate_numeric_range(
                results,
                df,
                table_name,
                "completion_rate",
                min_value=0.0,
                max_value=1.0,
            )

        if "avg_percent_viewed" in df.columns:
            validate_numeric_range(
                results,
                df,
                table_name,
                "avg_percent_viewed",
                min_value=0.0,
                max_value=1.0,
            )

    validate_unique(
        results,
        mart_visitor_engagement_df,
        "mart_visitor_engagement",
        "visitor_key",
    )

    return results


def print_validation_results(results: List[ValidationResult]) -> None:
    """
    Print validation results in a readable format.
    """
    passed_count = sum(result.passed for result in results)
    failed_count = len(results) - passed_count

    print()
    print("=" * 80)
    print("DATA QUALITY VALIDATION RESULTS")
    print("=" * 80)
    print(f"Total checks: {len(results)}")
    print(f"Passed:       {passed_count}")
    print(f"Failed:       {failed_count}")

    print()
    print("-" * 80)
    print("DETAILS")
    print("-" * 80)

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.check_name} -- {result.message}")


def run_validation(
    gold_base_dir: str | Path,
    mart_base_dir: str | Path,
) -> bool:
    """
    Run all validations.

    Returns:
    - True if all checks pass.
    - False otherwise.
    """
    results: List[ValidationResult] = []

    results.extend(validate_gold_outputs(gold_base_dir))
    results.extend(validate_mart_outputs(mart_base_dir))

    print_validation_results(results)

    return all(result.passed for result in results)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Validate local Wistia Gold and Mart Parquet outputs."
    )

    parser.add_argument(
        "--gold-dir",
        default="data/gold/wistia",
        help="Base Gold Wistia output directory.",
    )

    parser.add_argument(
        "--mart-dir",
        default="data/marts/wistia",
        help="Base Mart Wistia output directory.",
    )

    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint.
    """
    args = parse_args()

    passed = run_validation(
        gold_base_dir=args.gold_dir,
        mart_base_dir=args.mart_dir,
    )

    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()