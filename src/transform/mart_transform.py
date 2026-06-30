"""
Mart transform for Wistia Video Analytics.

Responsibilities:
- Read Gold Parquet datasets.
- Build dashboard/query-friendly analytics marts.
- Write Mart datasets as Parquet.

Expected Gold inputs:

data/gold/wistia/
├── dim_media/data.parquet
├── dim_visitor/data.parquet
├── fact_media_daily_stats/data.parquet
└── fact_media_engagement/data.parquet

Expected Mart outputs:

data/marts/wistia/
├── mart_channel_performance/data.parquet
├── mart_geo_engagement/data.parquet
├── mart_device_browser/data.parquet
└── mart_visitor_engagement/data.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def read_parquet(path: str | Path) -> pd.DataFrame:
    """
    Read a Parquet file into a DataFrame.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    return pd.read_parquet(path)


def write_parquet(df: pd.DataFrame, output_path: str | Path) -> None:
    """
    Write a DataFrame to Parquet, creating parent directories as needed.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, engine="pyarrow", index=False)


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """
    Safely divide two pandas Series.

    Returns 0 where denominator is 0 or null.
    """
    result = numerator / denominator
    result = result.replace([float("inf"), float("-inf")], pd.NA)
    return result.fillna(0)


def build_mart_channel_performance(
    fact_media_daily_stats_df: pd.DataFrame,
    fact_media_engagement_df: pd.DataFrame,
    dim_media_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build channel performance mart.

    Grain:
    - One row per channel per snapshot_date.

    Business questions:
    - How does YouTube perform vs Facebook?
    - Which channel has better play rate?
    - Which channel has higher average percent viewed?
    - Which channel has more unique visitors and engagement events?
    """
    stats_df = fact_media_daily_stats_df.copy()
    engagement_df = fact_media_engagement_df.copy()
    media_df = dim_media_df.copy()

    if "snapshot_date" not in stats_df.columns:
        stats_df["snapshot_date"] = stats_df["ingest_date"]

    if "event_date" in engagement_df.columns:
        engagement_df["event_date"] = pd.to_datetime(
            engagement_df["event_date"],
            errors="coerce",
        ).dt.date

    # Aggregate daily snapshot stats by channel.
    stats_agg = (
        stats_df.groupby(["channel", "snapshot_date"], dropna=False)
        .agg(
            media_count=("media_hashed_id", "nunique"),
            total_load_count=("load_count", "sum"),
            total_play_count=("play_count", "sum"),
            total_hours_watched=("hours_watched", "sum"),
            avg_api_play_rate=("api_play_rate", "mean"),
            avg_calculated_play_rate=("calculated_play_rate", "mean"),
            avg_engagement=("engagement", "mean"),
            total_visitor_count=("visitor_count", "sum"),
        )
        .reset_index()
    )

    # Aggregate event-level engagement by channel.
    engagement_agg = (
        engagement_df.groupby(["channel"], dropna=False)
        .agg(
            engagement_event_count=("event_key", "count"),
            unique_event_visitors=("visitor_key", "nunique"),
            avg_percent_viewed=("percent_viewed", "mean"),
            max_percent_viewed=("percent_viewed", "max"),
            completed_view_count=("percent_viewed", lambda x: (x >= 0.95).sum()),
            mobile_event_count=("is_mobile", lambda x: (x == True).sum()),
        )
        .reset_index()
    )

    engagement_agg["completion_rate"] = safe_divide(
        engagement_agg["completed_view_count"],
        engagement_agg["engagement_event_count"],
    )

    engagement_agg["mobile_event_rate"] = safe_divide(
        engagement_agg["mobile_event_count"],
        engagement_agg["engagement_event_count"],
    )

    # Add media names for readability.
    media_names = (
        media_df.groupby("channel", dropna=False)
        .agg(
            media_names=("media_name", lambda x: " | ".join(sorted(x.dropna().unique()))),
        )
        .reset_index()
    )

    mart_df = stats_agg.merge(
        engagement_agg,
        on="channel",
        how="left",
    ).merge(
        media_names,
        on="channel",
        how="left",
    )

    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in mart_df.columns]
    mart_df = mart_df[final_columns]

    return mart_df


def build_mart_geo_engagement(
    fact_media_engagement_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build geography engagement mart.

    Grain:
    - One row per country/region/city/channel.

    Business questions:
    - Where are viewers located?
    - Which geographies have the highest engagement?
    - Which geographies produce completed views?
    """
    df = fact_media_engagement_df.copy()

    group_columns = [
        "country",
        "region",
        "city",
        "channel",
    ]

    existing_group_columns = [column for column in group_columns if column in df.columns]

    mart_df = (
        df.groupby(existing_group_columns, dropna=False)
        .agg(
            engagement_event_count=("event_key", "count"),
            unique_visitors=("visitor_key", "nunique"),
            avg_percent_viewed=("percent_viewed", "mean"),
            max_percent_viewed=("percent_viewed", "max"),
            completed_view_count=("percent_viewed", lambda x: (x >= 0.95).sum()),
            first_event_at=("received_at", "min"),
            last_event_at=("received_at", "max"),
        )
        .reset_index()
    )

    mart_df["completion_rate"] = safe_divide(
        mart_df["completed_view_count"],
        mart_df["engagement_event_count"],
    )

    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in mart_df.columns]
    mart_df = mart_df[final_columns]

    return mart_df


def build_mart_device_browser(
    fact_media_engagement_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build device/browser engagement mart.

    Grain:
    - One row per platform/browser/mobile flag/channel.

    Business questions:
    - Which devices and browsers are viewers using?
    - Is engagement different on mobile vs desktop?
    """
    df = fact_media_engagement_df.copy()

    group_columns = [
        "platform",
        "browser",
        "browser_version",
        "is_mobile",
        "channel",
    ]

    existing_group_columns = [column for column in group_columns if column in df.columns]

    mart_df = (
        df.groupby(existing_group_columns, dropna=False)
        .agg(
            engagement_event_count=("event_key", "count"),
            unique_visitors=("visitor_key", "nunique"),
            avg_percent_viewed=("percent_viewed", "mean"),
            max_percent_viewed=("percent_viewed", "max"),
            completed_view_count=("percent_viewed", lambda x: (x >= 0.95).sum()),
            first_event_at=("received_at", "min"),
            last_event_at=("received_at", "max"),
        )
        .reset_index()
    )

    mart_df["completion_rate"] = safe_divide(
        mart_df["completed_view_count"],
        mart_df["engagement_event_count"],
    )

    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in mart_df.columns]
    mart_df = mart_df[final_columns]

    return mart_df


def build_mart_visitor_engagement(
    dim_visitor_df: pd.DataFrame,
    fact_media_engagement_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build visitor engagement mart.

    Grain:
    - One row per visitor_key.

    Business questions:
    - Which visitors are most engaged?
    - Which visitors completed the video?
    - Which visitors watched across multiple media/channel records?
    """
    visitors_df = dim_visitor_df.copy()
    engagement_df = fact_media_engagement_df.copy()

    channel_summary = (
        engagement_df.groupby("visitor_key", dropna=False)
        .agg(
            channels_seen=("channel", lambda x: " | ".join(sorted(x.dropna().unique()))),
            media_seen=("media_hashed_id", lambda x: " | ".join(sorted(x.dropna().unique()))),
            completed_event_count=("percent_viewed", lambda x: (x >= 0.95).sum()),
        )
        .reset_index()
    )

    mart_df = visitors_df.merge(
        channel_summary,
        on="visitor_key",
        how="left",
    )

    mart_df["completion_rate"] = safe_divide(
        mart_df["completed_event_count"],
        mart_df["event_count"],
    )

    # Simple engagement banding for dashboard filters.
    mart_df["engagement_segment"] = pd.cut(
        mart_df["avg_percent_viewed"],
        bins=[-0.01, 0.25, 0.50, 0.75, 1.0],
        labels=[
            "0-25%",
            "26-50%",
            "51-75%",
            "76-100%",
        ],
    ).astype(str)

    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in mart_df.columns]
    mart_df = mart_df[final_columns]

    return mart_df


def run_mart_channel_performance_transform(
    gold_base_dir: str | Path,
    mart_base_dir: str | Path,
) -> None:
    """
    Build and write mart_channel_performance.
    """
    gold_base_dir = Path(gold_base_dir)
    mart_base_dir = Path(mart_base_dir)

    dim_media_df = read_parquet(gold_base_dir / "dim_media" / "data.parquet")
    fact_media_daily_stats_df = read_parquet(
        gold_base_dir / "fact_media_daily_stats" / "data.parquet"
    )
    fact_media_engagement_df = read_parquet(
        gold_base_dir / "fact_media_engagement" / "data.parquet"
    )

    mart_df = build_mart_channel_performance(
        fact_media_daily_stats_df=fact_media_daily_stats_df,
        fact_media_engagement_df=fact_media_engagement_df,
        dim_media_df=dim_media_df,
    )

    output_path = mart_base_dir / "mart_channel_performance" / "data.parquet"
    write_parquet(mart_df, output_path)

    print(f"Wrote mart_channel_performance: {output_path}")
    print(f"Rows: {len(mart_df)}")
    print(f"Columns: {list(mart_df.columns)}")


def run_mart_geo_engagement_transform(
    gold_base_dir: str | Path,
    mart_base_dir: str | Path,
) -> None:
    """
    Build and write mart_geo_engagement.
    """
    gold_base_dir = Path(gold_base_dir)
    mart_base_dir = Path(mart_base_dir)

    fact_media_engagement_df = read_parquet(
        gold_base_dir / "fact_media_engagement" / "data.parquet"
    )

    mart_df = build_mart_geo_engagement(
        fact_media_engagement_df=fact_media_engagement_df,
    )

    output_path = mart_base_dir / "mart_geo_engagement" / "data.parquet"
    write_parquet(mart_df, output_path)

    print(f"Wrote mart_geo_engagement: {output_path}")
    print(f"Rows: {len(mart_df)}")
    print(f"Columns: {list(mart_df.columns)}")


def run_mart_device_browser_transform(
    gold_base_dir: str | Path,
    mart_base_dir: str | Path,
) -> None:
    """
    Build and write mart_device_browser.
    """
    gold_base_dir = Path(gold_base_dir)
    mart_base_dir = Path(mart_base_dir)

    fact_media_engagement_df = read_parquet(
        gold_base_dir / "fact_media_engagement" / "data.parquet"
    )

    mart_df = build_mart_device_browser(
        fact_media_engagement_df=fact_media_engagement_df,
    )

    output_path = mart_base_dir / "mart_device_browser" / "data.parquet"
    write_parquet(mart_df, output_path)

    print(f"Wrote mart_device_browser: {output_path}")
    print(f"Rows: {len(mart_df)}")
    print(f"Columns: {list(mart_df.columns)}")


def run_mart_visitor_engagement_transform(
    gold_base_dir: str | Path,
    mart_base_dir: str | Path,
) -> None:
    """
    Build and write mart_visitor_engagement.
    """
    gold_base_dir = Path(gold_base_dir)
    mart_base_dir = Path(mart_base_dir)

    dim_visitor_df = read_parquet(gold_base_dir / "dim_visitor" / "data.parquet")
    fact_media_engagement_df = read_parquet(
        gold_base_dir / "fact_media_engagement" / "data.parquet"
    )

    mart_df = build_mart_visitor_engagement(
        dim_visitor_df=dim_visitor_df,
        fact_media_engagement_df=fact_media_engagement_df,
    )

    output_path = mart_base_dir / "mart_visitor_engagement" / "data.parquet"
    write_parquet(mart_df, output_path)

    print(f"Wrote mart_visitor_engagement: {output_path}")
    print(f"Rows: {len(mart_df)}")
    print(f"Columns: {list(mart_df.columns)}")


def run_mart_transform(
    gold_base_dir: str | Path,
    mart_base_dir: str | Path,
) -> None:
    """
    Run all Mart transforms.
    """
    run_mart_channel_performance_transform(
        gold_base_dir=gold_base_dir,
        mart_base_dir=mart_base_dir,
    )

    run_mart_geo_engagement_transform(
        gold_base_dir=gold_base_dir,
        mart_base_dir=mart_base_dir,
    )

    run_mart_device_browser_transform(
        gold_base_dir=gold_base_dir,
        mart_base_dir=mart_base_dir,
    )

    run_mart_visitor_engagement_transform(
        gold_base_dir=gold_base_dir,
        mart_base_dir=mart_base_dir,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform Gold Wistia data to Mart Parquet.")

    parser.add_argument(
        "--gold-dir",
        default="data/gold/wistia",
        help="Base Gold Wistia data directory.",
    )

    parser.add_argument(
        "--mart-dir",
        default="data/marts/wistia",
        help="Base Mart Wistia data directory.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_mart_transform(
        gold_base_dir=args.gold_dir,
        mart_base_dir=args.mart_dir,
    )


if __name__ == "__main__":
    main()