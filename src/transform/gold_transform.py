"""
Gold transform for Wistia Video Analytics.

Responsibilities:
- Read Silver Parquet datasets.
- Build Gold dimensional/fact tables.
- Enrich records using project configuration where needed.
- Write Gold datasets as Parquet.

Current implementation:
- Builds dim_media from Silver media_metadata plus channel mapping from config/media_config.yaml.

Expected input:

data/silver/wistia/media_metadata/data.parquet

Expected output:

data/gold/wistia/dim_media/data.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml


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


def load_media_config(config_path: str | Path) -> pd.DataFrame:
    """
    Load media/channel mapping from config/media_config.yaml.

    Expected YAML shape:

    media_ids:
      - media_id: gskhw4w4lm
        channel: YouTube
        name: Chris Face VSL The Gap Method Youtube Paid Ads
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Media config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    media_items: List[Dict[str, Any]] = config.get("media_ids", [])

    if not media_items:
        raise ValueError("media_config.yaml must contain a non-empty 'media_ids' list.")

    records = []

    for item in media_items:
        media_id = item.get("media_id")
        channel = item.get("channel")
        configured_name = item.get("name")

        if not media_id:
            raise ValueError(f"Media config item is missing media_id: {item}")

        records.append(
            {
                "media_hashed_id": media_id,
                "channel": channel,
                "configured_media_name": configured_name,
            }
        )

    return pd.DataFrame(records)


def build_dim_media(
    silver_media_metadata_df: pd.DataFrame,
    media_config_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build Gold dim_media.

    Grain:
    - One row per Wistia media/video.

    Source:
    - Silver media_metadata
    - media_config.yaml for channel mapping
    """
    df = silver_media_metadata_df.copy()

    # Keep only the columns we want in the Gold dimension.
    selected_columns = [
        "media_hashed_id",
        "wistia_numeric_id",
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
        "source_media_id",
    ]

    existing_columns = [column for column in selected_columns if column in df.columns]
    df = df[existing_columns]

    # Enrich with channel from config/media_config.yaml.
    df = df.merge(
        media_config_df,
        on="media_hashed_id",
        how="left",
    )

    # If media_name is missing for some reason, fall back to configured name.
    if "configured_media_name" in df.columns:
        df["media_name"] = df["media_name"].fillna(df["configured_media_name"])

    # Add a simple surrogate-ish key for the dimension.
    # For this project, the Wistia hashed ID is already stable and readable,
    # but a separate key makes the dimensional model clearer.
    df["media_key"] = df["media_hashed_id"]

    # One row per media_hashed_id. If multiple ingest_dates exist later,
    # keep the latest metadata snapshot by updated_at, then ingest_date.
    sort_columns = []
    if "updated_at" in df.columns:
        sort_columns.append("updated_at")
    if "ingest_date" in df.columns:
        sort_columns.append("ingest_date")

    if sort_columns:
        df = df.sort_values(sort_columns)

    df = df.drop_duplicates(subset=["media_hashed_id"], keep="last")

    # Final column order.
    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in df.columns]
    df = df[final_columns]

    return df

def build_fact_media_daily_stats(
    silver_media_stats_df: pd.DataFrame,
    media_config_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build Gold fact_media_daily_stats.

    Grain:
    - One row per media_hashed_id per ingest_date.

    Source:
    - Silver media_stats
    - media_config.yaml for channel mapping

    Notes:
    - Wistia media stats appear to be cumulative API metrics at time of extraction.
    - We store them as daily snapshots using ingest_date as snapshot_date.
    """
    df = silver_media_stats_df.copy()

    selected_columns = [
        "media_hashed_id",
        "load_count",
        "play_count",
        "api_play_rate",
        "calculated_play_rate",
        "hours_watched",
        "engagement",
        "visitor_count",
        "ingest_date",
        "source_media_id",
    ]

    existing_columns = [column for column in selected_columns if column in df.columns]
    df = df[existing_columns]

    # Enrich with channel from config/media_config.yaml.
    df = df.merge(
        media_config_df[["media_hashed_id", "channel"]],
        on="media_hashed_id",
        how="left",
    )

    # In Gold, make the snapshot meaning explicit.
    df["snapshot_date"] = df["ingest_date"]

    # Optional: daily stats fact key.
    df["media_daily_stats_key"] = (
        df["media_hashed_id"].astype(str) + "_" + df["snapshot_date"].astype(str)
    )

    # One row per media per snapshot date.
    df = df.drop_duplicates(
        subset=["media_hashed_id", "snapshot_date"],
        keep="last",
    )

    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in df.columns]
    df = df[final_columns]

    return df


def build_fact_media_engagement(
    silver_events_df: pd.DataFrame,
    media_config_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build Gold fact_media_engagement.

    Grain:
    - One row per Wistia engagement event.

    Natural key:
    - event_key

    Source:
    - Silver events
    - media_config.yaml for channel mapping

    Notes:
    - This is the main event-level analytics fact table.
    - Events are media-specific and include media_id, visitor_key, geography,
      device/browser info, and percent_viewed.
    """
    df = silver_events_df.copy()

    selected_columns = [
        "event_key",
        "received_at",
        "visitor_key",
        "media_hashed_id",
        "media_name",
        "media_url",
        "percent_viewed",
        "ip_address",
        "country",
        "region",
        "city",
        "latitude",
        "longitude",
        "organization",
        "email",
        "embed_url",
        "conversion_type",
        "conversion_data_json",
        "iframe_heatmap_url",
        "browser",
        "browser_version",
        "platform",
        "is_mobile",
        "thumbnail_url",
        "thumbnail_width",
        "thumbnail_height",
        "ingest_date",
        "source_media_id",
        "source_page",
    ]

    existing_columns = [column for column in selected_columns if column in df.columns]
    df = df[existing_columns]

    # Enrich with channel from config/media_config.yaml.
    df = df.merge(
        media_config_df[["media_hashed_id", "channel"]],
        on="media_hashed_id",
        how="left",
    )

    # Make date-level analysis easier in Athena and dashboards.
    df["event_date"] = pd.to_datetime(df["received_at"], errors="coerce", utc=True).dt.date

    # Add a fact key. The event_key itself is already a stable natural key,
    # but this keeps naming consistent with dimensional modeling conventions.
    df["media_engagement_key"] = df["event_key"]

    # Defensive dedupe. Silver already dedupes events, but Gold should protect
    # its own grain as well.
    before_count = len(df)
    df = df.drop_duplicates(subset=["event_key"], keep="last")
    after_count = len(df)

    if before_count != after_count:
        print(
            f"Deduplicated Gold fact_media_engagement: "
            f"{before_count} records before, {after_count} records after."
        )

    final_columns = [
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
    ]

    final_columns = [column for column in final_columns if column in df.columns]
    df = df[final_columns]

    return df


def build_dim_visitor(
    silver_events_df: pd.DataFrame,
    silver_visitors_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build Gold dim_visitor.

    Grain:
    - One row per visitor_key.

    Source:
    - Silver events as the primary source.
    - Silver visitors as optional supplemental enrichment.

    Notes:
    - During API exploration, the visitors endpoint appeared to return
      account-level visitor summaries rather than media-specific visitor data.
    - Therefore, media-specific visitor behavior is derived from events.
    """
    events_df = silver_events_df.copy()

    if events_df.empty:
        return pd.DataFrame()

    required_columns = [
        "visitor_key",
        "received_at",
        "media_hashed_id",
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
        "ingest_date",
    ]

    existing_columns = [column for column in required_columns if column in events_df.columns]
    events_df = events_df[existing_columns]

    events_df["received_at"] = pd.to_datetime(
        events_df["received_at"],
        errors="coerce",
        utc=True,
    )

    if "percent_viewed" in events_df.columns:
        events_df["percent_viewed"] = pd.to_numeric(
            events_df["percent_viewed"],
            errors="coerce",
        )

    # Sort so "last" values are based on latest observed event.
    events_df = events_df.sort_values(["visitor_key", "received_at"])

    aggregation_spec = {
        "received_at": ["min", "max"],
        "media_hashed_id": pd.Series.nunique,
        "percent_viewed": ["count", "mean", "max"],
        "ip_address": "last",
        "country": "last",
        "region": "last",
        "city": "last",
        "latitude": "last",
        "longitude": "last",
        "organization": "last",
        "email": "last",
        "browser": "last",
        "browser_version": "last",
        "platform": "last",
        "is_mobile": "last",
        "ingest_date": "last",
    }

    # Only aggregate columns that actually exist.
    aggregation_spec = {
        column: agg_func
        for column, agg_func in aggregation_spec.items()
        if column in events_df.columns
    }

    dim_visitor_df = events_df.groupby("visitor_key", dropna=False).agg(aggregation_spec)

    # Flatten MultiIndex columns created by aggregation.
    dim_visitor_df.columns = [
        "_".join(column_parts).strip("_")
        if isinstance(column_parts, tuple)
        else column_parts
        for column_parts in dim_visitor_df.columns
    ]

    dim_visitor_df = dim_visitor_df.reset_index()

    rename_map = {
        "received_at_min": "first_seen_at",
        "received_at_max": "last_seen_at",
        "media_hashed_id_nunique": "media_count",
        "percent_viewed_count": "event_count",
        "percent_viewed_mean": "avg_percent_viewed",
        "percent_viewed_max": "max_percent_viewed",
        "ip_address_last": "ip_address",
        "country_last": "country",
        "region_last": "region",
        "city_last": "city",
        "latitude_last": "latitude",
        "longitude_last": "longitude",
        "organization_last": "organization",
        "email_last": "email",
        "browser_last": "browser",
        "browser_version_last": "browser_version",
        "platform_last": "platform",
        "is_mobile_last": "is_mobile",
        "ingest_date_last": "ingest_date",
    }

    dim_visitor_df = dim_visitor_df.rename(columns=rename_map)

    # Optional enrichment from the visitors endpoint.
    # We only use it to fill missing fields, not to override event-derived data.
    if silver_visitors_df is not None and not silver_visitors_df.empty:
        visitors_df = silver_visitors_df.copy()

        visitor_enrichment_columns = [
            "visitor_key",
            "visitor_email",
            "visitor_org_name",
            "browser",
            "browser_version",
            "platform",
            "is_mobile",
            "created_at",
            "last_active_at",
            "load_count",
            "play_count",
        ]

        existing_visitor_columns = [
            column for column in visitor_enrichment_columns if column in visitors_df.columns
        ]

        visitors_df = visitors_df[existing_visitor_columns]
        visitors_df = visitors_df.drop_duplicates(subset=["visitor_key"], keep="last")

        dim_visitor_df = dim_visitor_df.merge(
            visitors_df,
            on="visitor_key",
            how="left",
            suffixes=("", "_visitor_summary"),
        )

        if "email" in dim_visitor_df.columns and "visitor_email" in dim_visitor_df.columns:
            dim_visitor_df["email"] = dim_visitor_df["email"].fillna(
                dim_visitor_df["visitor_email"]
            )

        if "organization" in dim_visitor_df.columns and "visitor_org_name" in dim_visitor_df.columns:
            dim_visitor_df["organization"] = dim_visitor_df["organization"].fillna(
                dim_visitor_df["visitor_org_name"]
            )

        for column in ["browser", "browser_version", "platform", "is_mobile"]:
            summary_column = f"{column}_visitor_summary"
            if column in dim_visitor_df.columns and summary_column in dim_visitor_df.columns:
                dim_visitor_df[column] = dim_visitor_df[column].fillna(
                    dim_visitor_df[summary_column]
                )

        # Use visitors endpoint timestamps only as fallback.
        if "created_at" in dim_visitor_df.columns:
            dim_visitor_df["created_at"] = pd.to_datetime(
                dim_visitor_df["created_at"],
                errors="coerce",
                utc=True,
            )
            dim_visitor_df["first_seen_at"] = dim_visitor_df["first_seen_at"].fillna(
                dim_visitor_df["created_at"]
            )

        if "last_active_at" in dim_visitor_df.columns:
            dim_visitor_df["last_active_at"] = pd.to_datetime(
                dim_visitor_df["last_active_at"],
                errors="coerce",
                utc=True,
            )
            dim_visitor_df["last_seen_at"] = dim_visitor_df["last_seen_at"].fillna(
                dim_visitor_df["last_active_at"]
            )

    dim_visitor_df["visitor_key_dim"] = dim_visitor_df["visitor_key"]

    final_columns = [
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
    ]

    final_columns = [
        column for column in final_columns if column in dim_visitor_df.columns
    ]

    dim_visitor_df = dim_visitor_df[final_columns]

    return dim_visitor_df


def run_dim_media_transform(
    silver_base_dir: str | Path,
    gold_base_dir: str | Path,
    config_path: str | Path,
) -> None:
    """
    Build and write Gold dim_media.
    """
    silver_base_dir = Path(silver_base_dir)
    gold_base_dir = Path(gold_base_dir)

    silver_media_metadata_path = silver_base_dir / "media_metadata" / "data.parquet"
    gold_dim_media_path = gold_base_dir / "dim_media" / "data.parquet"

    silver_media_metadata_df = read_parquet(silver_media_metadata_path)
    media_config_df = load_media_config(config_path)

    dim_media_df = build_dim_media(
        silver_media_metadata_df=silver_media_metadata_df,
        media_config_df=media_config_df,
    )

    write_parquet(dim_media_df, gold_dim_media_path)

    print(f"Wrote Gold dim_media: {gold_dim_media_path}")
    print(f"Rows: {len(dim_media_df)}")
    print(f"Columns: {list(dim_media_df.columns)}")


def run_fact_media_daily_stats_transform(
    silver_base_dir: str | Path,
    gold_base_dir: str | Path,
    config_path: str | Path,
) -> None:
    """
    Build and write Gold fact_media_daily_stats.
    """
    silver_base_dir = Path(silver_base_dir)
    gold_base_dir = Path(gold_base_dir)

    silver_media_stats_path = silver_base_dir / "media_stats" / "data.parquet"
    gold_fact_media_daily_stats_path = (
        gold_base_dir / "fact_media_daily_stats" / "data.parquet"
    )

    silver_media_stats_df = read_parquet(silver_media_stats_path)
    media_config_df = load_media_config(config_path)

    fact_media_daily_stats_df = build_fact_media_daily_stats(
        silver_media_stats_df=silver_media_stats_df,
        media_config_df=media_config_df,
    )

    write_parquet(fact_media_daily_stats_df, gold_fact_media_daily_stats_path)

    print(f"Wrote Gold fact_media_daily_stats: {gold_fact_media_daily_stats_path}")
    print(f"Rows: {len(fact_media_daily_stats_df)}")
    print(f"Columns: {list(fact_media_daily_stats_df.columns)}")


def run_fact_media_engagement_transform(
    silver_base_dir: str | Path,
    gold_base_dir: str | Path,
    config_path: str | Path,
) -> None:
    """
    Build and write Gold fact_media_engagement.
    """
    silver_base_dir = Path(silver_base_dir)
    gold_base_dir = Path(gold_base_dir)

    silver_events_path = silver_base_dir / "events" / "data.parquet"
    gold_fact_media_engagement_path = (
        gold_base_dir / "fact_media_engagement" / "data.parquet"
    )

    silver_events_df = read_parquet(silver_events_path)
    media_config_df = load_media_config(config_path)

    fact_media_engagement_df = build_fact_media_engagement(
        silver_events_df=silver_events_df,
        media_config_df=media_config_df,
    )

    write_parquet(fact_media_engagement_df, gold_fact_media_engagement_path)

    print(f"Wrote Gold fact_media_engagement: {gold_fact_media_engagement_path}")
    print(f"Rows: {len(fact_media_engagement_df)}")
    print(f"Columns: {list(fact_media_engagement_df.columns)}")


def run_dim_visitor_transform(
    silver_base_dir: str | Path,
    gold_base_dir: str | Path,
) -> None:
    """
    Build and write Gold dim_visitor.
    """
    silver_base_dir = Path(silver_base_dir)
    gold_base_dir = Path(gold_base_dir)

    silver_events_path = silver_base_dir / "events" / "data.parquet"
    silver_visitors_path = silver_base_dir / "visitors" / "data.parquet"
    gold_dim_visitor_path = gold_base_dir / "dim_visitor" / "data.parquet"

    silver_events_df = read_parquet(silver_events_path)

    if silver_visitors_path.exists():
        silver_visitors_df = read_parquet(silver_visitors_path)
    else:
        silver_visitors_df = None

    dim_visitor_df = build_dim_visitor(
        silver_events_df=silver_events_df,
        silver_visitors_df=silver_visitors_df,
    )

    write_parquet(dim_visitor_df, gold_dim_visitor_path)

    print(f"Wrote Gold dim_visitor: {gold_dim_visitor_path}")
    print(f"Rows: {len(dim_visitor_df)}")
    print(f"Columns: {list(dim_visitor_df.columns)}")


def run_gold_transform(
    silver_base_dir: str | Path,
    gold_base_dir: str | Path,
    config_path: str | Path,
) -> None:
    """
    Run Gold transforms.

    Currently implemented:
    - dim_media
    - fact_media_daily_stats
    - fact_media_engagement
    - dim_visitor
    """
    run_dim_media_transform(
        silver_base_dir=silver_base_dir,
        gold_base_dir=gold_base_dir,
        config_path=config_path,
    )

    run_fact_media_daily_stats_transform(
        silver_base_dir=silver_base_dir,
        gold_base_dir=gold_base_dir,
        config_path=config_path,
    )

    run_fact_media_engagement_transform(
        silver_base_dir=silver_base_dir,
        gold_base_dir=gold_base_dir,
        config_path=config_path,
    )

    run_dim_visitor_transform(
        silver_base_dir=silver_base_dir,
        gold_base_dir=gold_base_dir,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform Silver Wistia data to Gold Parquet.")

    parser.add_argument(
        "--silver-dir",
        default="data/silver/wistia",
        help="Base Silver Wistia data directory.",
    )

    parser.add_argument(
        "--gold-dir",
        default="data/gold/wistia",
        help="Base Gold Wistia data directory.",
    )

    parser.add_argument(
        "--config",
        default="config/media_config.yaml",
        help="Path to media config YAML file.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_gold_transform(
        silver_base_dir=args.silver_dir,
        gold_base_dir=args.gold_dir,
        config_path=args.config,
    )


if __name__ == "__main__":
    main()