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


def run_gold_transform(
    silver_base_dir: str | Path,
    gold_base_dir: str | Path,
    config_path: str | Path,
) -> None:
    """
    Run Gold transforms.

    Currently implemented:
    - dim_media
    """
    run_dim_media_transform(
        silver_base_dir=silver_base_dir,
        gold_base_dir=gold_base_dir,
        config_path=config_path,
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