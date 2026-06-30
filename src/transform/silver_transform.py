"""
Silver transform for Wistia Video Analytics.

Responsibilities:
- Read raw Wistia JSON files from the local raw landing zone.
- Flatten nested JSON structures.
- Normalize column names.
- Cast common data types.
- Add ingest metadata.
- Write Silver datasets as Parquet.

Raw input example:

data/raw/wistia/
├── media_metadata/
├── media_stats/
├── visitors/
└── events/

Silver output example:

data/silver/wistia/
├── media_metadata/
├── media_stats/
├── visitors/
└── events/

This local version mirrors the transformation logic that will later run in AWS Glue.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


def read_json_file(path: str | Path) -> Any:
    """
    Read a JSON file and return the parsed payload.
    """
    path = Path(path)

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_parquet(df: pd.DataFrame, output_path: str | Path) -> None:
    """
    Write a DataFrame to Parquet, creating parent directories as needed.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, engine="pyarrow", index=False)


def snake_case(value: str) -> str:
    """
    Convert a string to simple snake_case.
    """
    value = value.strip()
    value = re.sub(r"[^0-9a-zA-Z]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize all DataFrame column names to snake_case.
    """
    df = df.copy()
    df.columns = [snake_case(column) for column in df.columns]
    return df


def extract_partition_value(path: Path, partition_name: str) -> Optional[str]:
    """
    Extract a Hive-style partition value from a path.

    Example:
    data/raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-06-09/page=1.json

    extract_partition_value(path, "media_id") -> "gskhw4w4lm"
    extract_partition_value(path, "ingest_date") -> "2026-06-09"
    """
    prefix = f"{partition_name}="

    for part in path.parts:
        if part.startswith(prefix):
            return part.replace(prefix, "", 1)

    return None


def extract_page_number(path: Path) -> Optional[int]:
    """
    Extract page number from event filename.

    Example:
    page=2.json -> 2
    """
    match = re.match(r"page=(\d+)\.json$", path.name)

    if not match:
        return None

    return int(match.group(1))


def add_ingest_metadata(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    """
    Add useful ingestion metadata columns.
    """
    df = df.copy()

    ingest_date = extract_partition_value(source_file, "ingest_date")
    media_id = extract_partition_value(source_file, "media_id")
    page_number = extract_page_number(source_file)

    df["source_file"] = str(source_file)
    df["ingest_date"] = ingest_date

    if media_id:
        df["source_media_id"] = media_id

    if page_number is not None:
        df["source_page"] = page_number

    return df


def cast_datetime_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """
    Cast selected columns to pandas datetime where present.
    """
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)

    return df


def cast_numeric_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """
    Cast selected columns to numeric where present.
    """
    df = df.copy()

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def flatten_media_metadata(payload: Dict[str, Any], source_file: Path) -> pd.DataFrame:
    """
    Flatten one media metadata response.

    The raw Wistia media metadata response is a single JSON object.
    """
    project = payload.get("project") or {}
    section = payload.get("section") or {}
    share_link = payload.get("share_link") or {}
    thumbnail = payload.get("thumbnail") or {}

    if isinstance(section, dict):
        section_id = section.get("id")
        section_name = section.get("name")
    else:
        section_id = None
        section_name = section

    record = {
        "wistia_numeric_id": payload.get("id"),
        "media_hashed_id": payload.get("hashed_id"),
        "media_type": payload.get("type"),
        "media_name": payload.get("name"),
        "duration_seconds": payload.get("duration"),
        "created_at": payload.get("created"),
        "updated_at": payload.get("updated"),
        "status": payload.get("status"),
        "archived": payload.get("archived"),
        "description": payload.get("description"),
        "project_id": project.get("id"),
        "project_hashed_id": project.get("hashed_id"),
        "project_name": project.get("name"),
        "section_id": section_id,
        "section_name": section_name,
        "share_url": share_link.get("url"),
        "thumbnail_url": thumbnail.get("url"),
        "thumbnail_width": thumbnail.get("width"),
        "thumbnail_height": thumbnail.get("height"),
    }

    df = pd.DataFrame([record])
    df = add_ingest_metadata(df, source_file)
    df = normalize_columns(df)

    df = cast_datetime_columns(df, ["created_at", "updated_at"])
    df = cast_numeric_columns(
        df,
        [
            "wistia_numeric_id",
            "duration_seconds",
            "project_id",
            "section_id",
            "thumbnail_width",
            "thumbnail_height",
        ],
    )

    return df


def flatten_media_stats(payload: Dict[str, Any], source_file: Path) -> pd.DataFrame:
    """
    Flatten one media stats response.

    The raw Wistia media stats response is a single JSON object.
    """
    media_id = extract_partition_value(source_file, "media_id")

    load_count = payload.get("load_count")
    play_count = payload.get("play_count")

    calculated_play_rate = None
    if load_count not in (None, 0):
        calculated_play_rate = play_count / load_count

    record = {
        "media_hashed_id": media_id,
        "load_count": load_count,
        "play_count": play_count,
        "api_play_rate": payload.get("play_rate"),
        "calculated_play_rate": calculated_play_rate,
        "hours_watched": payload.get("hours_watched"),
        "engagement": payload.get("engagement"),
        "visitor_count": payload.get("visitors"),
    }

    df = pd.DataFrame([record])
    df = add_ingest_metadata(df, source_file)
    df = normalize_columns(df)

    df = cast_numeric_columns(
        df,
        [
            "load_count",
            "play_count",
            "api_play_rate",
            "calculated_play_rate",
            "hours_watched",
            "engagement",
            "visitor_count",
        ],
    )

    return df


def flatten_visitors(payload: Any, source_file: Path) -> pd.DataFrame:
    """
    Flatten visitor summary response.

    During API exploration, the visitors endpoint behaved like an account-level
    visitor summary endpoint rather than a media-specific endpoint.
    """
    if isinstance(payload, dict):
        records = payload.get("visitors", [])
    elif isinstance(payload, list):
        records = payload
    else:
        records = []

    flattened_records: List[Dict[str, Any]] = []

    for visitor in records:
        visitor_identity = visitor.get("visitor_identity") or {}
        org = visitor_identity.get("org") or {}
        user_agent = visitor.get("user_agent_details") or {}

        flattened_records.append(
            {
                "visitor_key": visitor.get("visitor_key"),
                "created_at": visitor.get("created_at"),
                "last_active_at": visitor.get("last_active_at"),
                "last_event_key": visitor.get("last_event_key"),
                "load_count": visitor.get("load_count"),
                "play_count": visitor.get("play_count"),
                "identifying_event_key": visitor.get("identifying_event_key"),
                "visitor_name": visitor_identity.get("name"),
                "visitor_email": visitor_identity.get("email"),
                "visitor_org_name": org.get("name"),
                "visitor_org_title": org.get("title"),
                "browser": user_agent.get("browser"),
                "browser_version": user_agent.get("browser_version"),
                "platform": user_agent.get("platform"),
                "is_mobile": user_agent.get("mobile"),
            }
        )

    df = pd.DataFrame(flattened_records)
    df = add_ingest_metadata(df, source_file)
    df = normalize_columns(df)

    df = cast_datetime_columns(df, ["created_at", "last_active_at"])
    df = cast_numeric_columns(df, ["load_count", "play_count"])

    return df


def flatten_events(payload: Any, source_file: Path) -> pd.DataFrame:
    """
    Flatten one page of Wistia event records.

    Events are the authoritative media-specific engagement records because they
    include media_id, visitor_key, event_key, geography, device, and percent_viewed.
    """
    if not isinstance(payload, list):
        raise ValueError(f"Expected events payload to be a list. Source file: {source_file}")

    flattened_records: List[Dict[str, Any]] = []

    for event in payload:
        user_agent = event.get("user_agent_details") or {}
        thumbnail = event.get("thumbnail") or {}

        flattened_records.append(
            {
                "event_key": event.get("event_key"),
                "received_at": event.get("received_at"),
                "visitor_key": event.get("visitor_key"),
                "media_hashed_id": event.get("media_id"),
                "media_name": event.get("media_name"),
                "media_url": event.get("media_url"),
                "percent_viewed": event.get("percent_viewed"),
                "ip_address": event.get("ip"),
                "country": event.get("country"),
                "region": event.get("region"),
                "city": event.get("city"),
                "latitude": event.get("lat"),
                "longitude": event.get("lon"),
                "organization": event.get("org"),
                "email": event.get("email"),
                "embed_url": event.get("embed_url"),
                "conversion_type": event.get("conversion_type"),
                "conversion_data_json": (
                    json.dumps(event.get("conversion_data"), ensure_ascii=False)
                    if event.get("conversion_data") is not None
                    else None
                ),
                "iframe_heatmap_url": event.get("iframe_heatmap_url"),
                "browser": user_agent.get("browser"),
                "browser_version": user_agent.get("browser_version"),
                "platform": user_agent.get("platform"),
                "is_mobile": user_agent.get("mobile"),
                "thumbnail_url": thumbnail.get("url"),
                "thumbnail_width": thumbnail.get("width"),
                "thumbnail_height": thumbnail.get("height"),
            }
        )

    df = pd.DataFrame(flattened_records)
    df = add_ingest_metadata(df, source_file)
    df = normalize_columns(df)

    df = cast_datetime_columns(df, ["received_at"])
    df = cast_numeric_columns(
        df,
        [
            "percent_viewed",
            "latitude",
            "longitude",
            "thumbnail_width",
            "thumbnail_height",
        ],
    )

    return df


def combine_dataframes(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine DataFrames safely.
    """
    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


def transform_endpoint(
    raw_base_dir: str | Path,
    silver_base_dir: str | Path,
    endpoint_name: str,
) -> None:
    """
    Transform one endpoint from raw JSON to Silver Parquet.
    """
    raw_base_dir = Path(raw_base_dir)
    silver_base_dir = Path(silver_base_dir)

    endpoint_dir = raw_base_dir / endpoint_name

    if not endpoint_dir.exists():
        print(f"Skipping {endpoint_name}; raw directory does not exist: {endpoint_dir}")
        return

    json_files = sorted(endpoint_dir.rglob("*.json"))

    if not json_files:
        print(f"Skipping {endpoint_name}; no JSON files found in: {endpoint_dir}")
        return

    transformed_frames: List[pd.DataFrame] = []

    for json_file in json_files:
        payload = read_json_file(json_file)

        if endpoint_name == "media_metadata":
            df = flatten_media_metadata(payload, json_file)
        elif endpoint_name == "media_stats":
            df = flatten_media_stats(payload, json_file)
        elif endpoint_name == "visitors":
            df = flatten_visitors(payload, json_file)
        elif endpoint_name == "events":
            df = flatten_events(payload, json_file)
        else:
            raise ValueError(f"Unsupported endpoint_name: {endpoint_name}")

        if not df.empty:
            transformed_frames.append(df)

    combined_df = combine_dataframes(transformed_frames)

    if combined_df.empty:
        print(f"No records produced for endpoint: {endpoint_name}")
        return

    if endpoint_name == "events" and "event_key" in combined_df.columns:
        before_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=["event_key"])
        after_count = len(combined_df)
        print(
            f"Deduplicated events: {before_count} records before, "
            f"{after_count} records after."
        )

    output_path = silver_base_dir / endpoint_name / "data.parquet"
    write_parquet(combined_df, output_path)

    print(f"Wrote Silver {endpoint_name}: {output_path}")
    print(f"Rows: {len(combined_df)}")
    print(f"Columns: {list(combined_df.columns)}")


def run_silver_transform(
    raw_base_dir: str | Path,
    silver_base_dir: str | Path,
    endpoints: Optional[List[str]] = None,
) -> None:
    """
    Run the Silver transform for selected endpoints.
    """
    endpoints = endpoints or [
        "media_metadata",
        "media_stats",
        "visitors",
        "events",
    ]

    for endpoint_name in endpoints:
        transform_endpoint(
            raw_base_dir=raw_base_dir,
            silver_base_dir=silver_base_dir,
            endpoint_name=endpoint_name,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform raw Wistia JSON to Silver Parquet.")

    parser.add_argument(
        "--raw-dir",
        default="data/raw/wistia",
        help="Base raw Wistia data directory.",
    )

    parser.add_argument(
        "--silver-dir",
        default="data/silver/wistia",
        help="Base Silver Wistia data directory.",
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

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_silver_transform(
        raw_base_dir=args.raw_dir,
        silver_base_dir=args.silver_dir,
        endpoints=args.endpoint,
    )


if __name__ == "__main__":
    main()