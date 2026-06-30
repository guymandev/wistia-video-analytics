"""
Local pipeline runner for Wistia Video Analytics.

Responsibilities:
- Run the full local Wistia analytics pipeline in order:
  1. Raw API ingestion
  2. Silver transform
  3. Gold transform
  4. Mart transform

This script is intended for local development and smoke testing before
deploying the same logical workflow to AWS Glue / S3 / Athena.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from src.ingest.wistia_ingest import run_ingestion
from src.transform.gold_transform import run_gold_transform
from src.transform.mart_transform import run_mart_transform
from src.transform.silver_transform import run_silver_transform


def validate_path_exists(path: str | Path, description: str) -> None:
    """
    Validate that a required file or directory exists.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"{description} not found: {path}")


def print_section(title: str) -> None:
    """
    Print a readable section header for local pipeline logs.
    """
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def run_local_pipeline(
    config_path: str | Path,
    raw_output_dir: str | Path,
    silver_output_dir: str | Path,
    gold_output_dir: str | Path,
    mart_output_dir: str | Path,
    api_token: str,
    ingest_date: Optional[str] = None,
    per_page: int = 100,
    max_pages: Optional[int] = None,
    skip_ingestion: bool = False,
) -> None:
    """
    Run the complete local Wistia analytics pipeline.

    Parameters:
    - config_path: path to config/media_config.yaml
    - raw_output_dir: local raw JSON output directory
    - silver_output_dir: local Silver Parquet output directory
    - gold_output_dir: local Gold Parquet output directory
    - mart_output_dir: local Mart Parquet output directory
    - api_token: Wistia API token
    - ingest_date: optional YYYY-MM-DD ingest date override
    - per_page: number of Wistia event records per API page
    - max_pages: optional max number of event pages per media ID
    - skip_ingestion: reuse existing raw files instead of calling the Wistia API
    """
    validate_path_exists(config_path, "Media config file")

    if not skip_ingestion:
        print_section("STEP 1: RAW WISTIA API INGESTION")

        run_ingestion(
            config_path=config_path,
            base_output_dir=raw_output_dir,
            api_token=api_token,
            ingest_date=ingest_date,
            per_page=per_page,
            max_pages=max_pages,
        )
    else:
        print_section("STEP 1: RAW WISTIA API INGESTION SKIPPED")
        print(f"Reusing existing raw files from: {raw_output_dir}")

    print_section("STEP 2: SILVER TRANSFORM")

    run_silver_transform(
        raw_base_dir=raw_output_dir,
        silver_base_dir=silver_output_dir,
    )

    print_section("STEP 3: GOLD TRANSFORM")

    run_gold_transform(
        silver_base_dir=silver_output_dir,
        gold_base_dir=gold_output_dir,
        config_path=config_path,
    )

    print_section("STEP 4: MART TRANSFORM")

    run_mart_transform(
        gold_base_dir=gold_output_dir,
        mart_base_dir=mart_output_dir,
    )

    print_section("LOCAL PIPELINE COMPLETE")

    print(f"Raw output:    {raw_output_dir}")
    print(f"Silver output: {silver_output_dir}")
    print(f"Gold output:   {gold_output_dir}")
    print(f"Mart output:   {mart_output_dir}")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run the full local Wistia analytics pipeline."
    )

    parser.add_argument(
        "--config",
        default="config/media_config.yaml",
        help="Path to media config YAML file.",
    )

    parser.add_argument(
        "--raw-dir",
        default=None,
        help=(
            "Base raw output directory. Defaults to RAW_OUTPUT_DIR env var "
            "or data/raw/wistia."
        ),
    )

    parser.add_argument(
        "--silver-dir",
        default="data/silver/wistia",
        help="Base Silver output directory.",
    )

    parser.add_argument(
        "--gold-dir",
        default="data/gold/wistia",
        help="Base Gold output directory.",
    )

    parser.add_argument(
        "--mart-dir",
        default="data/marts/wistia",
        help="Base Mart output directory.",
    )

    parser.add_argument(
        "--ingest-date",
        default=None,
        help="Optional ingest date override in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Number of event records per API page.",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional maximum number of event pages to fetch per media ID.",
    )

    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip Wistia API calls and reuse existing local raw files.",
    )

    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint.
    """
    load_dotenv()

    args = parse_args()

    api_token = os.getenv("WISTIA_API_TOKEN")

    if not api_token and not args.skip_ingestion:
        raise ValueError(
            "WISTIA_API_TOKEN is not set. Add it to your .env file or shell environment."
        )

    raw_output_dir = (
        args.raw_dir
        or os.getenv("RAW_OUTPUT_DIR")
        or "data/raw/wistia"
    )

    run_local_pipeline(
        config_path=args.config,
        raw_output_dir=raw_output_dir,
        silver_output_dir=args.silver_dir,
        gold_output_dir=args.gold_dir,
        mart_output_dir=args.mart_dir,
        api_token=api_token or "",
        ingest_date=args.ingest_date,
        per_page=args.per_page,
        max_pages=args.max_pages,
        skip_ingestion=args.skip_ingestion,
    )


if __name__ == "__main__":
    main()