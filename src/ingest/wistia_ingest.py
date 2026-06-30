"""
Wistia API ingestion script.

Responsibilities:
- Load configured Wistia media IDs.
- Authenticate using Bearer token.
- Call Wistia media metadata, media stats, visitors, and events endpoints.
- Handle pagination for events.
- Write raw JSON responses to local storage using an S3-like folder layout.

This script writes raw JSON locally first. Later, the file-writing function can be
adapted or replaced with an S3 writer.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional

import requests
import yaml
from dotenv import load_dotenv


BASE_URL = "https://api.wistia.com/v1"


def load_media_config(config_path: str | Path) -> List[Dict[str, Any]]:
    """
    Load media configuration from YAML.

    Expected YAML shape:

    media_ids:
      - media_id: gskhw4w4lm
        channel: YouTube
        name: Example Name
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Media config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    media_ids = config.get("media_ids")

    if not isinstance(media_ids, list) or not media_ids:
        raise ValueError("media_config.yaml must contain a non-empty 'media_ids' list.")

    for item in media_ids:
        if "media_id" not in item:
            raise ValueError(f"Media config item is missing 'media_id': {item}")

    return media_ids


def build_headers(api_token: str) -> Dict[str, str]:
    """
    Build Wistia API request headers.

    The token should be the raw token value only, not 'Bearer <token>'.
    """
    if not api_token:
        raise ValueError("Wistia API token is required.")

    cleaned_token = api_token.strip()

    if cleaned_token.lower().startswith("bearer "):
        cleaned_token = cleaned_token.split(" ", 1)[1].strip()

    return {
        "Authorization": f"Bearer {cleaned_token}",
        "Accept": "application/json",
    }


def build_media_metadata_url(media_id: str) -> str:
    return f"{BASE_URL}/medias/{media_id}.json"


def build_media_stats_url(media_id: str) -> str:
    return f"{BASE_URL}/stats/medias/{media_id}.json"


def build_visitors_url() -> str:
    return f"{BASE_URL}/stats/visitors.json"


def build_events_url() -> str:
    return f"{BASE_URL}/stats/events.json"


def fetch_json(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 30,
) -> Any:
    """
    Fetch JSON from the Wistia API.

    Raises an HTTPError for non-2xx responses.
    """
    response = session.get(
        url,
        headers=headers,
        params=params,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def fetch_events_pages(
    session: requests.Session,
    media_id: str,
    headers: Dict[str, str],
    per_page: int = 100,
) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch paginated event records for a single media ID.

    Yields dictionaries shaped like:
    {
        "page": 1,
        "records": [...]
    }

    Stops when the API returns an empty list.
    """
    page = 1
    url = build_events_url()

    while True:
        params = {
            "media_id": media_id,
            "page": page,
            "per_page": per_page,
        }

        records = fetch_json(
            session=session,
            url=url,
            headers=headers,
            params=params,
        )

        if not records:
            break

        if not isinstance(records, list):
            raise ValueError(
                f"Expected events endpoint to return a list, got {type(records)}"
            )

        yield {
            "page": page,
            "records": records,
        }

        page += 1


def write_json(output_path: str | Path, payload: Any) -> None:
    """
    Write JSON payload to disk, creating parent directories as needed.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)


def get_raw_output_path(
    base_dir: str | Path,
    endpoint_name: str,
    ingest_date: str,
    media_id: Optional[str] = None,
    page: Optional[int] = None,
) -> Path:
    """
    Build the local raw output path.

    Examples:

    data/raw/wistia/media_metadata/media_id=gskhw4w4lm/ingest_date=2026-06-09/response.json

    data/raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-06-09/page=1.json

    data/raw/wistia/visitors/ingest_date=2026-06-09/response.json
    """
    path = Path(base_dir) / endpoint_name

    if media_id:
        path = path / f"media_id={media_id}"

    path = path / f"ingest_date={ingest_date}"

    if endpoint_name == "events":
        if page is None:
            raise ValueError("page is required for events output paths.")
        return path / f"page={page}.json"

    return path / "response.json"


def get_current_ingest_date() -> str:
    """
    Return current UTC date as YYYY-MM-DD.
    """
    return datetime.now(timezone.utc).date().isoformat()


def ingest_media_metadata(
    session: requests.Session,
    media_ids: Iterable[Dict[str, Any]],
    headers: Dict[str, str],
    base_output_dir: str | Path,
    ingest_date: str,
) -> None:
    for media in media_ids:
        media_id = media["media_id"]
        url = build_media_metadata_url(media_id)

        payload = fetch_json(session=session, url=url, headers=headers)

        output_path = get_raw_output_path(
            base_dir=base_output_dir,
            endpoint_name="media_metadata",
            media_id=media_id,
            ingest_date=ingest_date,
        )

        write_json(output_path, payload)
        print(f"Wrote media metadata: {output_path}")


def ingest_media_stats(
    session: requests.Session,
    media_ids: Iterable[Dict[str, Any]],
    headers: Dict[str, str],
    base_output_dir: str | Path,
    ingest_date: str,
) -> None:
    for media in media_ids:
        media_id = media["media_id"]
        url = build_media_stats_url(media_id)

        payload = fetch_json(session=session, url=url, headers=headers)

        output_path = get_raw_output_path(
            base_dir=base_output_dir,
            endpoint_name="media_stats",
            media_id=media_id,
            ingest_date=ingest_date,
        )

        write_json(output_path, payload)
        print(f"Wrote media stats: {output_path}")


def ingest_visitors(
    session: requests.Session,
    headers: Dict[str, str],
    base_output_dir: str | Path,
    ingest_date: str,
) -> None:
    """
    Ingest visitor summary data.

    During API exploration, the visitors endpoint appeared to return account-level
    visitor summaries even when media_id was supplied. Therefore, this function
    intentionally calls the endpoint once without media_id and stores it as
    supplemental visitor summary data.
    """
    url = build_visitors_url()

    payload = fetch_json(session=session, url=url, headers=headers)

    output_path = get_raw_output_path(
        base_dir=base_output_dir,
        endpoint_name="visitors",
        ingest_date=ingest_date,
    )

    write_json(output_path, payload)
    print(f"Wrote visitors summary: {output_path}")


def ingest_events(
    session: requests.Session,
    media_ids: Iterable[Dict[str, Any]],
    headers: Dict[str, str],
    base_output_dir: str | Path,
    ingest_date: str,
    per_page: int = 100,
) -> None:
    for media in media_ids:
        media_id = media["media_id"]

        for page_result in fetch_events_pages(
            session=session,
            media_id=media_id,
            headers=headers,
            per_page=per_page,
        ):
            page = page_result["page"]
            records = page_result["records"]

            output_path = get_raw_output_path(
                base_dir=base_output_dir,
                endpoint_name="events",
                media_id=media_id,
                ingest_date=ingest_date,
                page=page,
            )

            write_json(output_path, records)
            print(f"Wrote events page {page} for {media_id}: {output_path}")


def run_ingestion(
    config_path: str | Path,
    base_output_dir: str | Path,
    api_token: str,
    ingest_date: Optional[str] = None,
    per_page: int = 100,
) -> None:
    """
    Run the full local raw ingestion pipeline.
    """
    media_ids = load_media_config(config_path)
    headers = build_headers(api_token)
    ingest_date = ingest_date or get_current_ingest_date()

    with requests.Session() as session:
        ingest_media_metadata(
            session=session,
            media_ids=media_ids,
            headers=headers,
            base_output_dir=base_output_dir,
            ingest_date=ingest_date,
        )

        ingest_media_stats(
            session=session,
            media_ids=media_ids,
            headers=headers,
            base_output_dir=base_output_dir,
            ingest_date=ingest_date,
        )

        ingest_visitors(
            session=session,
            headers=headers,
            base_output_dir=base_output_dir,
            ingest_date=ingest_date,
        )

        ingest_events(
            session=session,
            media_ids=media_ids,
            headers=headers,
            base_output_dir=base_output_dir,
            ingest_date=ingest_date,
            per_page=per_page,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw Wistia API data locally.")

    parser.add_argument(
        "--config",
        default="config/media_config.yaml",
        help="Path to media config YAML file.",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Base raw output directory. Defaults to RAW_OUTPUT_DIR env var or data/raw/wistia.",
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
        help="Number of event records per page.",
    )

    return parser.parse_args()


def main() -> None:
    load_dotenv()

    args = parse_args()

    api_token = os.getenv("WISTIA_API_TOKEN")
    if not api_token:
        raise ValueError(
            "WISTIA_API_TOKEN is not set. Add it to your .env file or shell environment."
        )

    base_output_dir = (
        args.output_dir
        or os.getenv("RAW_OUTPUT_DIR")
        or "data/raw/wistia"
    )

    run_ingestion(
        config_path=args.config,
        base_output_dir=base_output_dir,
        api_token=api_token,
        ingest_date=args.ingest_date,
        per_page=args.per_page,
    )


if __name__ == "__main__":
    main()