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
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Union

import requests
import yaml
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from urllib.parse import urlparse

import boto3


BASE_URL = "https://api.wistia.com/v1"


def load_media_config(config_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Load media configuration from YAML.

    Supports both local paths and S3 paths.

    Expected YAML shape:

    media_ids:
      - media_id: gskhw4w4lm
        channel: YouTube
        name: Example Name
    """
    config_path_str = str(config_path)

    if is_s3_path(config_path_str):
        bucket, key = parse_s3_uri(config_path_str)
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        config_text = response["Body"].read().decode("utf-8")
        config = yaml.safe_load(config_text) or {}
    else:
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


def get_secret_value(secret_name: str, region_name: str) -> str:
    """
    Read a secret value from AWS Secrets Manager.

    Supports either:
    - Plain text secret containing the token directly
    - JSON secret containing one of these keys:
      - WISTIA_API_TOKEN
      - wistia_api_token
      - api_token
      - token
    """
    secrets_client = boto3.client("secretsmanager", region_name=region_name)

    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError(f"Secret {secret_name} does not contain SecretString.")

    try:
        secret_payload = json.loads(secret_string)

        for key in ["WISTIA_API_TOKEN", "wistia_api_token", "api_token", "token"]:
            if key in secret_payload:
                return str(secret_payload[key])

        raise ValueError(
            f"Secret {secret_name} is JSON but does not contain a recognized token key."
        )

    except json.JSONDecodeError:
        return secret_string


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


def is_s3_path(path: Union[str, Path]) -> bool:
    """
    Return True if the provided path is an S3 URI.
    """
    return str(path).startswith("s3://")


def parse_s3_uri(s3_uri: Union[str, Path]) -> Tuple[str, str]:
    """
    Parse an S3 URI into bucket and key.

    Example:
    s3://my-bucket/raw/wistia/events/page=1.json

    Returns:
    ("my-bucket", "raw/wistia/events/page=1.json")
    """
    parsed = urlparse(str(s3_uri))

    if parsed.scheme != "s3":
        raise ValueError(f"Not a valid S3 URI: {s3_uri}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not bucket or not key:
        raise ValueError(f"S3 URI must include bucket and key: {s3_uri}")

    return bucket, key


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
    max_pages: Optional[int] = None,
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

        if max_pages is not None and page > max_pages:
            break


def write_json(output_path: Union[str, Path], payload: Any) -> None:
    """
    Write JSON payload to either local disk or S3.

    Local example:
    data/raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-06-09/page=1.json

    S3 example:
    s3://wistia-video-analytics-guy/raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-06-09/page=1.json
    """
    output_path_str = str(output_path)
    json_body = json.dumps(payload, indent=2, ensure_ascii=False)

    if is_s3_path(output_path_str):
        bucket, key = parse_s3_uri(output_path_str)
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_body.encode("utf-8"),
            ContentType="application/json",
        )
        return

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        file.write(json_body)


def get_raw_output_path(
    base_dir: Union[str, Path],
    endpoint_name: str,
    ingest_date: str,
    media_id: Optional[str] = None,
    page: Optional[int] = None,
) -> Union[str, Path]:
    """
    Build the raw output path for either local storage or S3.

    Local example:
    data/raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-06-09/page=1.json

    S3 example:
    s3://bucket/raw/wistia/events/media_id=gskhw4w4lm/ingest_date=2026-06-09/page=1.json
    """
    base_dir_str = str(base_dir).rstrip("/")

    path_parts = [endpoint_name]

    if media_id:
        path_parts.append(f"media_id={media_id}")

    path_parts.append(f"ingest_date={ingest_date}")

    if endpoint_name == "events":
        if page is None:
            raise ValueError("page is required for events output paths.")
        filename = f"page={page}.json"
    else:
        filename = "response.json"

    path_parts.append(filename)

    if is_s3_path(base_dir_str):
        return "/".join([base_dir_str, *path_parts])

    return Path(base_dir_str).joinpath(*path_parts)


def get_current_ingest_date() -> str:
    """
    Return current UTC date as YYYY-MM-DD.
    """
    return datetime.now(timezone.utc).date().isoformat()


def ingest_media_metadata(
    session: requests.Session,
    media_ids: Iterable[Dict[str, Any]],
    headers: Dict[str, str],
    base_output_dir: Union[str, Path],
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
    base_output_dir: Union[str, Path],
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
    base_output_dir: Union[str, Path],
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
    base_output_dir: Union[str, Path],
    ingest_date: str,
    per_page: int = 100,
    max_pages: Optional[int] = None,
) -> None:
    for media in media_ids:
        media_id = media["media_id"]

        for page_result in fetch_events_pages(
            session=session,
            media_id=media_id,
            headers=headers,
            per_page=per_page,
            max_pages=max_pages,
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
    config_path: Union[str, Path],
    base_output_dir: Union[str, Path],
    api_token: str,
    ingest_date: Optional[str] = None,
    per_page: int = 100,
    max_pages: Optional[int] = None,
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
            max_pages=max_pages,
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

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional maximum number of event pages to fetch per media ID.",
    )

    parser.add_argument(
        "--secret-name",
        default=None,
        help="Optional AWS Secrets Manager secret name containing the Wistia API token.",
    )

    parser.add_argument(
        "--aws-region",
        default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1",
        help="AWS region for Secrets Manager/S3 clients.",
    )

    args, unknown_args = parser.parse_known_args()
    
    if unknown_args:
        print(f"Ignoring unknown Glue/job arguments: {unknown_args}")
    
    return args


def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    args = parse_args()

    print("Starting Wistia ingestion job")
    print(f"Config path: {args.config}")
    print(f"Output dir: {args.output_dir}")
    print(f"Secret name: {args.secret_name}")
    print(f"AWS region: {args.aws_region}")
    print(f"Ingest date: {args.ingest_date}")
    print(f"Per page: {args.per_page}")
    print(f"Max pages: {args.max_pages}")

    if args.secret_name:
        api_token = get_secret_value(
            secret_name=args.secret_name,
            region_name=args.aws_region,
        )
    else:
        api_token = os.getenv("WISTIA_API_TOKEN")

    if not api_token:
        raise ValueError(
            "Wistia API token is not set. Provide WISTIA_API_TOKEN locally "
            "or use --secret-name for AWS Secrets Manager."
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
        max_pages=args.max_pages,
    )


if __name__ == "__main__":
    main()