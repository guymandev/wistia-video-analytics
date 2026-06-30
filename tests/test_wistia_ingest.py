"""
Unit tests for Wistia API ingestion helpers.

These tests are intentionally focused on small, testable behaviors:
- loading media config
- building auth headers
- building endpoint URLs/params
- paginating event responses
- writing raw JSON files

They avoid calling the real Wistia API.
"""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from src.ingest import wistia_ingest


def test_load_media_config_reads_media_ids(tmp_path):
    config_data = {
        "media_ids": [
            {
                "media_id": "gskhw4w4lm",
                "channel": "YouTube",
                "name": "Chris Face VSL The Gap Method Youtube Paid Ads",
            },
            {
                "media_id": "v08dlrgr7v",
                "channel": "Facebook",
                "name": "Chris Face VSL The Gap Method Facebook Paid Ads",
            },
        ]
    }

    config_file = tmp_path / "media_config.yaml"
    config_file.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    result = wistia_ingest.load_media_config(config_file)

    assert len(result) == 2
    assert result[0]["media_id"] == "gskhw4w4lm"
    assert result[0]["channel"] == "YouTube"
    assert result[1]["media_id"] == "v08dlrgr7v"
    assert result[1]["channel"] == "Facebook"


def test_build_headers_uses_bearer_token():
    token = "fake-token-123"

    headers = wistia_ingest.build_headers(token)

    assert headers["Authorization"] == "Bearer fake-token-123"
    assert "Bearer Bearer" not in headers["Authorization"]


def test_build_media_metadata_url():
    media_id = "gskhw4w4lm"

    url = wistia_ingest.build_media_metadata_url(media_id)

    assert url == "https://api.wistia.com/v1/medias/gskhw4w4lm.json"


def test_build_media_stats_url():
    media_id = "gskhw4w4lm"

    url = wistia_ingest.build_media_stats_url(media_id)

    assert url == "https://api.wistia.com/v1/stats/medias/gskhw4w4lm.json"


def test_build_visitors_url():
    url = wistia_ingest.build_visitors_url()

    assert url == "https://api.wistia.com/v1/stats/visitors.json"


def test_build_events_url():
    url = wistia_ingest.build_events_url()

    assert url == "https://api.wistia.com/v1/stats/events.json"


def test_fetch_json_raises_for_bad_status():
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("Bad response")

    mock_session = Mock()
    mock_session.get.return_value = mock_response

    with pytest.raises(Exception, match="Bad response"):
        wistia_ingest.fetch_json(
            session=mock_session,
            url="https://api.wistia.com/v1/test.json",
            headers={"Authorization": "Bearer fake-token"},
        )


def test_fetch_json_returns_response_payload():
    expected_payload = {"load_count": 111197, "play_count": 16692}

    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = expected_payload

    mock_session = Mock()
    mock_session.get.return_value = mock_response

    result = wistia_ingest.fetch_json(
        session=mock_session,
        url="https://api.wistia.com/v1/stats/medias/gskhw4w4lm.json",
        headers={"Authorization": "Bearer fake-token"},
    )

    assert result == expected_payload


def test_fetch_events_pages_until_empty():
    page_1 = [
        {
            "event_key": "event-1",
            "media_id": "gskhw4w4lm",
            "percent_viewed": 0.5,
        }
    ]
    page_2 = [
        {
            "event_key": "event-2",
            "media_id": "gskhw4w4lm",
            "percent_viewed": 1.0,
        }
    ]
    empty_page = []

    mock_session = Mock()

    mock_response_1 = Mock()
    mock_response_1.raise_for_status.return_value = None
    mock_response_1.json.return_value = page_1

    mock_response_2 = Mock()
    mock_response_2.raise_for_status.return_value = None
    mock_response_2.json.return_value = page_2

    mock_response_3 = Mock()
    mock_response_3.raise_for_status.return_value = None
    mock_response_3.json.return_value = empty_page

    mock_session.get.side_effect = [
        mock_response_1,
        mock_response_2,
        mock_response_3,
    ]

    results = list(
        wistia_ingest.fetch_events_pages(
            session=mock_session,
            media_id="gskhw4w4lm",
            headers={"Authorization": "Bearer fake-token"},
            per_page=100,
        )
    )

    assert len(results) == 2
    assert results[0]["page"] == 1
    assert results[0]["records"] == page_1
    assert results[1]["page"] == 2
    assert results[1]["records"] == page_2

    assert mock_session.get.call_count == 3

    first_call_kwargs = mock_session.get.call_args_list[0].kwargs
    assert first_call_kwargs["params"] == {
        "media_id": "gskhw4w4lm",
        "page": 1,
        "per_page": 100,
    }

    second_call_kwargs = mock_session.get.call_args_list[1].kwargs
    assert second_call_kwargs["params"] == {
        "media_id": "gskhw4w4lm",
        "page": 2,
        "per_page": 100,
    }


def test_write_json_creates_parent_dirs_and_file(tmp_path):
    payload = {
        "id": 128842548,
        "hashed_id": "gskhw4w4lm",
        "name": "Chris Face VSL The Gap Method Youtube Paid Ads",
    }

    output_file = tmp_path / "raw" / "wistia" / "media_metadata" / "media_id=gskhw4w4lm" / "response.json"

    wistia_ingest.write_json(output_file, payload)

    assert output_file.exists()

    saved_payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert saved_payload == payload


def test_get_raw_output_path_for_metadata():
    path = wistia_ingest.get_raw_output_path(
        base_dir=Path("data/raw/wistia"),
        endpoint_name="media_metadata",
        ingest_date="2026-06-09",
        media_id="gskhw4w4lm",
    )

    assert path == Path(
        "data/raw/wistia/"
        "media_metadata/"
        "media_id=gskhw4w4lm/"
        "ingest_date=2026-06-09/"
        "response.json"
    )


def test_get_raw_output_path_for_events_page():
    path = wistia_ingest.get_raw_output_path(
        base_dir=Path("data/raw/wistia"),
        endpoint_name="events",
        ingest_date="2026-06-09",
        media_id="gskhw4w4lm",
        page=2,
    )

    assert path == Path(
        "data/raw/wistia/"
        "events/"
        "media_id=gskhw4w4lm/"
        "ingest_date=2026-06-09/"
        "page=2.json"
    )


def test_get_raw_output_path_for_visitors():
    path = wistia_ingest.get_raw_output_path(
        base_dir=Path("data/raw/wistia"),
        endpoint_name="visitors",
        ingest_date="2026-06-09",
    )

    assert path == Path(
        "data/raw/wistia/"
        "visitors/"
        "ingest_date=2026-06-09/"
        "response.json"
    )