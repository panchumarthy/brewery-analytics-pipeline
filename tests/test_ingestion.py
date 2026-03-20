"""
test_ingestion.py
-----------------
Unit tests for the ingestion layer.
Uses pytest-mock and moto to mock external calls — no real API or AWS needed.
Run with: pytest tests/test_ingestion.py -v
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


# ── fetch_breweries tests ──────────────────────────────────────────────────

class TestFetchBreweries:

    SAMPLE_BREWERY = {
        "id": "b54b16e1-ac3b-4bff-a11f-f7ae9ddc27e0",
        "name": "Black Mesa Brewing",
        "brewery_type": "micro",
        "city": "Albuquerque",
        "state": "New Mexico",
        "latitude": "35.0844",
        "longitude": "-106.6504",
        "website_url": "https://blackmesabrewing.com",
    }

    def test_fetch_page_returns_list(self):
        """fetch_page should return a list of dicts on a successful response."""
        from ingestion.fetch_breweries import fetch_page

        mock_response = MagicMock()
        mock_response.json.return_value = [self.SAMPLE_BREWERY]
        mock_response.raise_for_status = MagicMock()

        with patch("ingestion.fetch_breweries.requests.get", return_value=mock_response):
            result = fetch_page(page=1)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["name"] == "Black Mesa Brewing"

    def test_fetch_page_empty_signals_end(self):
        """An empty page response should return an empty list (signals last page)."""
        from ingestion.fetch_breweries import fetch_page

        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch("ingestion.fetch_breweries.requests.get", return_value=mock_response):
            result = fetch_page(page=99)

        assert result == []

    def test_save_to_file_creates_ndjson(self, tmp_path):
        """save_to_file should write one JSON object per line."""
        from ingestion import fetch_breweries

        breweries = [self.SAMPLE_BREWERY, {**self.SAMPLE_BREWERY, "id": "abc-123"}]

        with patch.object(fetch_breweries, "OUTPUT_DIR", tmp_path):
            file_path = fetch_breweries.save_to_file(breweries)

        assert Path(file_path).exists()
        lines = Path(file_path).read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["name"] == "Black Mesa Brewing"


# ── upload_to_s3 tests ─────────────────────────────────────────────────────

class TestUploadToS3:

    def test_raises_if_bucket_not_set(self, monkeypatch):
        """Should raise ValueError when S3_BUCKET_NAME env var is missing."""
        from ingestion import upload_to_s3

        monkeypatch.setattr(upload_to_s3, "S3_BUCKET", None)

        with pytest.raises(ValueError, match="S3_BUCKET_NAME not set"):
            upload_to_s3.upload_all_raw_files()

    def test_upload_file_logs_success(self):
        """upload_file should return True and not raise on a successful upload."""
        from ingestion.upload_to_s3 import upload_file

        mock_s3 = MagicMock()
        result = upload_file(mock_s3, Path("data/raw/breweries.json"), "raw/breweries.json")

        assert result is True
        mock_s3.upload_file.assert_called_once()

    def test_upload_file_returns_false_on_error(self):
        """upload_file should return False (not raise) when S3 call fails."""
        from ingestion.upload_to_s3 import upload_file

        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = Exception("Connection error")

        result = upload_file(mock_s3, Path("data/raw/breweries.json"), "raw/breweries.json")

        assert result is False
