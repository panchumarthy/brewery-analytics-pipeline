"""
fetch_breweries.py
------------------
Fetches all US breweries from the Open Brewery DB API (no auth required).
Saves raw JSON responses as newline-delimited JSON to a local staging area,
ready to be uploaded to S3 by upload_to_s3.py.

API docs: https://www.openbrewerydb.org/documentation
"""

import requests
import json
import logging
import time
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200          # max allowed by the API
MAX_RETRIES = 3
RETRY_DELAY = 2         # seconds between retries
OUTPUT_DIR = Path("data/raw/breweries")


def fetch_page(page: int, per_page: int = PER_PAGE) -> list[dict]:
    """Fetch a single page of brewery records with retry logic."""
    params = {"page": page, "per_page": per_page}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for page {page}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                raise


def fetch_all_breweries() -> list[dict]:
    """Paginate through all brewery records and return as a flat list."""
    all_breweries = []
    page = 1

    logger.info("Starting brewery data fetch from Open Brewery DB...")

    while True:
        logger.info(f"Fetching page {page}...")
        records = fetch_page(page)

        if not records:
            logger.info(f"No more records found at page {page}. Done.")
            break

        all_breweries.extend(records)
        logger.info(f"Page {page}: {len(records)} records fetched (total so far: {len(all_breweries)})")

        page += 1
        time.sleep(0.2)  # be polite to the free API

    return all_breweries


def save_to_file(breweries: list[dict]) -> Path:
    """
    Save brewery records to a date-partitioned JSON file.
    Format: data/raw/breweries/year=2024/month=03/breweries.json
    """
    today = datetime.utcnow()
    output_path = OUTPUT_DIR / f"year={today.year}" / f"month={today.month:02d}"
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "breweries.json"

    with open(file_path, "w") as f:
        for record in breweries:
            f.write(json.dumps(record) + "\n")

    logger.info(f"Saved {len(breweries)} records to {file_path}")
    return file_path


def main():
    breweries = fetch_all_breweries()

    if not breweries:
        logger.error("No brewery data fetched. Exiting.")
        return

    logger.info(f"Total breweries fetched: {len(breweries)}")
    file_path = save_to_file(breweries)
    logger.info(f"Ingestion complete. File ready for S3 upload: {file_path}")

    return str(file_path)


if __name__ == "__main__":
    main()
