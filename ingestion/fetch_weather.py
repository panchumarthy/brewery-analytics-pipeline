"""
fetch_weather.py
----------------
Enriches brewery records with historical weather data from Open-Meteo API.
For each brewery that has latitude/longitude, fetches the last 7 days of
daily weather at that location.

API docs: https://open-meteo.com/en/docs (no auth required, free tier)
Reads:  data/raw/breweries/year=.../month=.../breweries.json
Writes: data/raw/weather/year=.../month=.../weather.json
"""

import requests
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
OUTPUT_DIR = Path("data/raw/weather")
BATCH_SIZE = 50         # log progress every N breweries
RETRY_DELAY = 2


def find_latest_brewery_file() -> Path:
    """Find the most recently written brewery JSON file."""
    brewery_dir = Path("data/raw/breweries")
    files = sorted(brewery_dir.rglob("breweries.json"), reverse=True)
    if not files:
        raise FileNotFoundError(
            "No brewery data found. Run fetch_breweries.py first."
        )
    return files[0]


def load_breweries(file_path: Path) -> list[dict]:
    """Load brewery records from newline-delimited JSON."""
    breweries = []
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if line:
                breweries.append(json.loads(line))
    logger.info(f"Loaded {len(breweries)} breweries from {file_path}")
    return breweries


def filter_geocoded(breweries: list[dict]) -> list[dict]:
    """Keep only breweries that have valid lat/lng coordinates."""
    geocoded = [
        b for b in breweries
        if b.get("latitude") and b.get("longitude")
    ]
    skipped = len(breweries) - len(geocoded)
    logger.info(
        f"{len(geocoded)} breweries have coordinates. "
        f"{skipped} skipped (no lat/lng)."
    )
    return geocoded


def fetch_weather_for_brewery(brewery: dict) -> dict | None:
    """
    Fetch last 7 days of daily weather for a single brewery location.
    Returns a weather record dict, or None on failure.
    """
    lat = brewery["latitude"]
    lon = brewery["longitude"]
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=7)

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
        ],
        "temperature_unit": "fahrenheit",
        "windspeed_unit": "mph",
        "precipitation_unit": "inch",
        "start_date": str(start_date),
        "end_date": str(end_date),
        "timezone": "America/New_York",
    }

    for attempt in range(1, 4):
        try:
            response = requests.get(WEATHER_URL, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            return {
                "brewery_id": brewery["id"],
                "brewery_name": brewery["name"],
                "state": brewery.get("state"),
                "city": brewery.get("city"),
                "latitude": lat,
                "longitude": lon,
                "fetch_date": str(end_date),
                "weather": data.get("daily", {}),
            }

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Attempt {attempt}/3 failed for {brewery['name']}: {e}"
            )
            if attempt < 3:
                time.sleep(RETRY_DELAY * attempt)

    return None


def fetch_all_weather(breweries: list[dict]) -> list[dict]:
    """Iterate through breweries and collect weather records."""
    results = []
    failed = 0

    for i, brewery in enumerate(breweries, 1):
        record = fetch_weather_for_brewery(brewery)

        if record:
            results.append(record)
        else:
            failed += 1
            logger.warning(f"Failed to fetch weather for: {brewery['name']}")

        if i % BATCH_SIZE == 0:
            logger.info(
                f"Progress: {i}/{len(breweries)} breweries processed "
                f"({failed} failed)"
            )

        time.sleep(0.1)  # rate limit: ~10 requests/sec

    logger.info(
        f"Weather fetch complete. {len(results)} succeeded, {failed} failed."
    )
    return results


def save_weather(records: list[dict]) -> Path:
    """Save weather records to a date-partitioned JSON file."""
    today = datetime.utcnow()
    output_path = OUTPUT_DIR / f"year={today.year}" / f"month={today.month:02d}"
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "weather.json"
    with open(file_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    logger.info(f"Saved {len(records)} weather records to {file_path}")
    return file_path


def main():
    brewery_file = find_latest_brewery_file()
    breweries = load_breweries(brewery_file)
    geocoded = filter_geocoded(breweries)

    weather_records = fetch_all_weather(geocoded)

    if not weather_records:
        logger.error("No weather data collected. Check API connectivity.")
        return

    file_path = save_weather(weather_records)
    logger.info(f"Weather ingestion complete. File ready: {file_path}")
    return str(file_path)


if __name__ == "__main__":
    main()
