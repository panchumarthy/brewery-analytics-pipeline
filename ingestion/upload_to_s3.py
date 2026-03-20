"""
upload_to_s3.py
---------------
Uploads locally staged raw data files to S3, preserving the
date-partition folder structure (year=.../month=...).

Reads AWS config from environment variables (.env file).
Called by the Airflow DAG after fetch_breweries and fetch_weather.
"""

import boto3
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
LOCAL_DATA_DIR = Path("data/raw")


def get_s3_client():
    """Create and return a boto3 S3 client using env credentials."""
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def upload_file(s3_client, local_path: Path, s3_key: str) -> bool:
    """Upload a single file to S3. Returns True on success."""
    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=S3_BUCKET,
            Key=s3_key,
        )
        logger.info(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {local_path}: {e}")
        return False


def upload_all_raw_files() -> dict:
    """
    Walk the local data/raw/ directory and upload every file to S3
    under the raw/ prefix, preserving the folder structure.

    Example:
      local:  data/raw/breweries/year=2024/month=03/breweries.json
      s3 key: raw/breweries/year=2024/month=03/breweries.json
    """
    if not S3_BUCKET:
        raise ValueError(
            "S3_BUCKET_NAME not set. Check your .env file."
        )

    s3 = get_s3_client()
    results = {"uploaded": 0, "failed": 0}

    json_files = list(LOCAL_DATA_DIR.rglob("*.json"))

    if not json_files:
        logger.warning(f"No JSON files found under {LOCAL_DATA_DIR}")
        return results

    logger.info(f"Found {len(json_files)} file(s) to upload to s3://{S3_BUCKET}")

    for local_path in json_files:
        # Convert local path to S3 key
        # e.g. data/raw/breweries/... → raw/breweries/...
        relative = local_path.relative_to(Path("data"))
        s3_key = str(relative).replace("\\", "/")  # Windows path fix

        success = upload_file(s3, local_path, s3_key)
        if success:
            results["uploaded"] += 1
        else:
            results["failed"] += 1

    logger.info(
        f"Upload complete: {results['uploaded']} succeeded, "
        f"{results['failed']} failed."
    )
    return results


def main():
    logger.info(f"Starting S3 upload to bucket: {S3_BUCKET}")
    results = upload_all_raw_files()

    if results["failed"] > 0:
        raise RuntimeError(
            f"{results['failed']} file(s) failed to upload. Check logs."
        )

    logger.info("All files uploaded successfully.")
    return results


if __name__ == "__main__":
    main()
