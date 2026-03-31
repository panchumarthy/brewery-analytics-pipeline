# AWS Setup Guide

## S3 Bucket Structure

```
brewery-analytics-<yourname>/
├── raw/
│   ├── breweries/year=2026/month=03/breweries.json
│   └── weather/year=2026/month=03/weather.json
├── processed/
│   └── brewery_weather/state=*/part-*.parquet
└── athena-results/
```

## S3 Setup Steps

1. Create S3 bucket: `brewery-analytics-<yourname>`
2. Region: `us-east-2` (or your default region)
3. Create folders: `raw/`, `processed/`, `athena-results/`

## IAM Setup

1. Create IAM user: `brewery-pipeline-user`
2. Attach policy: `AmazonS3FullAccess`
3. Create access key and save to `.env` file

## Environment Variables

Copy `.env.example` to `.env` and fill in:
```
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-2
S3_BUCKET_NAME=brewery-analytics-<yourname>
```

## Athena Setup

1. Open Athena in AWS Console
2. Set query result location: `s3://brewery-analytics-<yourname>/athena-results/`
3. Create database:
```sql
CREATE DATABASE brewery_db;
```

4. Create raw breweries table:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS brewery_db.raw_breweries (
    id STRING,
    name STRING,
    brewery_type STRING,
    city STRING,
    state STRING,
    country STRING,
    latitude STRING,
    longitude STRING,
    website_url STRING,
    phone STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://brewery-analytics-<yourname>/raw/breweries/year=2026/month=03/'
TBLPROPERTIES ('has_encrypted_data'='false');
```

5. Verify with:
```sql
SELECT state, COUNT(*) as brewery_count
FROM brewery_db.raw_breweries
GROUP BY state
ORDER BY brewery_count DESC
LIMIT 10;
```
