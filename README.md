# 🍺 Brewery Analytics Pipeline

An end-to-end data engineering project that ingests US brewery data from a public API, enriches it with weather information, transforms it using PySpark and dbt, and serves analytics via a dashboard.

Built to demonstrate real-world data engineering skills: pipeline orchestration, data lake architecture, SQL transformations, and cloud deployment.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Orchestration                            │
│                     Apache Airflow DAG                          │
│              (ingest → transform → load → test)                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
  │ Open Brewery  │ │  Open-Meteo   │ │  Future APIs  │
  │   DB API      │ │  Weather API  │ │  (extensible) │
  └───────┬───────┘ └───────┬───────┘ └───────────────┘
          │                 │
          └────────┬────────┘
                   ▼
        ┌──────────────────────┐
        │   S3 Raw Data Lake   │
        │  (JSON / Parquet)    │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │    PySpark Job       │
        │  Clean + Join data   │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │  S3 Processed Zone   │
        │  (cleaned Parquet)   │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │     dbt Models       │
        │  staging → marts     │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │     AWS Athena       │
        │   (SQL interface)    │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │  Metabase Dashboard  │
        │  Charts + Insights   │
        └──────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + `requests` | Fetch data from REST APIs |
| Storage | AWS S3 | Raw and processed data lake |
| Processing | Apache Spark (PySpark) | Large-scale data transformation |
| Orchestration | Apache Airflow | DAG scheduling and monitoring |
| Transformation | dbt | SQL models, testing, documentation |
| Query engine | AWS Athena | Serverless SQL over S3 |
| Visualisation | Metabase | Open-source dashboard |
| Containerisation | Docker + Docker Compose | Local dev environment |

---

## Project Structure

```
brewery-analytics-pipeline/
│
├── ingestion/                    # Python scripts to fetch and upload data
│   ├── fetch_breweries.py        # Calls Open Brewery DB API
│   ├── fetch_weather.py          # Calls Open-Meteo weather API
│   └── upload_to_s3.py           # Uploads raw files to S3
│
├── spark/                        # PySpark transformation jobs
│   └── transform_breweries.py    # Cleans, joins, writes Parquet
│
├── dbt/brewery_dbt/              # dbt project
│   ├── models/
│   │   ├── staging/              # stg_* models — raw → typed
│   │   └── marts/                # dim_* and fact_* models
│   ├── tests/                    # Custom dbt data tests
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/
│       └── brewery_pipeline_dag.py   # Main DAG definition
│
├── docker/
│   ├── docker-compose.yml        # All services in one command
│   ├── Dockerfile.airflow
│   └── Dockerfile.spark
│
├── tests/                        # Python unit tests (pytest)
│   ├── test_ingestion.py
│   └── test_transform.py
│
├── dashboard/                    # Metabase export / screenshots
├── docs/                         # Architecture diagrams, notes
├── .env.example                  # Copy to .env — fill in your keys
├── requirements.txt
├── Makefile                      # Common commands
└── README.md
```

---

## Data Sources

### Open Brewery DB
- **URL:** https://www.openbrewerydb.org/
- **Auth:** None required
- **Fields used:** `id`, `name`, `brewery_type`, `city`, `state`, `latitude`, `longitude`, `website_url`
- **Volume:** ~8,000 US breweries

### Open-Meteo
- **URL:** https://open-meteo.com/
- **Auth:** None required (free tier)
- **Fields used:** `temperature_2m_max`, `precipitation_sum`, `windspeed_10m_max`
- **Enrichment:** Daily weather at each brewery's coordinates

---

## dbt Data Models

```
Raw S3 data
    │
    ▼
┌─────────────────────────────┐
│  staging layer              │
│  stg_breweries              │  ← type-cast, rename, basic clean
│  stg_weather                │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│  marts layer                │
│  dim_breweries              │  ← brewery dimension table
│  fact_brewery_weather       │  ← one row per brewery per day
└─────────────────────────────┘
```

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker Desktop
- AWS account (free tier is enough)
- Git

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/brewery-analytics-pipeline.git
cd brewery-analytics-pipeline
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Open .env and fill in your AWS credentials and S3 bucket name
```

### 3. Install dependencies
```bash
make setup
```

### 4. Start services with Docker
```bash
make up
# Airflow UI → http://localhost:8080  (user: admin / pass: admin)
# Spark UI  → http://localhost:4040
```

### 5. Run the pipeline manually
```bash
# Trigger ingestion
python ingestion/fetch_breweries.py
python ingestion/fetch_weather.py

# Run Spark transformation
spark-submit spark/transform_breweries.py

# Run dbt models
make dbt-run

# Run tests
make test
```

---

## Airflow DAG

The pipeline runs daily on a schedule:

```
brewery_pipeline_dag
│
├── task: fetch_breweries      (PythonOperator)
├── task: fetch_weather        (PythonOperator)  ← runs after fetch_breweries
├── task: spark_transform      (BashOperator)    ← runs after both fetches
├── task: dbt_run              (BashOperator)    ← runs after spark
└── task: dbt_test             (BashOperator)    ← runs after dbt_run
```

---

## Dashboard Insights

The Metabase dashboard answers:

- Which US states have the most breweries?
- What types of breweries (micro, nano, taproom, regional) dominate each region?
- Is there a correlation between climate and craft brewery density?
- How has brewery count changed over time by state?

---

## Running Tests

```bash
make test
```

Tests cover:
- API response parsing and error handling
- PySpark schema validation and null handling
- dbt model row counts and uniqueness assertions

---

## AWS Setup (Free Tier)

1. Create an S3 bucket: `brewery-analytics-yourname`
2. Create two folders inside: `raw/` and `processed/`
3. Enable AWS Athena in your region
4. Create an Athena database: `brewery_db`
5. Add your credentials to `.env`

Estimated monthly cost on free tier: **< $1**

---

## What I Learned / Skills Demonstrated

- Building production-style ETL pipelines with Airflow DAGs
- Processing data at scale with PySpark
- Applying the dbt staging → marts layering pattern
- Designing a partitioned S3 data lake (by `year/month/day`)
- Writing data quality tests with dbt and pytest
- Containerising a multi-service data stack with Docker Compose
- Serverless querying with AWS Athena

---

## Roadmap

- [ ] Add incremental dbt models (avoid full refresh)
- [ ] Add Great Expectations for deeper data quality checks
- [ ] Deploy Airflow to AWS MWAA
- [ ] Stream live tap-room check-in data with Kafka

---

## Author

**Your Name**
Data Engineer | [LinkedIn](https://linkedin.com/in/yourprofile) | [GitHub](https://github.com/yourusername)

---

## License

MIT License — free to use, adapt, and share.
