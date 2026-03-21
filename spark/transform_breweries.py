"""
transform_breweries.py
----------------------
PySpark job that reads raw brewery and weather JSON files,
cleans and transforms them, joins on brewery_id, and writes
the result as Parquet to data/processed/.

Run locally with:
    python spark/transform_breweries.py

Or with spark-submit:
    spark-submit spark/transform_breweries.py
"""

import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────
RAW_BREWERIES  = "/home/panch/brewery-pipeline/data/raw/breweries/*/*/breweries.json"
RAW_WEATHER    = "/home/panch/brewery-pipeline/data/raw/weather/*/*/weather.json"
OUTPUT_PATH    = "/home/panch/brewery_output/brewery_weather"


# ── Schema definitions ─────────────────────────────────────────────────────
BREWERY_SCHEMA = StructType([
    StructField("id",            StringType(),  True),
    StructField("name",          StringType(),  True),
    StructField("brewery_type",  StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("state",         StringType(),  True),
    StructField("country",       StringType(),  True),
    StructField("latitude",      StringType(),  True),  # comes as string
    StructField("longitude",     StringType(),  True),  # comes as string
    StructField("website_url",   StringType(),  True),
    StructField("phone",         StringType(),  True),
])


def create_spark_session() -> SparkSession:
    """Create a local Spark session."""
    spark = (
        SparkSession.builder
        .appName("BreweryAnalyticsPipeline")
        .master("local[*]")              # use all local CPU cores
        .config("spark.sql.shuffle.partitions", "4")   # small data = fewer partitions
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # reduce Spark's verbose logging
    logger.info("Spark session created.")
    return spark


# ── Brewery transformations ────────────────────────────────────────────────

def read_breweries(spark: SparkSession):
    """Read raw brewery NDJSON files."""
    logger.info(f"Reading breweries from: {RAW_BREWERIES}")
    df = spark.read.schema(BREWERY_SCHEMA).json(RAW_BREWERIES)
    logger.info(f"Raw brewery records: {df.count()}")
    return df


def clean_breweries(df):
    """
    Clean the brewery dataset:
    - Filter to US only
    - Cast lat/lng to float
    - Standardise brewery_type
    - Drop rows missing critical fields
    - Rename id to brewery_id
    """
    logger.info("Cleaning brewery data...")

    df = (
        df
        # rename id to brewery_id for clarity in joins
        .withColumnRenamed("id", "brewery_id")

        # cast coordinates from string to float
        .withColumn("latitude",  F.col("latitude").cast(FloatType()))
        .withColumn("longitude", F.col("longitude").cast(FloatType()))

        # filter to US breweries only (nulls and non-US excluded)
        .filter(
            (F.col("country") == "United States") |
            (F.col("country").isNull())            # many rows omit country field
        )

        # standardise brewery_type to lowercase
        .withColumn(
            "brewery_type",
            F.lower(F.trim(F.col("brewery_type")))
        )

        # drop rows missing name or state — unusable for analytics
        .filter(F.col("name").isNotNull())
        .filter(F.col("state").isNotNull())

        # add a flag for whether we have GPS coordinates
        .withColumn(
            "has_coordinates",
            F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        )

        # drop unused columns
        .drop("country", "phone")
    )

    logger.info(f"Clean brewery records: {df.count()}")
    return df


# ── Weather transformations ────────────────────────────────────────────────

def read_weather(spark: SparkSession):
    """Read raw weather NDJSON files."""
    logger.info(f"Reading weather from: {RAW_WEATHER}")
    df = spark.read.json(RAW_WEATHER)
    logger.info(f"Raw weather records: {df.count()}")
    return df


def clean_weather(df):
    """
    Flatten the nested weather struct into scalar columns.
    Each record has arrays of daily values — we take the mean
    across the 7-day window as a single summary per brewery.

    Input schema (nested):
        brewery_id, weather.temperature_2m_max (array),
        weather.precipitation_sum (array), ...

    Output schema (flat):
        brewery_id, avg_temp_max_f, avg_temp_min_f,
        avg_precipitation_inch, avg_windspeed_mph, fetch_date
    """
    logger.info("Flattening and cleaning weather data...")

    df = (
        df
        .select(
            F.col("brewery_id"),
            F.col("fetch_date"),
            # compute 7-day averages from the daily arrays
            F.aggregate(
                F.col("weather.temperature_2m_max"),
                F.lit(0.0),
                lambda acc, x: acc + x,
                lambda acc: acc / F.size(F.col("weather.temperature_2m_max"))
            ).cast(FloatType()).alias("avg_temp_max_f"),

            F.aggregate(
                F.col("weather.temperature_2m_min"),
                F.lit(0.0),
                lambda acc, x: acc + x,
                lambda acc: acc / F.size(F.col("weather.temperature_2m_min"))
            ).cast(FloatType()).alias("avg_temp_min_f"),

            F.aggregate(
                F.col("weather.precipitation_sum"),
                F.lit(0.0),
                lambda acc, x: acc + x,
                lambda acc: acc / F.size(F.col("weather.precipitation_sum"))
            ).cast(FloatType()).alias("avg_precipitation_inch"),

            F.aggregate(
                F.col("weather.windspeed_10m_max"),
                F.lit(0.0),
                lambda acc, x: acc + x,
                lambda acc: acc / F.size(F.col("weather.windspeed_10m_max"))
            ).cast(FloatType()).alias("avg_windspeed_mph"),
        )
        .filter(F.col("brewery_id").isNotNull())
    )

    logger.info(f"Clean weather records: {df.count()}")
    return df


# ── Join & enrich ──────────────────────────────────────────────────────────

def join_brewery_weather(breweries_df, weather_df):
    """
    Left join breweries with weather on brewery_id.
    Breweries without weather data are kept (nulls in weather cols).
    """
    logger.info("Joining brewery and weather datasets...")

    joined = (
        breweries_df
        .join(weather_df, on="brewery_id", how="left")
    )

    total       = joined.count()
    with_weather = joined.filter(F.col("avg_temp_max_f").isNotNull()).count()

    logger.info(f"Joined records: {total} total, {with_weather} with weather data")
    return joined


def add_derived_columns(df):
    """Add extra analytics-friendly columns."""
    return (
        df
        # temperature category based on avg max temp
        .withColumn(
            "climate_zone",
            F.when(F.col("avg_temp_max_f") >= 80, "hot")
             .when(F.col("avg_temp_max_f") >= 60, "moderate")
             .when(F.col("avg_temp_max_f") >= 40, "cool")
             .when(F.col("avg_temp_max_f").isNotNull(), "cold")
             .otherwise("unknown")
        )
        # partition column for efficient querying
        .withColumn("state_code", F.upper(F.substring(F.col("state"), 1, 2)))
    )


# ── Write output ───────────────────────────────────────────────────────────

def write_parquet(df, output_path: str):
    """
    Write final dataset as Parquet, partitioned by state.
    Partitioning makes Athena queries much faster and cheaper
    when filtering by state.
    """
    logger.info(f"Writing Parquet to: {output_path}")

    (
        df
        .repartition("state")           # one file per state
        .write
        .mode("overwrite")
        .partitionBy("state")
        .parquet(output_path)
    )

    # count output files written
    output_files = list(Path(output_path).rglob("*.parquet"))
    logger.info(f"Parquet write complete. {len(output_files)} partition files written.")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()

    try:
        # 1. Read
        raw_breweries = read_breweries(spark)
        raw_weather   = read_weather(spark)

        # 2. Clean
        clean_brew    = clean_breweries(raw_breweries)
        clean_weath   = clean_weather(raw_weather)

        # 3. Join
        joined        = join_brewery_weather(clean_brew, clean_weath)

        # 4. Enrich
        final         = add_derived_columns(joined)

        # 5. Preview
        logger.info("Sample output:")
        final.select(
            "brewery_id", "name", "state", "brewery_type",
            "avg_temp_max_f", "climate_zone"
        ).show(10, truncate=False)

        # 6. Write
        write_parquet(final, OUTPUT_PATH)

        logger.info("Spark transformation complete!")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
