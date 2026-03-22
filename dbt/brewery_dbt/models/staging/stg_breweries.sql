-- stg_breweries.sql
-- Reads directly from the Parquet files Spark wrote
-- Casts types and renames columns cleanly

with source as (
    select *
    from read_parquet(
        '/home/panch/brewery_output/brewery_weather/state=*/*.parquet',
        hive_partitioning = true
    )
),

renamed as (
    select
        brewery_id,
        name                                    as brewery_name,
        brewery_type,
        city,
        state,
        cast(latitude  as double)               as latitude,
        cast(longitude as double)               as longitude,
        website_url,
        has_coordinates,
        avg_temp_max_f,
        avg_temp_min_f,
        avg_precipitation_inch,
        avg_windspeed_mph,
        climate_zone,
        fetch_date

    from source
    where brewery_id is not null
      and brewery_name is not null
)

select * from renamed
