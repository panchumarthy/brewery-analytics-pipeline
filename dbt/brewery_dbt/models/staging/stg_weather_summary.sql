-- depends_on: {{ ref("stg_breweries") }}
-- stg_weather_summary.sql
-- Summarises weather stats by state

with base as (
    select * from {{ ref('stg_breweries') }}
    where climate_zone != 'unknown'
)

select
    state,
    climate_zone,
    round(avg(avg_temp_max_f), 2)        as state_avg_temp_max_f,
    round(avg(avg_temp_min_f), 2)        as state_avg_temp_min_f,
    round(avg(avg_precipitation_inch), 2) as state_avg_precipitation,
    round(avg(avg_windspeed_mph), 2)     as state_avg_windspeed,
    count(*)                             as brewery_count

from base
group by state, climate_zone
