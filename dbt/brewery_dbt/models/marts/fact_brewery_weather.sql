-- depends_on: {{ ref("dim_breweries") }}
-- depends_on: {{ ref("stg_weather_summary") }}

with breweries as (
    select * from {{ ref('dim_breweries') }}
),

weather as (
    select * from {{ ref('stg_weather_summary') }}
),

joined as (
    select
        b.brewery_id,
        b.brewery_name,
        b.brewery_type,
        b.brewery_category,
        b.city,
        b.state,
        b.latitude,
        b.longitude,
        b.climate_zone,
        b.has_coordinates,
        w.state_avg_temp_max_f,
        w.state_avg_temp_min_f,
        w.state_avg_precipitation,
        w.state_avg_windspeed,
        w.brewery_count         as breweries_in_state

    from breweries b
    left join weather w
        on b.state = w.state
)

select * from joined
