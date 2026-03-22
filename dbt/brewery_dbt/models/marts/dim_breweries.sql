-- depends_on: {{ ref("stg_breweries") }}
-- dim_breweries.sql
-- Final brewery dimension table — one row per brewery

with stg as (
    select * from {{ ref('stg_breweries') }}
)

select
    brewery_id,
    brewery_name,
    brewery_type,
    city,
    state,
    latitude,
    longitude,
    website_url,
    has_coordinates,
    climate_zone,

    -- buckets for dashboard filtering
    case
        when brewery_type in ('micro', 'nano', 'brewpub') then 'craft'
        when brewery_type in ('regional', 'large')        then 'commercial'
        when brewery_type = 'taproom'                     then 'taproom'
        else 'other'
    end as brewery_category

from stg
