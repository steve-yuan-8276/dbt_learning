{{
  config(
    materialized = 'table',
    )
}}

with fct_reviews as (
    select * from {{ ref('fct_reviews') }}
), 
full_moon_date as (
    select * from {{ ref('seed_full_moon_dates') }}
)

select
    fr.*,
    case    
        when fm.full_moon_date is null then 'Not Full Moon'
        else 'Full Moon'
        end as is_full_moon
from
    fct_reviews fr left join full_moon_date fm
    on (To_Date(fr.REVIEW_DATE) = dateadd(day, 1, fm.full_moon_date))