{{
  config(
    materialized = 'incremental',
    on_schema_change = 'fail'
  )
}}

{% set this_table = this %}
{% do log(this_table, info=true) %}

with src_reviews as (
  select * from {{ ref('src_reviews') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} as review_id,
  *
from
  src_reviews
where
  "REVIEW_TEXT" is not null
  {% if is_incremental() %}
  and "REVIEW_DATE" > (
      select max("REVIEW_DATE")
      from {{ this }}
  )
  {% endif %}
