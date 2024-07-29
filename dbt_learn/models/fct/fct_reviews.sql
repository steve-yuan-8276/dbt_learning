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
