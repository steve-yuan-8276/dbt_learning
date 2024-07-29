select
    fr.listing_id,
    fr.review_date,
    dl.created_at
from
    {{ ref('fct_reviews') }} fr join {{ ref('dim_listing_cleansed') }} dl 
    on fr.listing_id = dl.listing_id
where   
    fr.review_date < dl.created_at
