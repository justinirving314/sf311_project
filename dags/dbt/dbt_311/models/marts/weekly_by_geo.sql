{{ config(materialized="table", target="public") }}

select 
    week_end_date, tractce, neighborhoods_analysis_boundaries, service_categories, agency_categories,
    COUNT(DISTINCT service_request_id) AS daily_requests
from 
    {{ ref('stg_sf311')}}
group by 
    1,2,3,4,5


