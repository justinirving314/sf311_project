{{ config(materialized="table", target="public") }}

select 
    requested_date, service_categories, agency_categories, day_of_week,
    COUNT(DISTINCT service_request_id) AS daily_requests
from 
    {{ ref('stg_sf311')}}
group by 
    1,2,3,4