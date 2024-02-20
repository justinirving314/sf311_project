select 
    requested_date, tractce, neighborhoods_analysis_boundaries, 
    COUNT(DISTINCT service_request_id) AS daily_requests
from 
    {{ ref('stg_sf311')}}
group by 
    1,2,3