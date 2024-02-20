select 
    requested_date, COUNT(DISTINCT service_request_id) AS daily_requests
from 
    {{ ref('stg_sf311')}}
group by 
    1