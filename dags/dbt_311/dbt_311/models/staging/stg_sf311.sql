with cte as (
    select *
    from {{ source("sf311","sfdata_311")}}
    )

select service_request_id, requested_datetime, updated_datetime, DATE(requested_datetime) AS requested_date, 
CASE EXTRACT(DOW FROM DATE(requested_datetime)) 
    WHEN 0 THEN 'Sunday'
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday' END AS day_of_week,
(date_trunc('week', DATE(requested_datetime)) + interval '6 days')::date AS week_end_date,
(date_trunc('month', DATE(requested_datetime)) + interval '1 month - 1 day')::date AS month_end_date,
status_description, status_notes, agency_responsible, service_name, service_subtype,
service_details, supervisor_district, neighborhoods_sffind_boundaries, police_district,
source, tractce, name, neighborhoods_analysis_boundaries, lat, long
from 
    cte
order by 
    service_request_id