select 
    DISTINCT service_name
from 
    {{ ref('stg_sf311')}}

CASE WHEN service_name IN ('Catch Basin Maintenance', 'Sewer Issues', 'Tree Maintenance','Street Defects',
'Street and Sidewalk Cleaning') THEN 'weather_related'
    WHEN service_name IN ('General Request - HUMAN SERVICES AGENCY','')