# San Francisco 311 Forecasting Project 
San Francisco receives thousands of service requests through its 311 system on a daily basis. The City has an impressive response time given limited resources and the sheer volume of complaints. The purpose of this project is to forecast San Francisco 311 complaints by region to help improve service response time and resource allocation during times of heightened complaint volume.
# Data Sources
Data from the following sources will be used to train our model and provide forecasts:
1. San Francisco 311 Compalints
2. Historical and forecast weather (e.g. temperature, precipitation, sun, daylight hours, etc.)
3. Twitter
4. Neighborhood and census boundaries
# Architecture
- Data Storage: Amazon Web Services (S3, AWS Glue, Postgres Running on EC2 Instance)
- Pipeline orchestration: Airflow, dbt
- Data Transformations: Python, SQL (coupled with dbt)
- TDB: forecasting algorithm deployment, dashboarding, notifications
