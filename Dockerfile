# Start from Apache Airflow base image
FROM apache/airflow:2.8.1

# Set environment variables to enable pickling for XCom
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

#Copy requirements.txt file
COPY requirements.txt /requirements.txt

# Install GDAL dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gdal-bin \
        libgdal-dev \
        && rm -rf /var/lib/apt/lists/*




# Switch back to the airflow user
USER airflow

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

# Set environment variables for GDAL
ENV GDAL_DATA=/usr/share/gdal/2.4
ENV PROJ_LIB=/usr/share/proj


USER root
# Install system dependencies
RUN apt-get update && \
    apt-get install -y python3-venv

# Create a directory for the virtual environment
RUN mkdir /opt/dbt_env

# Create and activate the virtual environment
RUN python3 -m venv /opt/dbt_env
ENV PIP_USER=false
ENV PATH="/opt/dbt_env/bin:$PATH"

#RUN pip install --user --upgrade pip


# Install DBT and dependencies
RUN pip install --no-cache-dir dbt-core dbt-postgres