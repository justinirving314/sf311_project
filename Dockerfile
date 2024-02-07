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