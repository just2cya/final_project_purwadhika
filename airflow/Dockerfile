FROM apache/airflow:2.10.2-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.12.0 \
    apache-airflow-providers-google==10.21.1 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    google-cloud-bigquery==3.17.2 \
    google-cloud-bigquery-storage==2.24.0 \
    pyarrow==15.0.0 \
    apache-airflow-providers-telegram==4.1.0 \
    dbt-core==1.5.0 \
    dbt-bigquery==1.5.0

# Set working directory
WORKDIR /opt/airflow