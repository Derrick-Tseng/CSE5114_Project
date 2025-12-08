# Dockerfile
# Use the official Airflow image with Python 3.8 to match Spark worker
FROM apache/airflow:2.9.3-python3.8

USER root

# Install Java and Spark binaries
RUN apt-get update && \
    apt-get install -y default-jre wget && \
    wget -q https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.3-bin-hadoop3.tgz -C /opt && \
    rm spark-3.5.3-bin-hadoop3.tgz && \
    ln -s /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Create directories with proper permissions for airflow user
RUN mkdir -p /opt/airflow/data/processed /opt/airflow/data/gdp && \
    chown -R airflow:root /opt/airflow/data && \
    chmod -R 775 /opt/airflow/data

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.10.0" \
    "apache-airflow-providers-http==4.12.0" \
    "apache-airflow-providers-common-sql==1.17.1" \
    "apache-airflow-providers-snowflake==5.7.1" \
    "pyspark==3.5.3" \
    "snowflake-connector-python==3.12.2" \
    "boto3"