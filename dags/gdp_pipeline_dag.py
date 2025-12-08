from datetime import datetime, timedelta
import shutil
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark_submit_operator import SparkSubmitOperator
from gdp_download_operator import GDPDownloadOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def cleanup_directories():
    """Delete bronze and gold directories after validation."""
    base_path = '/opt/airflow/data'
    bronze_path = os.path.join(base_path, 'bronze')
    gold_path = os.path.join(base_path, 'gold')
    
    deleted = []
    for path, name in [(bronze_path, 'bronze'), (gold_path, 'gold')]:
        if os.path.exists(path):
            shutil.rmtree(path)
            deleted.append(name)
            print(f"Deleted {name} directory")
    
    if deleted:
        print(f"Cleaned up: {', '.join(deleted)}")

dag = DAG(
    'gdp_pipeline_dag.py',
    default_args=default_args,
    description='GDP data pipeline',
    schedule_interval='@yearly',
    catchup=False,
    tags=['gdp'],
)

download_gdp = GDPDownloadOperator(
    task_id='download_gdp_data',
    data_path='/opt/airflow/data/gdp/',
    fips_csv_path='/opt/airflow/data/valid_fips_codes.csv',
    timeout=15,
    update_invalid_codes=True,
    s3_bucket=os.getenv('S3_BUCKET_NAME'),
    s3_prefix='raw/gdp/',
)

bulk_load_bronze = SparkSubmitOperator(
    task_id='bulk_load_bronze_table',
    application='/opt/airflow/jobs/bulk_load_gdp.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    spark_conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    },
    dag=dag,
)

process_gold_tables = SparkSubmitOperator(
    task_id='process_gold_tables',
    application='/opt/airflow/jobs/process_gdp_data.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    spark_conf={
        'spark.sql.adaptive.enabled': 'true',
    },
    dag=dag,
)

validate_gold_tables = SparkSubmitOperator(
    task_id='validate_gold_tables',
    application='/opt/airflow/jobs/validate_gold_tables.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    dag=dag,
)

load_to_snowflake = SparkSubmitOperator(
    task_id='load_to_snowflake',
    application='/opt/airflow/jobs/load_to_snowflake.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    packages='net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1',
    dag=dag,
)

validate_snowflake = SparkSubmitOperator(
    task_id='validate_snowflake_tables',
    application='/opt/airflow/jobs/validate_snowflake_tables.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    packages='net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1',
    dag=dag,
)

cleanup = PythonOperator(
    task_id='cleanup_directories',
    python_callable=cleanup_directories,
    dag=dag,
)

download_gdp >> bulk_load_bronze >> process_gold_tables >> validate_gold_tables >> load_to_snowflake >> validate_snowflake >> cleanup
