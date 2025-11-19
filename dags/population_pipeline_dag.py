from datetime import datetime, timedelta
from airflow import DAG
from spark_submit_operator import SparkSubmitOperator
from population_download_operator import PopulationDownloadOperator
from file_cleanup_operator import FileCleanupOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'population_pipeline_dag',
    default_args=default_args,
    description='Population data pipeline - download population data by age and sex',
    schedule_interval='@yearly',
    catchup=False,
    tags=['population'],
)

download_population = PopulationDownloadOperator(
    task_id='download_population_data',
    data_path='/opt/airflow/data/population/',
    url='https://www.statsamerica.org/downloads/Population-by-Age-and-Sex.zip',
    timeout=60,
    extract=True,
    dag=dag,
)

cleanup_files = FileCleanupOperator(
    task_id='cleanup_files',
    data_path='/opt/airflow/data/population/',
    files_to_remove=[
        'Population by Age and Sex - EDDs.csv',
        'Population by Age and Sex - Metadata.csv',
        'Population by Age and Sex - Metros, Micros.csv',
        'Population-by-Age-and-Sex.zip'
    ],
    dag=dag,
)

load_population_bronze = SparkSubmitOperator(
    task_id='load_population_bronze',
    application='/opt/airflow/jobs/load_population_data.py',
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

load_to_snowflake = SparkSubmitOperator(
    task_id='load_population_to_snowflake',
    application='/opt/airflow/jobs/load_population_to_snowflake.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    packages='net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1',
    dag=dag,
)

process_population_gold = SparkSubmitOperator(
    task_id='process_population_gold',
    application='/opt/airflow/jobs/process_population_data.py',
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

load_processed_to_snowflake = SparkSubmitOperator(
    task_id='load_processed_population_to_snowflake',
    application='/opt/airflow/jobs/load_processed_population_to_snowflake.py',
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    packages='net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1',
    dag=dag,
)

download_population >> cleanup_files >> load_population_bronze >> load_to_snowflake >> process_population_gold >> load_processed_to_snowflake
