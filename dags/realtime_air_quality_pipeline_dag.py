from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark_submit_operator import SparkSubmitOperator
import shutil
import os
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def cleanup_files(**context):
    ti = context['ti']
    path = ti.xcom_pull(task_ids='build_paths', key='base_path')
    if not path:
        print('No base_path found; skipping cleanup')
        return

    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Deleted {path}")
    else:
        print(f"Path {path} does not exist")


def build_realtime_paths(**context):
    ts_nodash = context['ts_nodash']
    base_path = f"/opt/airflow/data/realtime_aqi/{ts_nodash}"
    paths = {
        'base_path': base_path,
        'raw_path': f"{base_path}/raw.json",
        'processed_path': f"{base_path}/processed",
    }
    ti = context['ti']
    for key, value in paths.items():
        ti.xcom_push(key=key, value=value)
    return paths


def fetch_realtime_data(**context):
    ti = context['ti']
    raw_path = ti.xcom_pull(task_ids='build_paths', key='raw_path')

    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    subprocess.run(
        ['python', '/opt/airflow/jobs/realtime_aqi_fetch.py', raw_path],
        check=True,
    )

dag = DAG(
    'realtime_air_quality_pipeline_dag',
    default_args=default_args,
    description='Realtime Air Quality data pipeline',
    schedule_interval='*/20 * * * *',
    catchup=False,
    tags=['realtime', 'air_quality'],
)

build_paths = PythonOperator(
    task_id='build_paths',
    python_callable=build_realtime_paths,
    provide_context=True,
    dag=dag,
)

paths = build_paths.output

data_fetch = PythonOperator(
    task_id='data_fetch',
    python_callable=fetch_realtime_data,
    provide_context=True,
    dag=dag,
)

data_process = SparkSubmitOperator(
    task_id='data_process',
    application='/opt/airflow/jobs/realtime_aqi_process.py',
    application_args=[paths['raw_path'], paths['processed_path']],
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    dag=dag,
)

data_store = SparkSubmitOperator(
    task_id='data_store',
    application='/opt/airflow/jobs/realtime_aqi_store.py',
    application_args=[paths['processed_path']],
    master='spark://spark-master:7077',
    deploy_mode='client',
    driver_memory='2g',
    executor_memory='2g',
    executor_cores=2,
    packages='net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1',
    dag=dag,
)

file_cleanup = PythonOperator(
    task_id='file_cleanup',
    python_callable=cleanup_files,
    provide_context=True,
    dag=dag,
)

build_paths >> data_fetch >> data_process >> data_store >> file_cleanup
