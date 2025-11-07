"""
Hello World Airflow DAG with PySpark
This DAG demonstrates how to trigger a PySpark job from Airflow.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_start():
    """Print a start message."""
    print("=" * 50)
    print("🚀 Starting Hello World Pipeline")
    print("=" * 50)
    return "Pipeline started!"


def print_completion():
    """Print a completion message."""
    print("=" * 50)
    print("✅ Hello World Pipeline Completed Successfully!")
    print("=" * 50)
    return "Pipeline completed!"


# Create the DAG
with DAG(
    dag_id='hello_world_spark',
    default_args=default_args,
    description='A simple Hello World DAG that runs a PySpark job',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['example', 'pyspark', 'hello-world'],
) as dag:
    
    # Task 1: Print start message
    start_task = PythonOperator(
        task_id='print_start',
        python_callable=print_start,
    )
    
    # Task 2: Run the PySpark job using BashOperator with direct spark-submit
    spark_job = BashOperator(
        task_id='run_hello_world_spark',
        bash_command="""
        export PYSPARK_PYTHON=python3
        export PYSPARK_DRIVER_PYTHON=python3
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.pyspark.python=python3 \
            --conf spark.pyspark.driver.python=python3 \
            --conf spark.eventLog.enabled=true \
            --conf spark.eventLog.dir=file:///opt/spark/spark-events \
            --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
            --conf spark.speculation=false \
            --total-executor-cores 2 \
            --executor-cores 2 \
            --executor-memory 2g \
            --name HelloWorldSpark \
            --verbose \
            /opt/airflow/jobs/hello_world_spark.py \
            --output-path /opt/spark/data/hello_world_output \
            --app-name HelloWorldSpark_{{ ds_nodash }}
        """,
    )
    
    # Task 3: Print completion message
    end_task = PythonOperator(
        task_id='print_completion',
        python_callable=print_completion,
    )
    
    # Define task dependencies
    start_task >> spark_job >> end_task
