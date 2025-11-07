# CSE5114_Project

A data engineering project using Apache Airflow and Apache Spark with Spark History Server for persistent monitoring.

## 🏗️ Architecture

This project integrates:

- **Apache Airflow** - Workflow orchestration
- **Apache Spark Standalone Cluster** - Distributed data processing
  - 1 Master node
  - 1 Worker node (2 cores, 2GB RAM)
- **Spark History Server** - Persistent Spark UI for completed jobs
- **PostgreSQL** - Airflow metadata database

## 📁 Project Structure

```
CSE5114_Project/
├── dags/                      # Airflow DAG definitions
│   └── hello_world_dag.py     # Example DAG with PySpark
├── jobs/                      # PySpark job scripts
│   └── hello_world_spark.py   # Example PySpark job
├── data/                      # Data files and outputs
├── spark-events/              # Spark event logs (for History Server)
├── logs/                      # Airflow logs
├── plugins/                   # Airflow plugins
├── scripts/                   # Utility scripts
├── docker-compose.yml         # Container orchestration
├── Dockerfile                 # Custom Airflow image with Spark
└── spark-defaults.conf        # Spark configuration
```

## 🚀 Getting Started

### Prerequisites

- Docker
- Docker Compose

### Setup

1. **Clone the repository:**

   ```bash
   git clone git@github.com:Derrick-Tseng/CSE5114_Project.git
   cd CSE5114_Project
   ```

2. **Build and start the containers:**

   ```bash
   docker-compose up -d
   ```

3. **Access the web interfaces:**

   - **Airflow UI**: http://localhost:8000
     - Username: `airflow`
     - Password: `airflow`
   - **Spark Master UI**: http://localhost:8080 (cluster overview & running jobs)
   - **Spark Worker UI**: http://localhost:8081 (worker details)
   - **Spark History Server**: http://localhost:18080 (completed jobs)

4. **Stop the containers:**
   ```bash
   docker-compose down
   ```

## 📊 Monitoring Spark Jobs

### Spark Master UI (Port 8080)

- Shows **Spark cluster status** (master + workers)
- Shows **currently running** Spark applications
- Real-time metrics for active jobs
- Worker registration and resource allocation

### Spark Worker UI (Port 8081)

- Shows **individual worker status**
- Running executors and tasks
- Resource usage (CPU, memory)
- Logs for executors running on this worker

### Spark History Server (Port 18080)

- Shows **all completed** Spark applications
- Full execution DAG and metrics
- Stages, tasks, and SQL queries
- Persists after jobs complete - no need to pause tasks!

### How Event Logging Works

All Spark jobs automatically log events to the `spark-events/` directory:

```
Spark Job → Writes events → /spark-events/ → History Server reads → UI at :18080
```

Configuration in `spark-defaults.conf`:

```properties
spark.eventLog.enabled=true
spark.eventLog.dir=file:///opt/spark/spark-events
```

## 🔧 Creating PySpark Jobs with Airflow

### Directory Structure

- **PySpark Scripts** → Put in `/jobs/` directory
- **Airflow DAGs** → Put in `/dags/` directory

### Example: Hello World

1. **PySpark Job** (`jobs/hello_world_spark.py`):

   - Contains your Spark application logic
   - Accepts command-line arguments
   - Automatically logs to History Server

2. **Airflow DAG** (`dags/hello_world_dag.py`):
   - Uses `SparkSubmitOperator` to trigger PySpark jobs
   - Defines workflow and dependencies
   - Passes arguments to Spark jobs

### Running the Example

1. Ensure containers are running
2. Access Airflow UI at http://localhost:8000
3. Find `hello_world_spark` DAG
4. Toggle it ON (unpause)
5. Click the play button to trigger manually
6. Monitor in Airflow UI and Spark UIs (ports 8080, 8081, 18080)

### Creating Your Own Job

1. **Create PySpark script** in `jobs/`:

   ```python
   # jobs/my_job.py
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName("MyJob").getOrCreate()
   # Your Spark code here
   ```

2. **Create Airflow DAG** in `dags/`:

   ```python
   # dags/my_dag.py
   from airflow import DAG
   from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

   with DAG('my_dag', ...) as dag:
       spark_task = SparkSubmitOperator(
           task_id='my_spark_job',
           application='/opt/airflow/jobs/my_job.py',
           master='spark://spark-master:7077',
       )
   ```

3. Files are automatically picked up (may take a few seconds)

## 🔍 Troubleshooting

- **DAG not appearing?** Check Airflow logs in `logs/` directory
- **Spark job failing?** Check Spark UI at port 8080 or 18080
- **Can't access UI?** Verify containers are running: `docker-compose ps`
- **Event logs not showing?** Check `spark-events/` directory has write permissions

## 📦 Services & Ports

| Service                 | Port  | Purpose                       |
| ----------------------- | ----- | ----------------------------- |
| Airflow Webserver       | 8000  | Workflow management UI        |
| Spark Master            | 8080  | Cluster status & running jobs |
| Spark Worker            | 8081  | Worker details & executors    |
| Spark History Server    | 18080 | Completed Spark jobs UI       |
| Spark Master (internal) | 7077  | Spark cluster endpoint        |

## 🎯 Spark Cluster Configuration

- **Master**: Coordinates job execution and resource allocation
- **Worker**:
  - Cores: 2
  - Memory: 2GB
  - Runs executors for your Spark jobs
- **Event Logging**: Enabled by default for all jobs
- **History Server**: Persists job details indefinitely
