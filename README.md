# CSE5114_Project

Data engineering lab using Apache Airflow, Apache Spark, and PostgreSQL. Containers are orchestrated with Docker Compose, and Spark event logs are kept around via the History Server.

## Architecture

- Airflow for scheduling and monitoring
- Spark standalone cluster (one master, one worker)
- PostgreSQL backing the Airflow metadata
- Spark History Server reading logs from `spark-events/`

## Repository Layout

```
CSE5114_Project/
├── dags/            Airflow DAG definitions
├── jobs/            PySpark applications
├── plugins/         Custom Airflow operators
├── data/            Local datasets and outputs
├── spark-events/    Event logs mounted in all Spark containers
├── logs/            Airflow logs
├── docker-compose.yml
├── Dockerfile
└── spark-defaults.conf
```

## Running the stack

Prerequisites: Docker and Docker Compose.

```bash
git clone git@github.com:Derrick-Tseng/CSE5114_Project.git
cd CSE5114_Project
docker-compose up -d
```

Interfaces:

- Airflow: <http://localhost:8000> (user/password `airflow`)
- Spark Master: <http://localhost:8080>
- Spark Worker: <http://localhost:8081>
- Spark History Server: <http://localhost:18080>

Shut down everything with `docker-compose down`.

## Creating jobs

1. Drop PySpark scripts in `jobs/`. Example skeleton:

   ```python
   from pyspark.sql import SparkSession

   if __name__ == "__main__":
       spark = SparkSession.builder.appName("example").getOrCreate()
       # job logic here
       spark.stop()
   ```

2. Define a DAG in `dags/` that uses `SparkSubmitOperator` (imported from `plugins`). Point `application` to `/opt/airflow/jobs/<script>.py`.
3. The Airflow web UI auto-discovers new DAGs; unpause and trigger as needed.

## Monitoring Spark jobs

- Master UI (8080) shows cluster state and running apps.
- Worker UI (8081) shows executors and resource usage.
- History Server (18080) shows completed jobs using logs stored under `spark-events/` (configured via `spark-defaults.conf`).

## Services and ports

| Service              | Port  |
| -------------------- | ----- |
| Airflow Webserver    | 8000  |
| Spark Master UI      | 8080  |
| Spark Worker UI      | 8081  |
| Spark History Server | 18080 |
| Spark Master RPC     | 7077  |

## Notes

- Logs live under `logs/` and `spark-events/` inside the repo, so they persist across container restarts.
- If a DAG or job does not load, check container status with `docker-compose ps` and read the corresponding logs.
