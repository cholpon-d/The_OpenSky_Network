from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

logger = LoggingMixin().log

default_args = {
    "owner": "airflow",
    "retries":1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="spark_to_clickhouse",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval="15 * * * *",
    catchup=False,
    tags=["spark", "clickhouse"]
) as dag:
    logger.info("Initialized DAG spark_to_clickhouse")

    wait_for_create_clickhouse = ExternalTaskSensor(
        task_id="wait_for_clickhouse_tables",
        external_dag_id="create_clickhouse_tables",
        execution_delta=timedelta(minutes=5),
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=3600
    )


    spark_task = BashOperator(
    task_id="run_spark_job",
    bash_command="""
        set -e
        echo "=== [$(date)] Running a Spark job ==="
        if docker exec spark-master /usr/local/spark-3.4.1-bin-hadoop3/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/scripts/spark_to_clickhouse.py; then
            echo "=== [$(date)] Spark job completed successfully ==="
        else
            echo "=== [$(date)] Spark job FAILED ==="
            exit 1
        fi
    """
)

    wait_for_create_clickhouse >> spark_task

    logger.info("DAG spark_to_clickhouse is configured")