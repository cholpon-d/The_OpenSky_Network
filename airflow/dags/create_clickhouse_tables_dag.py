from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta 
from clickhouse_driver import Client

default_args = {
    "owner": "airflow",
    "retry_delay": timedelta(minutes=2),
    "retries": 1
}

def create_clickhouse_tables():
    import logging
    logger = logging.getLogger(__name__)

    client = Client(
        host="clickhouse-server",
        user="airflow",
        password="airflow",
        database="airflow"
    )

    tables = {
        "enriched_flights": """
            CREATE TABLE IF NOT EXISTS airflow.enriched_flights(
                icao24 String,
                callsign String,
                origin_country String,
                event_time DateTime,
                event_date Date,
                velocity_kmh Nullable(Decimal(10, 2)),
                baro_altitude Nullable(Decimal(10, 2)),
                geo_altitude Nullable(Decimal(10, 2)),
                rank_in_country Nullable(UInt32),
                speed_rank_global Nullable(UInt32),
                avg_speed_in_country Nullable(Decimal(10, 2)),
                max_speed_in_country Nullable(Decimal(10, 2)),
                min_speed_in_country Nullable(Decimal(10, 2)),
                is_anomaly Nullable(UInt8),
                time_of_day String,
                flight_status String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, origin_country)
            SETTINGS index_granularity = 8192;
        """,
        "country_stats": """
            CREATE TABLE IF NOT EXISTS airflow.country_stats(
                origin_country String,
                total_aircraft Nullable(UInt32),
                avg_speed_kmh Nullable(Decimal(10, 2)),
                unique_flights Nullable(UInt32),
                anomaly_count Nullable(UInt32),
                global_rank Nullable(UInt32),
                percentage_of_total Nullable(Decimal(5, 2))
            )
            ENGINE = MergeTree()
            ORDER BY (origin_country)
            SETTINGS index_granularity = 8192;
        """,
        "flights_analytics": """
            CREATE TABLE IF NOT EXISTS airflow.flights_analytics(
                origin_country String,
                event_date Date,
                time_of_day String,
                flight_status String,
                velocity_category String,
                flight_count Nullable(UInt32),
                avg_velocity_kmh Nullable(Decimal(10, 2)),
                avg_baro_altitude Nullable(Decimal(10, 2)),
                avg_geo_altitude Nullable(Decimal(10, 2)),
                anomaly_count Nullable(UInt32),
                unique_aircraft Nullable(UInt32),
                min_velocity_kmh Nullable(Decimal(10, 2)),
                max_velocity_kmh Nullable(Decimal(10, 2)),
                median_velocity_kmh Nullable(Decimal(10, 2)),
                anomaly_percentage Nullable(Decimal(5, 2))
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, origin_country, flight_status)
            SETTINGS index_granularity = 8192;
        """
    }

    for table, query in tables.items():
        client.execute(query)
        logger.info(f"Table {table} created or already exists.")

    client.disconnect()

with DAG(
    dag_id="create_clickhouse_tables",
    default_args=default_args,
    start_date=datetime(2025,10,1),
    schedule_interval="10 * * * *",
    catchup=False,
    tags=["clickhouse"]
) as dag:
    wait_for_api_dag = ExternalTaskSensor(
        task_id="wait_for_openSky_dag",
        external_dag_id="open_sky_to_postgres_airflow",
        external_task_id="fetch_and_load",
        execution_delta=timedelta(minutes=10),
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        soft_fail=False 
    )

    create_tables = PythonOperator(
        task_id="create_clickhouse_tables_task",
        python_callable=create_clickhouse_tables
    )

    wait_for_api_dag >> create_tables