import os
import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from db_utils import save_flights_to_postgres


logger = LoggingMixin().log


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


def get_access_token():
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("OPEN_SKY_CLIENT_ID"),
        "client_secret": os.getenv("OPEN_SKY_CLIENT_SECRET")
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        logger.error("No access token received from OpenSky")
        raise ValueError("No access token received")
    logger.info("Access token successfully obtained")
    return token


def fetch_and_load():
    try:
        token = get_access_token()
        url = os.getenv("OPEN_SKY_URL")
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

        if data and "states" in data and data["states"]:
            count = len(data["states"])
            logger.info(f"{count} records received from OpenSky")
            save_flights_to_postgres(conn_id="postgres_default", states=data["states"])
        else:
            logger.info("No flight data received from OpenSky")

    except Exception as e:
        logger.error(f"Error in fetch_and_load: {e}")
        raise


with DAG(
    dag_id="open_sky_to_postgres_airflow",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",  
    catchup=False,
    tags=["opensky", "postgres"]
) as dag:

    load_task = PythonOperator(
        task_id="fetch_and_load",
        python_callable=fetch_and_load
    )