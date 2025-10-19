import os
import requests
from datetime import datetime, timedelta
import time
import json  

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowSkipException

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

        data = None
        for attempt in range(3):
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code == 503:
                logger.warning(f"OpenSky is temporarily unavailable (503). Try {attempt+1}/3...")
                time.sleep(90)
                continue
                
            resp.raise_for_status()
            
            if not resp.text.strip():
                logger.warning(f"OpenSky returned empty response. Try {attempt+1}/3...")
                time.sleep(20)
                continue
                
            try:
                data = resp.json()
                break  
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON response. Try {attempt+1}/3... Error: {e}")
                time.sleep(20)
        else:
            logger.error("All attempts failed. OpenSky API is not responding properly.")
            raise AirflowSkipException("OpenSky API unavailable - no data received")


        if data and "states" in data and data["states"]:
            limited_states = data["states"][:1000]
            save_flights_to_postgres(conn_id="postgres_default", states=limited_states)
            logger.info("Data loading completed successfully")
        else:
            logger.info("No flight data received from OpenSky")
            raise AirflowSkipException("No flight data available")

    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"Error in fetch_and_load: {e}")
        raise AirflowSkipException(f"Skipping due to error: {e}")

with DAG(
    dag_id="open_sky_to_postgres_airflow",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  
    catchup=False,
    tags=["opensky", "postgres"]
) as dag:

    load_task = PythonOperator(
        task_id="fetch_and_load",
        python_callable=fetch_and_load,
        execution_timeout=timedelta(minutes=30)
    )