from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime 
import logging

logger = logging.getLogger(__name__)

CREATE_RAW_FLIGHTS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_flights (
        fetch_time TIMESTAMP,
        icao24 VARCHAR(10),
        callsign VARCHAR(20),
        origin_country VARCHAR(50),
        time_position BIGINT,
        last_contact BIGINT,
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        baro_altitude DOUBLE PRECISION,
        on_ground BOOLEAN,
        velocity DOUBLE PRECISION,
        heading DOUBLE PRECISION,
        vertical_rate DOUBLE PRECISION,
        geo_altitude DOUBLE PRECISION,
        squawk VARCHAR(10),
        spi BOOLEAN,
        position_source INT
)
"""

def insert_flight(cur, fetch_time, state):
    field_mapping = [
        ("icao24", 0),
        ("callsign", 1), 
        ("origin_country", 2),
        ("time_position", 3),
        ("last_contact", 4),
        ("longitude", 5),
        ("latitude", 6),
        ("baro_altitude", 7),
        ("on_ground", 8),
        ("velocity", 9),
        ("heading", 10),
        ("vertical_rate", 11),
        ("geo_altitude", 12),
        ("squawk", 13),      
        ("spi", 14),         
        ("position_source", 15)  
    ]
    
    columns = ["fetch_time"]
    values = [fetch_time]
    
    for col_name, state_index in field_mapping:
        columns.append(col_name)
        if state_index < len(state) and state[state_index] is not None:
            value = state[state_index]
            
            if col_name == "spi":
                if isinstance(value, bool):
                    values.append(value)
                elif isinstance(value, (int, float)):
                    values.append(bool(value))
                elif isinstance(value, str):
                    values.append(value.lower() in ['true', '1', 'yes'])
                else:
                    values.append(False)
            elif col_name in ["longitude", "latitude", "baro_altitude", "velocity", "heading", "vertical_rate", "geo_altitude"]:
                try:
                    values.append(float(value) if value is not None else None)
                except (ValueError, TypeError):
                    values.append(None)
            elif col_name in ["time_position", "last_contact", "position_source"]:
                try:
                    values.append(int(value) if value is not None else None)
                except (ValueError, TypeError):
                    values.append(None)
            else:
                values.append(str(value) if value is not None else None)
        else:
            values.append(None)
    
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO raw_flights ({', '.join(columns)}) VALUES ({placeholders})"

    try:
        cur.execute(sql, values)
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        logger.error(f"SQL: {sql}")
        logger.error(f"Values types: {[type(v) for v in values]}")
        raise 

def save_flights_to_postgres(conn_id: str, states: list):
    hook = PostgresHook(postgres_conn_id=conn_id)
    fetch_time = datetime.utcnow()

    try:
        hook.run(CREATE_RAW_FLIGHTS_TABLE)
        logger.info("Table raw_flights is checked/created")

        conn = hook.get_conn()
        cur = conn.cursor()

        if states and len(states) > 0:
            logger.info(f"First state sample: {states[0]}")
            logger.info(f"First state length: {len(states[0])}")

        success_count = 0
        for i, state in enumerate(states):
            try:
                insert_flight(cur, fetch_time, state)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to insert record {i}: {e}")
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Successfully loaded {success_count} out of {len(states)} records in raw_flights")
    except Exception as e:
        logger.error(f"Error with PostgresHook: {e}")
        raise