from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Change this if your Airflow connection ID is different
SNOWFLAKE_CONN_ID = "snowflake_conn"

RAW_SQL = """
-- Context for your environment
USE WAREHOUSE GORILLA_WH_XSMALL;
USE DATABASE USER_DB_GORILLA;
USE SCHEMA RAW;

-- Create tables if they don't exist
CREATE TABLE IF NOT EXISTS USER_SESSION_CHANNEL (
  SESSION_ID STRING,
  USER_ID    STRING,
  CHANNEL    STRING
);

CREATE TABLE IF NOT EXISTS SESSION_TIMESTAMP (
  SESSION_ID STRING,
  EVENT_TS   TIMESTAMP_NTZ
);

-- Seed demo rows for the assignment (remove TRUNCATE/INSERT if using real data)
TRUNCATE TABLE USER_SESSION_CHANNEL;
TRUNCATE TABLE SESSION_TIMESTAMP;

INSERT INTO USER_SESSION_CHANNEL (SESSION_ID, USER_ID, CHANNEL) VALUES
  ('s1','u1','web'),
  ('s2','u1','web'),
  ('s3','u2','ios'),
  ('s4','u3','android');

INSERT INTO SESSION_TIMESTAMP (SESSION_ID, EVENT_TS) VALUES
  ('s1','2025-10-10 08:00:00'),
  ('s1','2025-10-10 08:10:00'),
  ('s2','2025-10-11 09:00:00'),
  ('s3','2025-10-12 10:00:00'),
  ('s4','2025-10-17 12:30:00');
"""

with DAG(
    dag_id="etl_raw_sessions",
    start_date=datetime(2025, 10, 1),
    schedule=None,          # manual trigger
    catchup=False,
    tags=["week8", "etl", "snowflake"],
    doc_md="""
### ETL: raw tables
Creates RAW.USER_SESSION_CHANNEL and RAW.SESSION_TIMESTAMP and loads sample data.
""",
) as dag:
    create_and_load_raw = SnowflakeOperator(
        task_id="create_and_load_raw_tables",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=RAW_SQL,
        split_statements=True,
    )

    create_and_load_raw
