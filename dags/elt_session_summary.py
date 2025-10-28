from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeCheckOperator

# Change this if your Airflow connection ID is different
SNOWFLAKE_CONN_ID = "snowflake_conn"

DUPLICATE_CHECK_SQL = """
USE WAREHOUSE GORILLA_WH_XSMALL;
USE DATABASE USER_DB_GORILLA;
USE SCHEMA RAW;

-- Fail if any session_id appears more than once in mapping (bonus data-quality check)
SELECT COUNT(*) = 0
FROM (
  SELECT SESSION_ID
  FROM USER_SESSION_CHANNEL
  GROUP BY SESSION_ID
  HAVING COUNT(*) > 1
);
"""

BUILD_SUMMARY_SQL = """
USE WAREHOUSE GORILLA_WH_XSMALL;
USE DATABASE USER_DB_GORILLA;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- Build analytics.session_summary (joined, deduped)
CREATE OR REPLACE TABLE SESSION_SUMMARY AS
WITH events AS (
  SELECT
    SESSION_ID,
    MIN(EVENT_TS) AS SESSION_START_TS,
    MAX(EVENT_TS) AS SESSION_END_TS
  FROM USER_DB_GORILLA.RAW.SESSION_TIMESTAMP
  GROUP BY SESSION_ID
),
deduped_channel AS (
  SELECT
    SESSION_ID,
    ANY_VALUE(USER_ID)  AS USER_ID,
    ANY_VALUE(CHANNEL)  AS CHANNEL
  FROM USER_DB_GORILLA.RAW.USER_SESSION_CHANNEL
  GROUP BY SESSION_ID
)
SELECT
  d.USER_ID,
  d.SESSION_ID,
  d.CHANNEL,
  e.SESSION_START_TS,
  e.SESSION_END_TS,
  DATEDIFF('second', e.SESSION_START_TS, e.SESSION_END_TS) AS SESSION_LENGTH_SECONDS
FROM deduped_channel d
JOIN events e USING (SESSION_ID);
"""

with DAG(
    dag_id="elt_session_summary",
    start_date=datetime(2025, 10, 1),
    schedule=None,          # manual trigger
    catchup=False,
    tags=["week8", "elt", "snowflake"],
    doc_md="""
### ELT: analytics table
Checks for duplicate session mappings, then creates ANALYTICS.SESSION_SUMMARY by joining RAW tables.
""",
) as dag:

    duplicate_check = SnowflakeCheckOperator(
        task_id="duplicate_check",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=DUPLICATE_CHECK_SQL,
    )

    build_session_summary = SnowflakeOperator(
        task_id="build_session_summary",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=BUILD_SUMMARY_SQL,
        split_statements=True,
    )

    duplicate_check >> build_session_summary
