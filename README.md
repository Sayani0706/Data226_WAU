# Data226 – WAU Pipeline (Airflow → Snowflake → Preset)

## DAGs
- `etl_raw_sessions.py` – creates & loads RAW tables (`USER_SESSION_CHANNEL`, `SESSION_TIMESTAMP`)
- `elt_session_summary.py` – duplicate check + builds `ANALYTICS.SESSION_SUMMARY`

## Run locally
1. `docker compose up -d airflow-init && docker compose up -d`
2. Airflow UI: http://localhost:8081 → create connection `snowflake_conn`
3. Trigger `etl_raw_sessions`, then `elt_session_summary`

## Preset
- Connect Snowflake (`sfedu02-lvb17920.us-east-1.aws`)
- Dataset: `ANALYTICS.SESSION_SUMMARY`
- Chart: Bar chart, Time grain **Week**, metric **COUNT(DISTINCT user_id)** renamed **WAU**

## Screenshots
1) ETL DAG, 2) ELT DAG, 3) Dataset, 4) WAU chart
