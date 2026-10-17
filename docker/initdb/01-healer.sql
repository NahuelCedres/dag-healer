-- Creates the separate database used by the dag-healer incident store
-- (HEALER_DATABASE_URL=postgresql+psycopg2://airflow:airflow@postgres/healer).
-- dag-healer never writes to Airflow's own metadata database.
--
-- The postgres image runs this file only when its data volume is initialised for the
-- first time; airflow-init also creates the database when it is missing (older volumes).
SELECT 'CREATE DATABASE healer OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'healer')\gexec
