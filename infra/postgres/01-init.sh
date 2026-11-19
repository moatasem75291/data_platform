#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set=reader_password="$READER_PASSWORD" <<'SQL'
CREATE DATABASE iceberg_catalog;
CREATE DATABASE airflow;
CREATE DATABASE source_demo;
CREATE ROLE bi_reader LOGIN PASSWORD :'reader_password';
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
GRANT CONNECT ON DATABASE warehouse TO bi_reader;
GRANT USAGE ON SCHEMA mart TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO bi_reader;
ALTER SYSTEM SET wal_level = 'logical';
SQL
