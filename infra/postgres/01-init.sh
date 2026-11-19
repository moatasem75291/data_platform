#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE DATABASE airflow;
CREATE DATABASE source_demo;
ALTER SYSTEM SET wal_level = 'logical';
SQL
