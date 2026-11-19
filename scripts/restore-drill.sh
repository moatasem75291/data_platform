#!/bin/bash
set -euo pipefail
export PGUSER="$POSTGRES_USER" PGPASSWORD="$POSTGRES_PASSWORD"
target="restore_verify_$(date +%s)"
start=$(date +%s)
pg_dump -Fc --no-owner --no-privileges -d warehouse -f /tmp/learning-warehouse.dump
createdb "$target"
pg_restore --no-owner --no-privileges --exit-on-error -d "$target" /tmp/learning-warehouse.dump
original=$(psql -At -d warehouse -c 'SELECT count(*)||chr(124)||sum(line_amount)::text FROM mart.fact_invoice_line')
restored=$(psql -At -d "$target" -c 'SELECT count(*)||chr(124)||sum(line_amount)::text FROM mart.fact_invoice_line')
test "$original" = "$restored"
elapsed=$(( $(date +%s) - start ))
printf '{"warehouse_restore":"passed","target_database":"%s","count_and_decimal_total":"%s","seconds":%s}\n' "$target" "$restored" "$elapsed"
