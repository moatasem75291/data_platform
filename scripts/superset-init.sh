#!/bin/bash
set -euo pipefail
superset db upgrade
superset fab create-admin --username admin --firstname Lab --lastname Admin --email lab@example.invalid --password "$SUPERSET_ADMIN_PASSWORD" || true
superset init
exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 'superset.app:create_app()'
