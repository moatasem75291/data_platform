#!/bin/bash
set -euo pipefail
cp /opt/requirements.lock.txt artifacts/requirements.lock.txt
python -m lab.service &
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --ServerApp.root_dir=/opt/lab
