# Data platform architecture — practical learning lab

Start with **data_platform_architecture_lab.ipynb**. It contains the complete guided lesson, diagrams, commands, expected outputs, implementation inspection, troubleshooting and exercises mapped to all 13 chapters of the report.

The project implements a lakehouse (SeaweedFS S3 + Iceberg + PostgreSQL JDBC catalog + Spark + Trino) and a PostgreSQL/dbt warehouse over the same UCI retail workbook. Both branches must agree on grouped decimal totals before publishing the full dataset. OpenMetadata brings the data, dashboards, pipelines, lineage and quality results into one catalog.

On the prepared Windows computer, `.\scripts\lab.cmd platform` starts the complete stack, creates the retail dashboards and refreshes the catalog. Open [OpenMetadata](http://localhost:8585) and search for **Retail Sales**. Sign in as `admin@open-metadata.org` with the generated password in `data/openmetadata/admin-password`. The [platform guide](docs/openmetadata.md) lists the dashboards and describes what the catalog collects.

## Open on the prepared Windows computer

```powershell
cd F:\webdev\research_and_development
.\scripts\lab.cmd start
.\scripts\lab.cmd open
```

Use the tokenized browser opener instead of copying credentials. Python dependencies and Java are inside the notebook image; no activation of a host venv is required. Docker Desktop uses WSL2. User-managed Python 3.12 and uv are available for configuring the project.

The `.cmd`/Python launcher works under the computer's existing PowerShell script policy. After CDC has been enabled, `start` also resumes Kafka/Connect so an inactive replication slot does not silently retain WAL during warehouse work. The synthetic source includes a published heartbeat table for offset progress when customer data is idle.

## Commands

| Command | Result |
|---|---|
| `.\scripts\lab.cmd platform` | Start all services, ensure a full release, then provision BI and refresh OpenMetadata |
| `.\scripts\lab.cmd sample` | Reconcile the 50,000-row debug slice in private namespaces |
| `.\scripts\lab.cmd full` | Build, reconcile and publish the full workbook |
| `.\scripts\lab.cmd notebook` | Execute the notebook with the full workbook and save a copy |
| `.\scripts\lab.cmd notebook-full` | Same full-workbook notebook run |
| `.\scripts\lab.cmd bi` | Build/start Superset |
| `.\scripts\lab.cmd bi-refresh` | Provision and query-check the three editable retail dashboards |
| `.\scripts\lab.cmd cdc` | Start Kafka, Debezium Connect and the read-only Kafka UI |
| `.\scripts\lab.cmd orchestration` | Start Airflow; DAG is initially paused |
| `.\scripts\lab.cmd monitoring` | Start Prometheus and Grafana |
| `.\scripts\lab.cmd metadata` | Start OpenMetadata after the core lab is running |
| `.\scripts\lab.cmd metadata-sync` | Refresh sources and quality results in OpenMetadata |
| `.\scripts\lab.cmd restore` | Restore into a new database and reconcile |
| `.\scripts\lab.cmd status` | Show service state |
| `.\scripts\lab.cmd stop` | Stop services while retaining data |

For a clean installation elsewhere: install Docker with a Linux-container engine, run `python scripts/configure.py`, then `docker compose up -d --build`. The supplied configuration is a loopback-only educational deployment. See the notebook and `docs/production-boundaries.md` for its precise boundaries.

## Reproducibility and evidence

`artifacts/runtime-lock.json` and `requirements.lock.txt` record the installed Python/Java packages and JAR checksums. `images.lock.json` records resolved container digests. A successful static check is not an integration test. Verification JSON files record actual runs and their input scopes; inspect `STATUS.md` for the final observed installation/test state.

The generated `.env`, S3/Trino config and `data/openmetadata/` contain local credentials. The shareable archive excludes them along with source data, database backups and container volumes. The configurator and platform launcher recreate the required local setup.

## Dataset attribution

Chen, D. (2012). *Online Retail II* [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5CG6D. CC BY 4.0. Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii.

The debug slice follows workbook order and is not a representative sample. Synthetic SCD2/customer/CDC/availability examples are labeled separately. Source duplicates are not silently removed, cancellations are retained, and the signed sales measure is not claimed to be audited revenue.
