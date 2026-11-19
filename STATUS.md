# Lakehouse verification status

Verified on the connected Windows computer on November 19, 2026.

## Observed results

| Check | Result |
|---|---|
| Full input | 1,067,371 workbook rows in 43 checksummed bronze parts. Iceberg accepted 1,067,366 rows and quarantined five |
| Iceberg validation | All five checks passed. Trino verified row accounting, source key uniqueness, the exact fact total, customer relationships and all 3,237 date/currency/country daily groups |
| Publication | The two Trino BI views share one validated Iceberg fact snapshot. The BI identity can read both views and is denied access to the candidate fact, silver transactions and quarantine |
| Airflow | A scheduler-executed run built, validated and published the full workbook |
| Debug isolation | The 50,000-row run validated its own Iceberg tables and left the published release ID unchanged |
| Superset | All 23 managed chart queries and all three original dashboard chart queries returned data through Trino. The managed overview showed the full accepted-row KPI and both filters |
| OpenMetadata | Catalog sync registered 11 services, 12 tables, 7 dashboards, 4 pipelines, 2 Kafka topics, 18 lineage edges and 3 quality results. Search and upstream lineage were verified for both daily sales assets |
| Grafana and Prometheus | All 27 panels in three dashboards returned live data. Prometheus loaded nine alert rules and reported 12 healthy dependencies |
| CDC | PostgreSQL → Debezium → Kafka → bronze → Iceberg. Replay and retained deletes passed. The connector and heartbeat were healthy, with zero unconsumed CDC messages at verification time |
| Notebook | All 22 code cells passed with BI and CDC enabled. Schema evolution, time travel, an intentional validation failure and reads of a retained release passed. The HTML export was regenerated |
| Runtime | Notebook and Superset images rebuilt, `pip check` passed, and all three unit tests passed |

The signed sales total was GBP 19,445,926.708000. It includes cancellations and retains overlapping source observations. The notebook profiles the overlapping dates and possible duplicates.

The retired PostgreSQL analytics databases have connections disabled. Retired files and backups are preserved in `data/retired-warehouse/`. Active PostgreSQL databases support Iceberg metadata, Airflow and synthetic CDC. OpenMetadata keeps its own catalog database.

## Open and resume

```powershell
cd F:\webdev\research_and_development
.\scripts\lab.cmd platform
.\scripts\lab.cmd open
```

The `.cmd` launcher works with the existing PowerShell policy. `platform` starts the full stack and registers assets in OpenMetadata. The [platform guide](docs/openmetadata.md) lists dashboard links and verification commands.

The hands-on lesson is in `data_platform_architecture_lab.ipynb` and its HTML export. Lock files record installed Python dependencies, Java library checksums and service image digests. Observed run results are in `artifacts/`.
