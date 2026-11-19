# Installation and verification status

Completed on the connected Windows computer. Main project: `F:\webdev\research_and_development`.

## Observed results

| Check | Result |
|---|---|
| Source ingestion | 1,067,371 workbook rows in 43 checksummed bronze parts |
| Accepted / quarantined | 1,067,366 accepted; 5 quarantined under the written contract |
| Spark write / Trino read | Passed against one PostgreSQL-backed Iceberg JDBC catalog |
| Warehouse build | 7 dbt models + 8 data tests, all 15 checks passed |
| Cross-engine reconciliation | All 3,237 date/currency/country groups match exactly, including decimal amounts |
| Fresh full notebook execution | All 22 code cells passed; 47 total cells; BI and real CDC enabled |
| Schema / table operations | Schema evolution, MERGE, time travel and replay/delete checks passed |
| Publication failure | A deliberately corrupted candidate was blocked; last valid release retained |
| Warehouse BI permissions | Mart reads succeed; staging and core reads denied |
| Superset | Three full-workbook dashboards and 23 editable charts. All chart queries returned data. Accepted-line and signed sales KPIs match the published warehouse |
| Real CDC | PostgreSQL → Debezium → Kafka → bronze → Iceberg; retained delete and replay passed |
| Airflow | The DAG test and a scheduler-executed manual run both published and validated the full workbook |
| Debug isolation | A 50,000-row sample reconciled in separate namespaces. The published release ID and 1,067,366-row mart stayed current |
| OpenMetadata | 12 services, 18 tables, 7 dashboards, 5 pipelines, 2 Kafka topics, 25 lineage edges and 3 native quality results. Search and lineage verified for both `daily_sales` assets |
| Monitoring | All 27 panels in three Grafana dashboards returned live data through the datasource. Prometheus loaded nine alert rules and observed 12 responding dependencies |
| Source protection | The Debezium connector and source heartbeat resumed. The slot advanced and the Kafka CDC consumer backlog reached zero |
| Warehouse recovery | In the recorded restore drill the backup restored into a separate database in 122 seconds. Count and decimal total matched |
| Dependency rebuild | Frozen image rebuilt successfully; `pip check` reports no broken requirements |
| Notebook QA | Valid nbformat, 22 executed code cells, zero error outputs; restore evidence refreshed by executing its cell; HTML export created |

Restored fact evidence: `1067366|19445926.708000`. The monetary total is a signed source-observation measure in GBP, not audited or deduplicated accounting revenue. The notebook explicitly profiles the overlapping source-sheet dates and duplicate candidates.

## Open and resume

```powershell
cd F:\webdev\research_and_development
.\scripts\lab.cmd platform
.\scripts\lab.cmd open
```

The `.cmd` launcher works with the existing PowerShell policy. `platform` starts the full stack and registers assets in OpenMetadata. See [the platform guide](docs/openmetadata.md) for dashboard links and verification commands.

Main files: `data_platform_architecture_lab.ipynb`, its readable HTML export, `README.md`, and `docs/pdf-coverage.md`. Installed packages, JAR checksums and service image digests are recorded in the lock files and `artifacts/` evidence.

This is a tested single-machine learning deployment. Cloud service mappings, enterprise identity/TLS, complete object-store disaster recovery and production RAG deployment are documented boundaries, not claimed installations. The shareable package excludes credentials, source data, database dumps and container volumes.
