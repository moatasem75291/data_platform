# Retail platform catalog and dashboards

Run `.\scripts\lab.cmd platform` after `scripts/configure.py` on a fresh checkout. The launcher starts Docker profiles, publishes the whole workbook if needed, creates the editable Superset charts, initializes the synthetic CDC exercise if needed, and refreshes OpenMetadata. Use `python scripts/manage.py platform` from other shells. OpenMetadata runs with a dedicated catalog database and Elasticsearch on the Compose network. The UI is available at [localhost:8585](http://localhost:8585). Use `admin@open-metadata.org` and the local password stored in `data/openmetadata/admin-password`.

In OpenMetadata, search for **Retail Sales** to find the governed data product. Open either `daily_sales` to inspect its lineage or open the warehouse asset's **Data Quality** tab to see the actual reconciliation result. The **Retail Analytics** domain groups the related services. The **Retail Data Stewards** team owns the managed assets, and `RetailAccess` labels identify published products, internal assets and pseudonymous customer identifiers. PostgreSQL enforces the existing `bi_reader` permission boundary. The catalog labels document intended use across engines.

| Tool | What the catalog collects |
|---|---|
| SeaweedFS | Verified workbook object, full bronze manifest, CDC bronze prefix and release manifests |
| PostgreSQL | Live column metadata for the stable raw, core and mart relations and the synthetic CDC source |
| Spark, Iceberg and Trino | Iceberg table and column metadata from Trino, Spark run status and the lab's declared transformations |
| dbt | Model dependencies from the archived manifest of the published release and the observed test result |
| Airflow and JupyterLab | The lab DAG and its recent run states from the Airflow database, plus the notebook link |
| Kafka, Kafka UI and Debezium | Live CDC topics and partitions, connector status, and lineage from source to Kafka, bronze and Iceberg |
| Superset | The provisioned dashboards and their charts via the Superset API, plus lineage from the published datasets |
| Prometheus and Grafana | Live rule definitions, dashboards, panels and links to the tools that serve metrics history |

The sync reads source metadata and updates OpenMetadata with its REST API. The catalog shows historical pipeline and quality results from the lab. It links out for BI exploration, operations, and live time series. Database credentials and customer rows stay with the original services. Registered assets describe the current lab, and the release manifest records versioned Iceberg reads. The local worker refreshes the catalog after full attempts. Use `.\scripts\lab.cmd metadata-sync` to refresh after changing dashboards or restarting a source. The full-stack launcher requires every source. The standalone refresh reports missing optional profiles.

The dashboards use the full reconciled workbook. Country and invoice date filters allow you to narrow the historical range inside each Superset dashboard.

| Dashboard | What you can explore |
|---|---|
| [Superset · Overview and Markets](http://localhost:18088/superset/dashboard/retail-overview/) | Gross, signed net and credits, monthly trends, invoice-line volume and market comparisons |
| [Superset · Products and Customers](http://localhost:18088/superset/dashboard/retail-products-customers/) | Purchase invoices, known customers, average purchase value, product leaders and purchasing frequency |
| [Superset · Credits and Data Coverage](http://localhost:18088/superset/dashboard/retail-returns-coverage/) | Negative amounts, cancellation flags, customer ID coverage, credit share by market and workbook sheet ranges |
| [Grafana · Releases and Quality](http://localhost:13000/d/learning-pipeline) | Full-workbook counts, dbt and reconciliation evidence, run status, processing time by stage and quarantine rate |
| [Grafana · Services and Catalog](http://localhost:13000/d/learning-services) | Dependency health, probe latency, catalog freshness and firing alerts |
| [Grafana · CDC and WAL](http://localhost:13000/d/learning-cdc) | Debezium task and replication slot status, heartbeat age, Kafka backlog and retained WAL |

The full pipeline is the default in Airflow, the notebook and `python -m lab.cli run`. The `sample` command still reconciles the first 50,000 source observations in `staging_sample`, `lake.sample_silver` and `lake.sample_gold`. A sample writes `artifacts/sample-verification.json`. It cannot publish to the warehouse marts or the current release manifest.

For a live check, run `docker compose exec -T notebook python scripts/verify-monitoring.py` and `docker compose exec -T notebook python scripts/verify-catalog.py`. Check `artifacts/verification.json`, `artifacts/bi-verification.json`, `artifacts/monitoring-verification.json` and `artifacts/openmetadata-verification.json` for the observed run and coverage. Prometheus evaluates the [local alerts](http://localhost:19090/alerts). The lab has no external alert destinations.

The integration uses the [OpenMetadata lineage](https://docs.open-metadata.org/v2.0.x/api-reference/lineage) and [data product](https://docs.open-metadata.org/v2.0.x/api-reference/governance/data-products) APIs. Grafana loads the dashboards through its [file provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) configuration.
