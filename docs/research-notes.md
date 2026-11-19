# Investigation and implementation decisions

## Chosen dataset

UCI Online Retail II provides two years of real invoice-line observations, a public direct workbook download, approximately 1.07 million rows and a CC BY 4.0 license. It exercises missing values, negative quantities, source identifiers and realistic grouping without needing authentication. The official source documents prices in sterling and cancellation invoice prefixes. Its documented fields do not establish authoritative customer segments, database update history, source timezone metadata or feature-availability times; those are labeled assumptions or separate synthetic fixtures.

NYC taxi data is a useful alternative for partition pruning and large scans but aligns less closely with the report's customer/order dimensional examples. A multi-table commerce dataset could add relationships, but would require separately verifying license and download access. The chosen dataset keeps this project reproducible from one official source.

## Storage and catalog

Observed data profiling found overlapping source-sheet windows: 1 December 2009–9 December 2010 and 1 December 2010–9 December 2011. The notebook audits duplicate-looking observations and makes the source-row grain explicit. Its source-observation totals are not presented as a deduplicated financial ledger. Deduplication needs a source-authoritative identity or an approved business policy.

The report's Ceph RGW is appropriate where a team can operate a storage cluster. This lab substitutes a single-node SeaweedFS S3 service, retains Iceberg table format v2 and uses a PostgreSQL JDBC catalog. SeaweedFS's own table catalog is not used. This avoids two metadata authorities. Files are accessed through committed Iceberg metadata; consumers do not glob arbitrary table data directories.

Both Spark and Trino document JDBC catalog integration. Trino requires the JDBC metadata tables to exist, so the initial Spark creation bootstraps them. Catalog name, database, schema version and S3 location must match. The first installed integration test checks that a Spark-written decimal is readable from Trino.

## Runtime family and locks

Spark 3.5 is a maintained runtime family in Iceberg's compatibility matrix. Java 17 and the Spark-3.5/Scala-2.12 Iceberg runtime are chosen together. Actual package versions, JAR checksums and container digests are recorded after installation. Seed tags are resolved to digests; package dependency resolution is captured as a lock rather than assumed reproducible from broad ranges.

## Modeling and workflow

Spark transforms the completed bronze manifest into Iceberg tables. Source-row identity differs from invoice and invoice/product keys because identical-looking source records can represent separate observations. Missing customers use an unknown member. Trino recomputes the daily groups from pinned fact rows and compares each key and exact decimal value with Spark's gold aggregate.

The release manifest captures the validated snapshot set. The published Trino views both follow the same fact snapshot so readers retain a consistent dataset while Spark builds candidates. The view pointer and the manifest update independently.

## Primary documentation

* https://archive.ics.uci.edu/dataset/502/online+retail+ii
* https://iceberg.apache.org/multi-engine-support/
* https://iceberg.apache.org/docs/latest/jdbc/
* https://spark.apache.org/docs/3.5.8/api/python/getting_started/install.html
* https://trino.io/docs/current/object-storage/metastores.html
* https://github.com/seaweedfs/seaweedfs
* https://trino.io/docs/current/connector/iceberg.html#time-travel-queries
* https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/
* https://debezium.io/documentation/reference/stable/connectors/postgresql.html
* https://superset.apache.org/docs/
* https://docs.docker.com/desktop/setup/install/windows-install/

Research checked 15 November 2026. Capability research is distinct from the actual installation and integration evidence in `artifacts/` and `STATUS.md`.
