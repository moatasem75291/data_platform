# Troubleshooting by failing boundary

| Symptom | Check | Fix / expected evidence |
|---|---|---|
| `docker-credential-desktop` not found | Docker CLI was invoked by absolute path before a fresh terminal inherited PATH | Use `scripts/lab.cmd`, which adds the installed Docker resources directory to this process's PATH |
| Image pull returns a temporary registry authentication error | Inspect the failing public image and the pull log; do not change the user's saved registry credentials | Retry that public image individually; cached layers are reused. An actual private-image sign-in requires the user's account choice |
| Notebook image stalls on a newly resolved dbt parser build | Inspect the dependency and metadata preparation log | The lab pins dbt Core 1.11.13 and dbt-postgres 1.11.0, whose dependency metadata was checked explicitly; record the installed lock after successful resolution |
| SeaweedFS says `No writable volumes ... collection:source-evidence` | Each S3 bucket has its own storage collection; an initial tiny volume count can be exhausted by allocated collections | The lab uses 32 slots and a 1 GiB volume size limit. Recreate only the SeaweedFS container while retaining its named volume |
| S3 upload checksum/streaming incompatibility | Check the client configuration and server log at the same operation | The boto3 client calculates optional checksums only when required; source uploads below 256 MiB use a single PUT. End-to-end hashes/counts remain in the manifest |
| Bronze failed after conversion but before final manifest | Look for `candidate-manifest.json`; verify every listed local part's hash | Retry ingestion; it reuses a verified candidate and uploads missing parts. Only the final manifest is a completed input |
| Trino cannot find Iceberg tables | Compare JDBC database, catalog name `lake`, schema version V1, warehouse root and storage credentials | Run the initial Spark smoke test to create metadata tables; then query the same table through Trino |
| Spark class not found / Scala errors | Inspect `artifacts/runtime-lock.json` and JAR checksums | Use the runtime built for Spark 3.5 + Scala 2.12; avoid mixing arbitrary Spark/Iceberg JAR versions |
| dbt build/test fails | Open `artifacts/dbt-build.log`; inspect the failing candidate schema and model | Fix the contract/SQL and rerun. Stable published warehouse views should remain on the last successful build |
| Daily totals differ across engines | Compare each date/currency/country key; inspect rejection reasons, decimals, timestamps and cancellation policy | Resolve the semantic mismatch before publication; do not weaken the equality check |
| CDC has no rows on first attempt | Check connector status, initial snapshot completion and Kafka topic existence | Land the baseline snapshot first, then mutate the synthetic source; reruns can reuse retained bronze events |
| CDC old updates resurrect deletes | Inspect retained tombstones and source ordering | Apply the latest source version per key and retain delete state; Kafka offsets alone are not a universal cross-source order |
| Idle-source CDC retains WAL while warehouse writes grow | Inspect `pg_replication_slots` and retained WAL bytes | A published `lab_heartbeat` table and Debezium heartbeat action advance source offsets even when customer records are idle; monitor confirmed/restart LSN and WAL retention |
| Dashboard uses old data | Check `data/releases/current.json`, the Superset filters and the query cache | Run `lab.cmd full` to publish a complete release, then `lab.cmd bi-refresh` to recheck the chart queries |
| An expected asset is missing from OpenMetadata | Check `artifacts/catalog-state.json`, especially `missing_optional_sources` | Bring up the missing profile, confirm its services respond, then run `lab.cmd metadata-sync` |
| OpenMetadata lacks the current quality result | Compare the release IDs in `artifacts/catalog-state.json` and `data/releases/current.json` | Run `lab.cmd metadata-sync` and inspect the `daily_sales` quality tab |
| Notebook kernel dies | Inspect `docker stats`, container logs and active optional profiles | Stop unused profiles, keep Spark at four threads, and start with the 50,000-row debug slice |

Inspect only the relevant log tail. Avoid printing `.env`, generated catalog/S3 credential files, connector configurations or database URLs containing passwords into shared notebook outputs.
