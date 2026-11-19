# From learning lab to an operated data platform

## Implemented boundaries

* Frozen source evidence and checksummed completed manifests.
* Source-row identifiers, explicit date/currency/negative-value rules, quarantine accounting.
* Compatible single-writer Spark/Iceberg tables and independent Trino reads.
* Snapshot IDs attached to a coherent release manifest.
* Trino validates row identity, accounting, customer relationships and group-by-group decimal totals on pinned Iceberg snapshots.
* Published BI datasets share a single Iceberg fact snapshot through Trino views.
* The read-only Trino BI identity passes positive published-view and negative direct-table access checks.
* A modified decimal total is rejected by the actual group comparison in the notebook experiment.
* Ordered-state/replay/delete conflict exercises, plus an optional real CDC integration.
* Retained Iceberg releases can be revalidated by snapshot ID while the associated metadata and objects are present.
* OpenMetadata discovery of live lab assets, modeled lineage, stewardship tags and actual publication quality results.

## Explicit limitations and concrete next steps

| Boundary | Local design | Production change / acceptance evidence |
|---|---|---|
| Identity | Lab admin bootstraps metadata, and a separate Trino identity reads published views | Authenticate and isolate writer, catalog and consumer identities |
| Network/TLS | Host ports bind to loopback; internal Docker HTTP | TLS, authenticated engines, restricted service network and reviewed remote access |
| Trino/S3 authorization | Trino limits the local BI identity to the published views on a trusted network | Add authenticated user mapping and align query and object-store permissions |
| Storage durability | One laptop and persistent Docker volumes | Durable storage with independent backups, deletion/retention policy and tested failure domains |
| Lakehouse recovery | Retained objects and catalog on working host | Back up catalog plus reachable object versions and configs; restore into an independent environment and verify pinned reads |
| Publication | Validated immutable S3 manifest, a Trino snapshot pointer and a local current-release file | Recover or retry if publication stops between object, view and manifest updates |
| CDC order | One partition, source LSN + offset tie-break | Explicit per-entity ordering, schema evolution and cross-table transaction policy under realistic fan-out |
| Orchestration | One task delegates a bounded vertical slice | Separate stages where it improves recovery/observability; externalize durable run state and backfill intervals |
| Metadata and monitoring | One OpenMetadata workspace, local catalogs and Prometheus alert evaluation | Use dedicated catalog identities, back up the catalog, define freshness and ownership objectives, and route incidents to the operations team |
| Maintenance | Inspect snapshots/files and preserve known release versions | Compaction/expiry policy with reader/writer coordination, snapshots retention and orphan safeguards |
| Customer history | Synthetic SCD2 changes | Obtain a trustworthy source history and temporal business contract |
| AI features/RAG | Small boundary demonstrations | Real point-in-time joins, deployment-like evaluation, tenant-isolated indexes/caches and backend authorization |
| Performance | One-machine public-data measurements | Representative concurrency, growth scenarios, p95 freshness/query SLOs and total operating cost |

The archive excludes secrets and database/source data. This makes it reproducible and reviewable, not a full disaster backup of the user's computer.
