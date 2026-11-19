# From learning lab to an operated data platform

## Implemented boundaries

* Frozen source evidence and checksummed completed manifests.
* Source-row identifiers, explicit date/currency/negative-value rules, quarantine accounting.
* Compatible single-writer Spark/Iceberg tables and independent Trino reads.
* Snapshot IDs attached to a coherent release manifest.
* Independently built SQL/dbt warehouse with isolated candidate schemas.
* Group-by-group decimal reconciliation before consumer publication.
* Transactional publication of related PostgreSQL views and audit entry.
* Restricted warehouse BI role, with negative permission tests.
* Actual candidate corruption test and last-valid release preservation.
* Ordered-state/replay/delete conflict exercises, plus an optional real CDC integration.
* A logical warehouse backup restored to a fresh database, when its evidence file exists.
* OpenMetadata discovery of live lab assets, modeled lineage, stewardship tags and actual publication quality results.

## Explicit limitations and concrete next steps

| Boundary | Local design | Production change / acceptance evidence |
|---|---|---|
| Identity | Lab admin bootstraps writers; separate warehouse reader | Separate loader, transformer, catalog writer, consumer and admin identities; deny tests on each path |
| Network/TLS | Host ports bind to loopback; internal Docker HTTP | TLS, authenticated engines, restricted service network and reviewed remote access |
| Trino/S3 authorization | Trusted lab network; broad lab writer | Align catalog/query/object permissions; test raw paths, spill, caches and metadata tables |
| Storage durability | One laptop and persistent Docker volumes | Durable storage with independent backups, deletion/retention policy and tested failure domains |
| Lakehouse recovery | Retained objects and catalog on working host | Back up catalog plus reachable object versions and configs; restore into an independent environment and verify pinned reads |
| Multi-system publication | Manifest defines snapshot set; warehouse views commit locally | Define consumer release barrier and recovery for the PG-commit/manifest-write failure window; no global transaction is claimed |
| CDC order | One partition, source LSN + offset tie-break | Explicit per-entity ordering, schema evolution and cross-table transaction policy under realistic fan-out |
| Orchestration | One task delegates a bounded vertical slice | Separate stages where it improves recovery/observability; externalize durable run state and backfill intervals |
| Metadata and monitoring | One OpenMetadata workspace, local catalogs and Prometheus alert evaluation | Use dedicated catalog identities, back up the catalog, define freshness and ownership objectives, and route incidents to the operations team |
| Maintenance | Inspect snapshots/files and preserve known release versions | Compaction/expiry policy with reader/writer coordination, snapshots retention and orphan safeguards |
| Customer history | Synthetic SCD2 changes | Obtain a trustworthy source history and temporal business contract |
| AI features/RAG | Small boundary demonstrations | Real point-in-time joins, deployment-like evaluation, tenant-isolated indexes/caches and backend authorization |
| Performance | One-machine public-data measurements | Representative concurrency, growth scenarios, p95 freshness/query SLOs and total operating cost |

The archive excludes secrets and database/source data. This makes it reproducible and reviewable, not a full disaster backup of the user's computer.
