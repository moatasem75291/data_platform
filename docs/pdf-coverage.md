# Report-to-lab coverage

Page numbers refer to the physical pages in the 42-page research report dated 10 November 2026.

| PDF chapter / pages | Notebook section | Practical evidence / scope |
|---|---|---|
| 1. Concepts, 5–6 | 1 | Explain file, table, catalog and publication responsibilities |
| 2. Data flows, 7–8 | 2 | Lakehouse flow and source-to-view lineage |
| 3. Components, 9–11 | 3 | Tool-role mapping, pinned runtime and integration gate |
| 4. Medallion, 12–15 | 4 | Immutable bronze, manifests, typed silver, quarantine, dimensional gold |
| 5. Analytical design, 16–18 | 5 | Iceberg fact validation and Trino release views |
| 6. Comparisons, 19–21 | 6 | Lakehouse architecture and consumption choices |
| 7. Lakehouse implementation, 22–25 | 4 and 7 | Shared catalog read/write, schema evolution, snapshots, merge, replay, CDC profile |
| 8. Modeling implementation, 26–28 | 5 and 8 | Facts and dimensions, synthetic SCD2 intervals, Trino query plans |
| 9. Cloud stacks, 29–31 | 9 | AWS/Azure/GCP capability mapping; no paid cloud provisioning |
| 10. Cost/scalability, 32–33 | 10 | Measured query timings and illustrative tariff calculator |
| 11. Operations, 34–36 | 11 | BI permissions, failure gate, OpenMetadata quality, Airflow, monitoring and retained snapshots |
| 12. AI connections, 37 | 12 | Availability-aware synthetic features and pre-prompt authorization boundary |
| 13. Decisions, 38–39 | 13 | Evidence-based architecture decision template and capstone exercises |
| References, 40–42 | Final notebook reference list | Official sources linked; installed version evidence captured separately |

The lab implements and tests selected operations locally and records their results. Kubernetes, enterprise identity/TLS, managed cloud provisioning, off-host object-store disaster recovery and production RAG deployment are documented design extensions.
