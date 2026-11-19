# Report-to-lab coverage

Page numbers refer to the physical pages in the 42-page research report dated 10 November 2026.

| PDF chapter / pages | Notebook section | Practical evidence / scope |
|---|---|---|
| 1. Concepts, 5–6 | 1 | Explain file, table, catalog and warehouse responsibilities |
| 2. Data flows, 7–8 | 2 | Two-branch architecture diagram; real record lineage |
| 3. Components, 9–11 | 3 | Tool-role mapping, pinned runtime and integration gate |
| 4. Medallion, 12–15 | 4 | Immutable bronze, manifests, typed silver, quarantine, dimensional gold |
| 5. Warehouse design, 16–18 | 5 | PostgreSQL bulk load, dbt models/tests, isolated candidates, transactional views |
| 6. Comparisons, 19–21 | 6 | Comparison matrix and identical-workload output reconciliation |
| 7. Lakehouse implementation, 22–25 | 4 and 7 | Shared catalog read/write, schema evolution, snapshots, merge, replay, CDC profile |
| 8. Warehouse implementation, 26–28 | 5 and 8 | Facts/dimensions, synthetic SCD2 intervals, query-plan inspection |
| 9. Cloud stacks, 29–31 | 9 | AWS/Azure/GCP capability mapping; no paid cloud provisioning |
| 10. Cost/scalability, 32–33 | 10 | Measured query timings and illustrative tariff calculator |
| 11. Operations, 34–36 | 11 | Negative BI permissions, failure gate, OpenMetadata discovery and quality, Airflow, monitoring and warehouse restore |
| 12. AI connections, 37 | 12 | Availability-aware synthetic features and pre-prompt authorization boundary |
| 13. Decisions, 38–39 | 13 | Evidence-based architecture decision template and capstone exercises |
| References, 40–42 | Final notebook reference list | Official sources linked; installed version evidence captured separately |

The lab implements and tests selected operations locally and records their results. Kubernetes, enterprise identity/TLS, managed cloud provisioning, off-host object-store disaster recovery and production RAG deployment are documented design extensions.
