"""Generate the teaching notebook using only Python's standard library."""
import json,textwrap
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
cells=[]
def md(text):cells.append({'cell_type':'markdown','metadata':{},'source':textwrap.dedent(text).strip()+'\n'})
def code(text):cells.append({'cell_type':'code','execution_count':None,'metadata':{},'outputs':[],'source':textwrap.dedent(text).strip()+'\n'})

md('''
# From source files to trusted data products
## A practical companion to *Data Platform Architecture Research Report*

**Project:** retail analytics on an Iceberg lakehouse, from workbook to published views.
**Source:** the 42-page report dated 10 November 2026, chapters 1–13.
**Learning goal:** understand each component's responsibility, trace a real record to a report, and prove correctness before choosing a larger stack.

Supporting Python, Spark SQL, Compose and monitoring files are available in JupyterLab. Code cells inspect the transformations and validation directly.

**How to study:** read one section, run its cells in order, inspect the output, then attempt its exercise. Use Kernel → Restart Kernel and Run All to check reproducibility. A successful cell is evidence for that specific check, not a production-readiness certificate.

The real dataset contains historical transaction rows. Operational customer changes, SCD2 segments and availability-time examples below are **synthetic teaching data**. They are never presented as observed retail history.

### Contents
0. Setup and controls · 1. Concepts · 2. Data flows · 3. Tools and contracts · 4. Medallion · 5. Validation and publication · 6. Alternatives · 7. Lakehouse operations and CDC · 8. Customer history and query plans · 9. Cloud mappings · 10. Cost and performance · 11. Operations and recovery · 12. AI connections · 13. Decision record and exercises.
''')
md('''
## 0. Setup, ports and learning controls

The installed environment uses Docker Desktop's WSL2 backend. Python 3.12 and Java 17 run inside the notebook container. Spark executes locally with four threads, and Trino is a separate container. PostgreSQL holds the Iceberg catalog, Airflow metadata and synthetic CDC source.

From **PowerShell**, in `F:\\webdev\\research_and_development`:

```powershell
uv run --python 3.12 --no-project python scripts/configure.py
.\\scripts\\lab.cmd start
.\\scripts\\lab.cmd open
```

`configure.py` generates credentials once and preserves them on later runs. They stay in local `.env` and generated configuration files; do not paste them into notebook outputs or source control. The browser opener reads the Jupyter token privately. No cloud account or GPU is needed.

| Service | Local address | Purpose |
|---|---|---|
| JupyterLab | http://localhost:18888 | This notebook; token supplied by opener |
| Trino | http://localhost:18080 | Query status UI; SQL through Python driver |
| PostgreSQL | localhost:15432 | SQL clients; use the appropriate role |
| S3 API | http://localhost:18333 | S3 clients, not a graphical bucket browser |
| Superset | http://localhost:18088 | BI profile; admin password in `.env` |
| Airflow | http://localhost:18081 | Orchestration profile; generated login in its logs |
| Kafka Connect | http://localhost:18083 | CDC profile management API |
| Prometheus / Grafana | localhost:19090 / localhost:13000 | Monitoring profile |
| OpenMetadata | http://localhost:8585 | Catalog, lineage and quality evidence |

Run `.\\scripts\\lab.cmd platform` for the complete stack or start individual profiles with `bi`, `cdc`, `orchestration`, `monitoring` and `metadata`. A skipped optional cell does not count as a passed integration test.
''')
code('''
import os, json, time, inspect
from pathlib import Path
from decimal import Decimal
from itertools import islice
import pandas as pd
from IPython.display import display, Code, SVG, Markdown, Image
from lab.config import ROOT, DATA, ARTIFACTS, pg_kwargs, trino_connection
from lab import ingest, lakehouse, pipeline, exercises

RUN_BI = os.getenv('LAB_BI', '0') == '1'
RUN_CDC = os.getenv('LAB_CDC', '0') == '1'
print({'dataset_scope': 'full', 'bi': RUN_BI, 'cdc': RUN_CDC})
print('Project:', ROOT)
''')
md('''
## 1. Core architectural concepts — PDF pp. 5–6

**Data lake:** durable files and source evidence, interpreted by a processing/query engine. A bucket can be governed, but it does not itself define table transactions. We will preserve the Excel workbook and immutable Parquet batches.

**Lakehouse:** a lake plus managed table metadata and compatible engines. Parquet describes a file; Iceberg describes which files belong to a committed table snapshot. The JDBC catalog resolves a table name to authoritative metadata. Spark writes and Trino reads through that same catalog.

**Published data:** Trino views point to a validated Iceberg fact snapshot. Superset queries these views to display the release consistently.

**Medallion:** quality and consumption boundaries. Bronze retains source fidelity; silver establishes reusable meanings; gold exposes a defined product. These are not three required vendors or automatically three physical systems.

**Grain / مستوى تفصيل الصف:** exactly what one row means. Our fact grain is **one accepted workbook row**, not one invoice. One invoice can contain many products and identical-looking source rows can be legitimate.

**Exercise:** explain how the S3 files, the Iceberg table snapshots and the Trino view each participate in a published query.
''')
md('''
## 2. Architecture and data flow — PDF pp. 7–8

Spark transforms the completed bronze manifest into silver and gold Iceberg snapshots. Trino validates the snapshots and serves the approved release to Superset. Airflow schedules the run. OpenMetadata records discovery, lineage and quality, and Grafana shows the operational measurements.

SeaweedFS replaces the report's Ceph cluster for this single-machine lesson. It supplies S3 object storage only; its built-in table catalog is not used. The PostgreSQL-backed Iceberg catalog remains the single authority.
''')
code('''
diagram = '<svg xmlns="http://www.w3.org/2000/svg" width="950" height="360" viewBox="0 0 950 360"><rect width="950" height="360" fill="#0b1221"/>'
nodes=[(20,95,160,'UCI workbook'),(205,95,175,'Bronze + manifest'),(405,95,175,'Spark / Iceberg'),(605,95,155,'Trino views'),(785,95,140,'Superset')]
for x,y,w,label in nodes:
    diagram+=f'<rect x="{x}" y="{y}" width="{w}" height="65" rx="12" fill="#142b43" stroke="#28d7e7"/><text x="{x+w/2}" y="{y+37}" fill="white" text-anchor="middle" font-family="sans-serif" font-size="17">{label}</text>'
for x1,y1,x2,y2 in [(180,127,205,127),(380,127,405,127),(580,127,605,127),(760,127,785,127)]:
    diagram+=f'<path d="M{x1} {y1} L{x2} {y2}" stroke="#28d7e7" stroke-width="3" fill="none"/>'
diagram+='<text x="475" y="228" fill="#f2c572" text-anchor="middle" font-family="sans-serif" font-size="17">Validate Iceberg snapshots before publishing the full release</text></svg>'
display(SVG(diagram))
''')
md('''
## 3. Tools, dataset investigation and contracts — PDF pp. 9–11

### Why these tools?

| Capability | Selected implementation | Responsibility |
|---|---|---|
| Raw/analytical object storage | SeaweedFS S3 | Store source files, data files and metadata objects |
| Table correctness | Iceberg format v2 | Snapshots, schema evolution, atomic single-table commits |
| Transaction catalog | JDBC catalog in PostgreSQL | One authoritative table-name/metadata mapping |
| Processing | Spark SQL / Python | Parse, validate, transform and write Iceberg |
| Independent serving | Trino | Query Iceberg through shared metadata |
| Published datasets | Trino views | Read one validated Iceberg fact snapshot and aggregate it for BI |
| BI / orchestration | Superset / Airflow | Present products / execute dependencies and retries |
| CDC | Debezium + Kafka | Capture source WAL changes and retain transport history |
| Monitoring | Prometheus + Grafana | Expose pipeline quality, duration, freshness, dependency status and CDC |
| Discovery and governance | OpenMetadata | Catalog assets, lineage, stewards and observed quality results |

Airflow orchestrates the pipeline. OpenMetadata collects data asset metadata and links the observed results to the tools that produced them. The Iceberg JDBC catalog manages the table metadata Spark and Trino need to operate. Kubernetes, Feast and vector databases are design extensions.

### Dataset choice

[UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) contains 1,067,371 rows from a UK online retailer over December 2009–December 2011. The workbook is about 43.5 MB, licensed CC BY 4.0. Attribution: Chen, D. (2012), DOI [10.24432/C5CG6D](https://doi.org/10.24432/C5CG6D). We download the original workbook and retain its computed SHA-256. This detects later changes/replay mistakes; UCI does not supply an external checksum here that we claim to have verified.

The dataset was chosen because its real missing values and cancellations make quality rules meaningful, its size fits this laptop, and it can support the report's commerce examples without a Kaggle account.

### Initial contract

* Source-row identity: SHA-256 of workbook digest + sheet name + row number. Invoice number alone is not a line key.
* Monetary values: GBP; decimal arithmetic with six fractional digits retained during transformations. Display rounding happens at consumption.
* Time: source timestamps are naive. `Europe/London` is an explicit lab interpretation, not timezone metadata verified from UCI. Business date remains source-local.
* Missing customers: retain the transaction with `UNKNOWN` customer key. Do not silently remove its amount from totals.
* Negative quantities and cancellation invoices: retain and label. Negative unit prices, invalid dates, zero/non-integral quantities and missing invoice/product identifiers are quarantined with reasons.
* Net sales: signed quantity × unit price after the above contract. This is not audited revenue recognition, and no tax/fee policy is invented.
* Publication: input rows equal accepted plus quarantined. Source keys are unique, and the gold daily totals match the fact rollup exactly.

**Exercise:** decide how zero-price lines and postage/discount stock codes should be handled for a different business product. Change the contract before changing the code.
''')
md('''
## 4. Medallion: build bronze, silver and gold — PDF pp. 12–15

### 4.1 Bronze: completed evidence, not just an existing folder

The extractor streams Excel rows into bounded Parquet parts. Every part has a row count, size and SHA-256. The raw workbook is preserved separately. Only after the parts are uploaded and validated is a completed manifest written. A retry with the same source/scope verifies and reuses the completed input.

The notebook builds the full workbook and records a reconciled release. To run it from PowerShell and save an executed copy, use `.\\scripts\\lab.cmd notebook-full`. The separate `.\\scripts\\lab.cmd sample` command checks a deterministic 50,000-row slice in its own namespaces. Reports continue to show the latest validated full release.
''')
code('''
manifest = ingest.make_bronze()
display({k:manifest[k] for k in ['run_id','scope','rows','source_sha256','license']})
display(pd.DataFrame(manifest['files']))
display(pd.DataFrame(islice(ingest.iter_rows(manifest),5)))
assert ingest.validate_manifest(manifest)
''')
code('''
# Read the actual extraction implementation; follow source_row_id, flush(), and final manifest commit.
display(Code(inspect.getsource(ingest.make_bronze),language='python'))
''')
md('''
### 4.2 Compatibility gate before building the lakehouse

Spark, Scala, Java, Iceberg runtime artifacts, S3 IO and Trino must agree on the operations we actually use. A shared format name is insufficient. This smoke test creates an Iceberg format-v2 table with Spark and reads an exact decimal value from Trino through the same PostgreSQL catalog.

The next cell runs the same full pipeline as Airflow. The compatibility check is its first query validation. Expect `spark_write_trino_read: passed` and amount `12.34`. The remaining cells explore the resulting lakehouse.
''')
code('''
full_run = pipeline.run()
release = json.loads((DATA/'releases'/f"{full_run['release_id']}.json").read_text())
spark = lakehouse.spark_session()
compatibility = full_run['compatibility']
display(compatibility)
''')
md('''
### 4.3 Silver: meaning, quarantine and reconciliation

Parsing separates source strings from accepted numeric/time semantics. A rejected row is retained with its original source identifiers and a rejection reason. Negative quantities are not automatically invalid. Missing customer IDs remain visible through an unknown member.

The example implements a single writer and atomic single-table replacements. A release manifest later captures the coherent set of snapshots readers must use. It does not pretend that several independent Iceberg commits form a global transaction.

**Dataset investigation finding:** the workbook sheets overlap in December 2010 (the first ends 9 December and the second starts 1 December). A workbook row is therefore a source observation, not proof of a globally unique business line. The profiling query below exposes duplicate-looking observations, including candidates crossing sheets. They remain visible under this lesson's declared source-row grain. A financial product must obtain an authoritative line identity or approve a defensible deduplication rule before treating these totals as business truth.
''')
code('''
lake_candidate = release['lakehouse']
display({k:lake_candidate[k] for k in ['input_rows','accepted_rows','quarantined_rows','totals']})
assert lake_candidate['input_rows']==lake_candidate['accepted_rows']+lake_candidate['quarantined_rows']
display(spark.sql('SELECT rejection_reason,count(*) rows FROM lake.silver.quarantine GROUP BY rejection_reason').toPandas())
display(spark.sql('SELECT invoice,quantity,unit_price,line_amount,is_cancellation FROM lake.silver.transactions WHERE is_cancellation LIMIT 8').toPandas())
display(spark.sql('SELECT source_sheet,min(invoice_date) first_date,max(invoice_date) last_date,count(*) rows FROM lake.silver.transactions GROUP BY source_sheet').toPandas())
display(spark.sql("""SELECT count(*) duplicate_candidate_groups,sum(n-1) extra_observations,
    sum(CASE WHEN sheets>1 THEN n-1 ELSE 0 END) cross_sheet_extra_observations FROM (
    SELECT invoice,stock_code,description,quantity,unit_price,invoice_ts,customer_key,country,
    count(*) n,count(DISTINCT source_sheet) sheets FROM lake.silver.transactions
    GROUP BY invoice,stock_code,description,quantity,unit_price,invoice_ts,customer_key,country
    HAVING count(*)>1)""").toPandas())
''')
code('''
display(Code(inspect.getsource(lakehouse.build),language='python'))
''')
md('''
### 4.4 Gold: dimensional facts and explicit metrics

The fact table retains line-level detail. `dim_customer`, `dim_product`, and `dim_date` supply descriptive joins. Customer country stays on the transaction rather than inventing a single historical home country for each customer. `dim_product.description` uses a deterministic representative label; it is not a claim of an authoritative product master.

`daily_sales` groups by source-local business date, GBP and observed country. Gross positive sales, credits and signed net sales have separate definitions. Do not expect `gross_sales - credits = net_sales` for every possible cancellation anomaly without first checking the inclusion rules.

Expected: unique source keys and no orphan customer joins. The sum of detailed line amounts equals the sum of aggregate net sales.
''')
code('''
display(spark.sql('SELECT * FROM lake.gold.daily_sales ORDER BY invoice_date,country LIMIT 10').toPandas())
detail=spark.sql('SELECT sum(line_amount) FROM lake.gold.fact_invoice_line').first()[0]
aggregate=spark.sql('SELECT sum(net_sales) FROM lake.gold.daily_sales').first()[0]
assert detail==aggregate
print('Gold detail/aggregate reconciliation:',detail)
''')
md('''
## 5. Trino validation and publication

Trino validates the Iceberg snapshots created by Spark. It checks full row accounting, unique source-row IDs, the fact's signed decimal total and customer relationships. Trino recomputes daily groups from the pinned invoice facts and compares them with the materialized gold aggregate.

After validation, the release records the Iceberg snapshot IDs. `lake.published.fact_invoice_line` pins the released fact snapshot, and `lake.published.daily_sales` groups that same view for Superset.
''')
code('''
display(Code(inspect.getsource(pipeline.validate),language='python'))
display(Code(inspect.getsource(lakehouse.publish_views),language='python'))
''')
md('''
### Read and verify the published snapshots

The comparison checks every daily date/currency/country group, row counts, gross sales, credits and signed net sales. Decimal equality is exact. A match of grand totals alone would be weaker: two errors could cancel each other.

The release manifest and the BI query paths expose the validated release. Superset stays on its last published fact while Spark builds newer snapshots.
''')
code('''
validation=pipeline.validate(lake_candidate)
assert validation==release['validation']
display(validation)
print('Published release:',release['release_id'])
display(lakehouse.verify_bi_access())
''')
md('''
## 6. Technology comparisons and consumption — PDF pp. 19–21

| Choice | What this lab demonstrates | When to consider an alternative |
|---|---|---|
| Iceberg | Shared Spark/Trino tables and snapshots | Delta for a Spark/Databricks estate; Hudi for a justified update-heavy design |
| Separate Trino engine | Independent SQL access to shared files | Omit when Spark SQL or an existing managed endpoint meets the need |
| SeaweedFS | Laptop S3-compatible storage | Ceph with an operating storage team; managed object storage for reduced operations |
| Airflow | Dependencies, retries and audit of scheduled jobs | Simpler scheduler for a few stable jobs; native platform workflows |

These are fit decisions, not vendor performance rankings. The same physical laptop and public dataset cannot establish a universal production benchmark.
''')
code('''
rows=lakehouse.trino_daily(lake_candidate)
daily=pd.DataFrame(rows,columns=['invoice_date','currency','country','line_count','gross_sales','credits','net_sales'])
daily['net_sales_plot']=daily.net_sales.map(float)
ax=daily.groupby('invoice_date')['net_sales_plot'].sum().plot(figsize=(12,4),title='Signed net sales — published input scope',color='#007f91')
ax.set_ylabel('GBP');ax.set_xlabel('Business date');ax.figure.tight_layout()
ax.figure.savefig(ARTIFACTS/'daily-sales.png',dpi=150)
display(Image(filename=str(ARTIFACTS/'daily-sales.png')))
display(daily.groupby('country').net_sales_plot.sum().nlargest(10).rename('signed_net_sales_GBP'))
''')
md('''
### Editable Superset dashboard

In PowerShell run `.\\scripts\\lab.cmd bi` to start Superset or `.\\scripts\\lab.cmd platform` to start and provision the complete stack. When `RUN_BI=True`, this cell configures the read-only Trino connection and provisions the overview, product/customer, and credit/coverage dashboards.

To enable the cell interactively, set `RUN_BI=True` above. Sign in at http://localhost:18088 with `admin` and the local `SUPERSET_ADMIN_PASSWORD`. The dashboards read the `lake.published` views and follow later approved releases.
''')
code('''
if RUN_BI:
    from lab.bi import provision
    bi_result=provision();display(bi_result)
else:
    print('BI cell not executed: start the bi profile and set RUN_BI=True.')
''')
md('''
## 7. Lakehouse table operations, ordering and CDC — PDF pp. 22–25

### Snapshots and schema evolution

An Iceberg snapshot names a coherent version of one table. Time travel is useful for reproducibility and rollback, but retained snapshots are not a complete backup strategy. Readers still require reachable files and the catalog.

We demonstrate evolution on a dedicated teaching table, not by changing the published retail contract. A nullable field addition must be readable from Trino as well as Spark. Merge is tested against a single unambiguous source row per target key.
''')
code('''
spark.sql("CREATE TABLE IF NOT EXISTS lake.demo.evolution (id BIGINT, amount DECIMAL(18,2)) USING iceberg TBLPROPERTIES ('format-version'='2')")
if 'note' not in spark.table('lake.demo.evolution').columns:
    spark.sql('ALTER TABLE lake.demo.evolution ADD COLUMN note STRING')
spark.sql("INSERT OVERWRITE lake.demo.evolution VALUES (1,12.34,'initial')")
old_snapshot=spark.sql('SELECT snapshot_id FROM lake.demo.evolution.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
spark.sql("MERGE INTO lake.demo.evolution t USING (SELECT 1L id,CAST(13.37 AS DECIMAL(18,2)) amount,'updated' note) s ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
assert spark.sql(f'SELECT amount FROM lake.demo.evolution VERSION AS OF {old_snapshot}').first()[0]==Decimal('12.34')
cursor=trino_connection().cursor();cursor.execute('SELECT amount,note FROM lake.demo.evolution');current=cursor.fetchall()
assert current[0][0]==Decimal('13.37')
display({'time_travel_old_amount':'12.34','trino_current':current})
''')
md('''
### Duplicate events, authoritative ordering and delete tombstones

The static retail workbook does not contain database WAL changes. First inspect a deterministic state-machine exercise: an update with source version 2 arrives after a delete with version 3. The retained tombstone prevents resurrection. Replaying a known event is harmless; a different payload at the same version is a conflict.
''')
code('''
display(exercises.replay_experiment())
display(Code(inspect.getsource(exercises.apply_ordered_events),language='python'))
''')
md('''
### Real CDC extension

Start `.\\scripts\\lab.cmd cdc` in PowerShell. This adds a single-node Kafka broker and Debezium Connect. PostgreSQL was initialized with `wal_level=logical`; the connector uses a dedicated replication role and an explicitly created publication of `source_demo.public.customers`.

A published heartbeat advances offsets when customer traffic is idle. The launcher resumes the connector with the core lab. Monitor slot progress, retained WAL and connector process health.

Set `RUN_CDC=True` to run the following cells. Steps: seed synthetic customers → snapshot with Debezium → update/insert/delete source rows → poll Kafka into immutable bronze JSONL → persist the completed manifest → commit consumer offsets → merge retained events into Iceberg silver state.

The lab uses one Kafka partition. Integer source LSN and Kafka offset form the documented source-order/tie policy within that bounded setup. Do not generalize it into a total order across arbitrary topics, partitions and transactions. A long-running production consumer requires durable scheduling/checkpoint ownership and source WAL retention monitoring; Airflow should not launch one task per event.
''')
code('''
if RUN_CDC:
    from lab import cdc
    display(cdc.configure_connector())
    initial_cdc=cdc.consume_bronze(seconds=20);display(initial_cdc)
    assert initial_cdc['events']>0 or list((DATA/'bronze/cdc').glob('*/events.jsonl')), 'Initial snapshot has not landed; retry this cell before making source changes.'
    display(cdc.mutate_source())
    display(cdc.consume_bronze(seconds=20))
    cdc_result=cdc.apply_to_iceberg(spark);display(cdc_result)
    replayed=cdc.apply_to_iceberg(spark)
    assert cdc_result['state']==replayed['state']
    print('CDC replay and retained delete: passed')
else:
    print('CDC integration not executed: start cdc profile and set RUN_CDC=True.')
''')
md('''
## 8. Customer history and Trino query plans

SCD Type 2 retains versions of selected descriptive attributes. A surrogate key identifies a version; a natural key identifies the real entity. Half-open validity intervals `[valid_from, valid_to)` avoid double-matching a fact exactly at a change boundary.

The synthetic exercise checks that the boundary matches one version and that the intervals do not overlap. For query performance, inspect Trino's `EXPLAIN ANALYZE` to see the aggregation and the Iceberg scan work. Measure pruning and scan cost before choosing partitioning or compaction.
''')
code('''
display(exercises.scd2_experiment())
from contextlib import closing
with closing(trino_connection('lake_bi','published')) as conn:
    cursor=conn.cursor()
    cursor.execute('EXPLAIN ANALYZE SELECT country,sum(net_sales) FROM lake.published.daily_sales GROUP BY country')
    plan=cursor.fetchall()
print(chr(10).join(row[0] for row in plan))
''')
md('''
## 9. Cloud-managed equivalents — PDF pp. 29–31

| Capability | Local lab | AWS example | Azure/Fabric example | GCP example |
|---|---|---|---|---|
| Object storage | SeaweedFS | S3 | ADLS / OneLake | Cloud Storage |
| Managed tables | Iceberg | Iceberg with supported Glue catalog | Delta in Fabric/Databricks | Supported Iceberg/BigLake integration |
| Processing | Spark | Glue Spark | Fabric Spark / Databricks | Managed Spark |
| SQL/BI | Trino + Superset | Athena + BI | SQL endpoint + Power BI | BigQuery + BI |
| CDC | Debezium/Kafka | Supported DMS path | Supported pipelines/connectors | Supported Datastream path |

Verify Iceberg catalog ownership, format versions, reader/writer permissions, region support and maintenance ownership for a managed implementation.

**Exercise:** map the lakehouse to one managed cloud and explain how its query endpoint would publish the validated release.
''')
md('''
## 10. Cost, complexity and measured performance — PDF pp. 32–33

TCO includes storage, ingestion, transforms, query/capacity, metadata, network, backups, BI/licenses and operating labor. A free package does not have zero operating cost. The following arithmetic uses **illustrative user-supplied tariffs**, not live cloud quotes.

Measure cold and warm latency separately. Record input scope and query shape with each measurement.
''')
code('''
display(exercises.cost_model(storage_gib=100,storage_rate='0.02',queries_per_day=100,gib_per_query=20,scan_rate_per_tib='5'))
timings=[]
for i in range(5):
    started=time.perf_counter();cursor=trino_connection().cursor()
    cursor.execute(lakehouse.published_query()+' LIMIT 100');cursor.fetchall()
    timings.append(time.perf_counter()-started)
display({'trino_seconds':timings,'p50':float(pd.Series(timings).quantile(.5)),'p95':float(pd.Series(timings).quantile(.95)),'scope':manifest['scope']})
''')
md('''
## 11. Production operating model: demonstrate the boundaries — PDF pp. 34–36

### Roles and publication

The notebook identity bootstraps schemas and validates snapshots. Superset's `lake_bi` identity reads the published views. Permission checks verify that it is denied direct access to silver and gold data. See `docs/production-boundaries.md` for the deployment's trust model.

The failure exercise reads the active Iceberg aggregate, modifies one amount in memory and runs the actual group comparison. A difference raises before publication.
''')
code('''
from lab.operations import publication_gate_experiment
display(publication_gate_experiment())
display(lakehouse.verify_bi_access())
''')
md('''
### Scheduling, backfills and observability

Start `.\\scripts\\lab.cmd orchestration`. Airflow's `retail_learning_pipeline` submits a full-workbook run to the notebook service, validates its result and retries failures. The DAG delegates the full build to one task. Its stage timings are exported to Prometheus.

Start `.\\scripts\\lab.cmd monitoring` for Grafana's release and quality, services and catalog, and CDC dashboards. Prometheus records full-input coverage, reconciled groups, duration by stage, dependencies, and replication health.

Open [OpenMetadata](http://localhost:8585) and search for **Retail Sales**. Follow the workbook into bronze, silver, gold and the published Trino views. The gold `daily_sales` quality result contains the group comparison. See `docs/openmetadata.md` for dashboard links and verification.

### Read a retained release

Verify the data in an older release by querying its saved Iceberg snapshot IDs. Full recovery also depends on retained files, catalog metadata and the deployment configuration.
''')
code('''
from lab.operations import verify_pinned_release
older=[p for p in sorted((DATA/'releases').glob('*.json'),reverse=True) if p.stem < release['release_id']]
retained=next((item for path in older if (item:=json.loads(path.read_text())).get('engine')=='lakehouse'),release)
display(verify_pinned_release(retained))
display(pd.DataFrame([{'table':k,'published_snapshot':v} for k,v in release['lakehouse']['snapshots'].items()]))
''')
md('''
### Maintenance exercise

Inspect `lake.silver.transactions.files` and `.snapshots`. Compare file counts to query planning time. A lab with a few large files may not need compaction. A production compaction operation must consider concurrent writers and retention requirements. Never delete arbitrary old Parquet/metadata files from an Iceberg table directory.

Before expiration, identify every snapshot used by the published views and retained releases.
''')
code('''
display(spark.sql('SELECT count(*) data_files,sum(file_size_in_bytes) total_bytes FROM lake.silver.transactions.files').toPandas())
display(spark.sql('SELECT committed_at,snapshot_id,operation FROM lake.silver.transactions.snapshots ORDER BY committed_at DESC LIMIT 8').toPandas())
''')
md('''
## 12. Data science and AI guardrails — PDF p. 37

### Temporal features

An event can happen before prediction time but arrive afterwards. Training on its corrected/final value leaks information unavailable at deployment. The synthetic exercise compares an event-time-only sum with a sum that also respects `available_at`. Expected values: leaky feature 100; point-in-time feature 10.

### Governed retrieval

A document's presence in gold does not authorize every user to read it. The small retrieval exercise filters tenant permissions **before** constructing the model's evidence set. Real RAG additionally needs versioned parsing/chunking/index builds, cache isolation, revocation propagation, citation checks and backend tool authorization. No paid LLM, embedding model or vector database is installed for this boundary lesson.
''')
code('''
display(exercises.temporal_features())
display(exercises.governed_retrieval())
''')
md('''
## 13. Make an architecture decision from evidence — PDF pp. 38–39

Complete an architecture decision record with: consumers/products, grain and metric definitions, freshness/concurrency requirements, recovery/access constraints, measured workload, operating ownership, chosen option, alternatives, and revisit triggers.

The lab uses Iceberg, Spark and Trino to demonstrate how a writer, table catalog and query engine work together on the same data.

**Capstone exercises**

1. Trace one `source_row_id` from workbook sheet/row through bronze, silver and the published fact.
2. Replay the identical completed input. Prove row counts and grouped decimal totals do not change; snapshot IDs are allowed to change.
3. Add a malformed quantity to a separate synthetic batch. Show its quarantine reason and reconcile accepted + rejected rows.
4. Change a cancellation rule in the transformation and independent fact rollup. Update the contract and explain the expected metric movement.
5. Inject an old update after a newer delete and show the tombstone prevents resurrection.
6. Compare the row counts and totals in two retained Iceberg releases.
7. Define a query latency or freshness threshold that would justify scaling the lakehouse.

### What completion means

Check the run evidence for compatibility, row accounting, Iceberg integrity, daily totals, the publication gate, BI access and CDC replay. BI, orchestration, monitoring and the catalog report their own live results.
''')
code('''
from datetime import datetime,timezone
evidence={'executed_at':datetime.now(timezone.utc).isoformat(),'input_scope':manifest['scope'],'input_rows':manifest['rows'],
 'accepted_rows':lake_candidate['accepted_rows'],'quarantined_rows':lake_candidate['quarantined_rows'],
 'compatibility':'passed','iceberg_validation':validation,'schema_evolution_and_time_travel':'passed',
 'negative_BI_permissions':'passed','publication_failure_gate':'passed','replay_and_tombstones':'passed',
 'SCD2_intervals':'passed','temporal_features':'passed','permission_aware_retrieval':'passed',
 'BI_executed_in_this_notebook':RUN_BI,'CDC_executed_in_this_notebook':RUN_CDC}
(ARTIFACTS/'notebook-verification.json').write_text(json.dumps(evidence,indent=2))
display(evidence)
spark.stop()
''')
md('''
## References and next steps

Primary learning source: *Data Platform Architecture Research Report*, 10 November 2026, 42 pages. The coverage map is in `docs/pdf-coverage.md`; the original report remains available in your ChatGPT files.

Official implementation references checked during preparation:

* Dataset and attribution: https://archive.ics.uci.edu/dataset/502/online+retail+ii
* Iceberg engine compatibility: https://iceberg.apache.org/multi-engine-support/
* Spark installation: https://spark.apache.org/docs/3.5.8/api/python/getting_started/install.html
* Iceberg JDBC catalog: https://iceberg.apache.org/docs/latest/jdbc/
* Trino catalogs: https://trino.io/docs/current/object-storage/metastores.html
* SeaweedFS: https://github.com/seaweedfs/seaweedfs
* Airflow Docker: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/
* Debezium PostgreSQL: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
* Superset: https://superset.apache.org/docs/
* OpenMetadata: https://docs.open-metadata.org/v2.0.x/api-reference/lineage
* Docker Desktop WSL2: https://docs.docker.com/desktop/setup/install/windows-install/

`artifacts/runtime-lock.json`, `requirements.lock.txt` and `images.lock.json` record what was actually installed. `docs/production-boundaries.md` distinguishes implemented learning checks from a real production operating model.

Stop services without deleting data: `.\\scripts\\lab.cmd stop`. To resume: `.\\scripts\\lab.cmd start`. Do not use `docker compose down -v` unless you intentionally want to delete the lab's persistent volumes.
''')

notebook={'nbformat':4,'nbformat_minor':5,'metadata':{'kernelspec':{'display_name':'Python 3 (ipykernel)','language':'python','name':'python3'},'language_info':{'name':'python','version':'3.12'}},'cells':cells}
for i,cell in enumerate(cells):cell['id']=f'lesson-{i:03d}'
for name in ('notebook_template.ipynb', 'data_platform_architecture_lab.ipynb'):
    (ROOT/name).write_text(json.dumps(notebook,ensure_ascii=False,indent=1),encoding='utf-8')
print(f'Created notebook with {len(cells)} cells; {sum(c["cell_type"]=="code" for c in cells)} executable cells.')
