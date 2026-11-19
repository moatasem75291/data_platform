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

**Project:** retail analytics on a lake, an Iceberg lakehouse, and a PostgreSQL warehouse.
**Source:** the 42-page report dated 10 November 2026, chapters 1–13.
**Learning goal:** understand each component's responsibility, trace a real record to a report, and prove correctness before choosing a larger stack.

This notebook is the main learning document. Supporting Python, SQL/dbt, Compose and monitoring files are part of the lesson, and can be opened and edited from JupyterLab's file browser. Code cells inspect important implementations so that helper calls are not unexplained black boxes.

**How to study:** read one section, run its cells in order, inspect the output, then attempt its exercise. Use Kernel → Restart Kernel and Run All to check reproducibility. A successful cell is evidence for that specific check, not a production-readiness certificate.

The real dataset contains historical transaction rows. Operational customer changes, SCD2 segments and availability-time examples below are **synthetic teaching data**. They are never presented as observed retail history.

### Contents
0. Setup and controls · 1. Concepts · 2. Data flows · 3. Tools and contracts · 4. Medallion · 5. Warehouse · 6. Alternatives · 7. Lakehouse operations and CDC · 8. Warehouse history · 9. Cloud mappings · 10. Cost and performance · 11. Operations and recovery · 12. AI connections · 13. Decision record and exercises.
''')
md('''
## 0. Setup, ports and learning controls

The installed environment uses Docker Desktop's WSL2 backend. Python 3.12 and Java 17 run inside the notebook container. Spark executes locally with four threads; Trino is a separate container. PostgreSQL contains separate databases for warehouse, Iceberg catalog, Airflow metadata and synthetic operational source data.

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
from lab import ingest, lakehouse, warehouse, pipeline, exercises

RUN_BI = os.getenv('LAB_BI', '0') == '1'
RUN_CDC = os.getenv('LAB_CDC', '0') == '1'
print({'dataset_scope': 'full', 'bi': RUN_BI, 'cdc': RUN_CDC})
print('Project:', ROOT)
''')
md('''
## 1. Core architectural concepts — PDF pp. 5–6

**Data lake:** durable files and source evidence, interpreted by a processing/query engine. A bucket can be governed, but it does not itself define table transactions. We will preserve the Excel workbook and immutable Parquet batches.

**Lakehouse:** a lake plus managed table metadata and compatible engines. Parquet describes a file; Iceberg describes which files belong to a committed table snapshot. The JDBC catalog resolves a table name to authoritative metadata. Spark writes and Trino reads through that same catalog.

**Warehouse:** an analytical database with governed SQL models. PostgreSQL manages its own tables, transactions and system catalog. We will load staging data, use dbt for SQL transformations/tests, and publish dimensional marts.

**Medallion:** quality and consumption boundaries. Bronze retains source fidelity; silver establishes reusable meanings; gold exposes a defined product. These are not three required vendors or automatically three physical systems.

**Grain / مستوى تفصيل الصف:** exactly what one row means. Our fact grain is **one accepted workbook row**, not one invoice. One invoice can contain many products and identical-looking source rows can be legitimate.

**Exercise:** explain why a Parquet folder, an Iceberg table and a PostgreSQL table are different even when all three contain the same rows.
''')
md('''
## 2. Architecture and data flow — PDF pp. 7–8

Both branches use the same completed bronze manifest and business rules. They are independently transformed, then compared by business date, currency and country. The visual below shows the data plane. Airflow, contracts, catalog, quality checks, identities and recovery form the control plane around it.

SeaweedFS replaces the report's Ceph cluster for this single-machine lesson. It supplies S3 object storage only; its built-in table catalog is not used. The PostgreSQL-backed Iceberg catalog remains the single authority.
''')
code('''
diagram = '<svg xmlns="http://www.w3.org/2000/svg" width="950" height="360" viewBox="0 0 950 360"><rect width="950" height="360" fill="#0b1221"/>'
nodes=[(20,140,170,'UCI workbook'),(230,140,190,'Bronze + manifest'),(470,55,210,'Spark / Iceberg'),(470,235,210,'PostgreSQL / dbt'),(740,55,180,'Trino / Gold'),(740,235,180,'Warehouse marts')]
for x,y,w,label in nodes:
    diagram+=f'<rect x="{x}" y="{y}" width="{w}" height="65" rx="12" fill="#142b43" stroke="#28d7e7"/><text x="{x+w/2}" y="{y+37}" fill="white" text-anchor="middle" font-family="sans-serif" font-size="17">{label}</text>'
for x1,y1,x2,y2 in [(190,172,230,172),(420,172,470,87),(420,172,470,267),(680,87,740,87),(680,267,740,267)]:
    diagram+=f'<path d="M{x1} {y1} L{x2} {y2}" stroke="#28d7e7" stroke-width="3" fill="none"/>'
diagram+='<text x="475" y="340" fill="#f2c572" text-anchor="middle" font-family="sans-serif" font-size="17">Quality gates → exact reconciliation → versioned publication → BI</text></svg>'
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
| Warehouse | PostgreSQL + dbt | SQL models, relational publication, model dependency/tests |
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
* Publication: input rows = accepted + quarantined; source keys are unique; daily totals match exactly across engines.

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

The next cell runs the same full pipeline as Airflow. The compatibility check is its first query validation. Expect `spark_write_trino_read: passed` and amount `12.34`. The remaining cells explore the resulting lakehouse and warehouse.
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
## 5. Traditional warehouse and ELT — PDF pp. 16–18

The warehouse branch starts from the same completed bronze files, not the lakehouse's silver output. Python bulk-loads restricted `staging.raw_lines`. dbt independently parses and models the data using PostgreSQL SQL.

Every build gets a new `candidate_<id>` schema. dbt builds typed staging models, dimensions, facts and aggregates there, then runs uniqueness, relationship, row-accounting and amount-reconciliation tests. This deliberately avoids changing the published interface while tests are still running.

After cross-engine validation, a PostgreSQL transaction updates the stable `core` and `mart` views and writes audit evidence. This implements ELT: load source-shaped data, then transform inside the analytical database. ETL would move more of those transformations before loading. Both still need contracts and replay rules.
''')
code('''
warehouse_candidate=release['warehouse']
display({'loaded':full_run['input_rows'],**warehouse_candidate})
display(Code((ROOT/'dbt/models/staging/stg_typed.sql').read_text(),language='sql'))
display(Code((ROOT/'dbt/models/marts/daily_sales.sql').read_text(),language='sql'))
''')
md('''
### Compare independently produced products before publication

The comparison checks every daily date/currency/country group, row counts, gross sales, credits and signed net sales. Decimal equality is exact. A match of grand totals alone would be weaker: two errors could cancel each other.

The release records Iceberg snapshot IDs and the successful warehouse build schema. Consumers can pin Iceberg reads to those snapshots. PostgreSQL publication is atomic within its transaction; the combined lakehouse/warehouse release is recorded explicitly, rather than claiming a cross-system distributed transaction.
''')
code('''
reconciliation=pipeline.reconcile(lake_candidate,warehouse_candidate)
assert reconciliation==release['reconciliation']
display(reconciliation)
print('Published release:',release['release_id'])
display(warehouse.negative_access_test())
''')
md('''
## 6. Technology comparisons and consumption — PDF pp. 19–21

| Choice | What this lab demonstrates | When to consider an alternative |
|---|---|---|
| Iceberg | Shared Spark/Trino tables and snapshots | Delta for a Spark/Databricks estate; Hudi for a justified update-heavy design |
| PostgreSQL warehouse | Dimensional SQL products and transactional publication | MPP/managed warehouse after measured concurrency or scan limits |
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

In PowerShell run `.\\scripts\\lab.cmd bi` to start Superset or `.\\scripts\\lab.cmd platform` to start and provision the complete stack. The following cell, when `RUN_BI=True`, creates the read-only warehouse connection and provisions the overview, product/customer, and credit/coverage dashboards.

To enable the cell interactively, set `RUN_BI=True` above and rerun this cell. Sign in at http://localhost:18088 with `admin` and the local `SUPERSET_ADMIN_PASSWORD`. Do not display that password in the notebook. The dashboard reads stable `mart` views; it is refreshed by later approved releases.
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

A separately published heartbeat table advances offsets when customer traffic is idle. This prevents warehouse writes in other databases from silently accumulating retained source WAL. Once CDC is enabled, the launcher resumes its consumer with the core lab. Monitor slot progress as well as connector process health.

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
## 8. Warehouse history and maintenance — PDF pp. 26–28

SCD Type 2 retains versions of selected descriptive attributes. A surrogate key identifies a version; a natural key identifies the real entity. Half-open validity intervals `[valid_from, valid_to)` avoid double-matching a fact exactly at a change boundary.

The exercise creates `learning.customer_history` from synthetic customer segments and checks that the boundary resolves to exactly one row and that no versions overlap. It does not invent segment history for UCI customers.

For performance, inspect `EXPLAIN (ANALYZE, BUFFERS)` before adding indexes. PostgreSQL sequential scans can be appropriate for broad analytical aggregates. Partitioning is justified by pruning/retention needs, not by a rule that every fact table must be partitioned.
''')
code('''
display(exercises.scd2_experiment())
import psycopg
with psycopg.connect(**pg_kwargs()) as conn:
    plan=conn.execute('EXPLAIN (ANALYZE, BUFFERS) SELECT country,sum(net_sales) FROM mart.daily_sales GROUP BY country').fetchall()
print(chr(10).join(row[0] for row in plan))
''')
md('''
## 9. Cloud-managed equivalents — PDF pp. 29–31

| Capability | Local lab | AWS example | Azure/Fabric example | GCP example |
|---|---|---|---|---|
| Object storage | SeaweedFS | S3 | ADLS / OneLake | Cloud Storage |
| Managed tables | Iceberg | Iceberg with supported Glue catalog | Delta in Fabric/Databricks | Supported Iceberg/BigLake integration |
| Processing | Spark | Glue Spark | Fabric Spark / Databricks | Managed Spark |
| Warehouse | PostgreSQL | Redshift | Fabric Warehouse | BigQuery |
| SQL/BI | Trino + Superset | Athena + BI | SQL endpoint + Power BI | BigQuery + BI |
| CDC | Debezium/Kafka | Supported DMS path | Supported pipelines/connectors | Supported Datastream path |

These are alternative mappings, not a combined shopping list. Fabric Lakehouse's SQL endpoint is not the same item as Fabric Warehouse. Iceberg catalog ownership, format versions, reader/writer permissions, region/edition support and maintenance ownership must be verified for each managed path.

**Exercise:** design a warehouse-first Azure/Fabric version of this lab. Name which local components disappear, which capabilities remain, and which business definitions stay unchanged. No cloud resources are provisioned by this notebook.
''')
md('''
## 10. Cost, complexity and measured performance — PDF pp. 32–33

TCO includes storage, ingestion, transforms, query/capacity, metadata, network, backups, BI/licenses and operating labor. A free package does not have zero operating cost. The following arithmetic uses **illustrative user-supplied tariffs**, not live cloud quotes.

Measure cold/warm latency separately. This local exercise times repeated queries against already built products. Results describe the laptop lab, not a general lakehouse-versus-warehouse winner. Record input scope and query shape with every measurement.
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

The notebook connection is a lab administrator because it bootstraps schemas and catalogs. The BI warehouse connection is a separate `bi_reader` role with access to published marts; tests verify that staging/core access is denied. Spark/Trino and local S3 are in a trusted educational network. This lab does not claim full multi-tenant catalog/bucket authorization or enterprise TLS/SSO; see `docs/production-boundaries.md` for the concrete changes needed.

The publication failure experiment below creates an isolated candidate daily-sales table, changes one amount, then calls the real cross-engine reconciliation function. The error must block publication and leave the current release manifest unchanged. The temporary failure schema is removed after the check.
''')
code('''
from lab.operations import publication_gate_experiment
display(publication_gate_experiment())
display(warehouse.negative_access_test())
''')
md('''
### Scheduling, backfills and observability

Start `.\\scripts\\lab.cmd orchestration`. Airflow's `retail_learning_pipeline` submits a full-workbook run to the notebook service, validates its result and retries failures. The DAG delegates the full build to one task. Its stage timings are exported to Prometheus.

Start `.\\scripts\\lab.cmd monitoring` for Grafana's release and quality, services and catalog, and CDC dashboards. Prometheus records full-input coverage, reconciled groups, duration by stage, dependencies, and replication health.

Open [OpenMetadata](http://localhost:8585) and search for **Retail Sales**. Follow the bronze lineage into the dbt and Iceberg branches, then open the warehouse `daily_sales` test result to see the count of reconciled groups. The release catalog sync reads real asset metadata and pipeline evidence. See `docs/openmetadata.md` for dashboards, startup and verification commands.

### Backup and restore drill

In PowerShell run `.\\scripts\\lab.cmd restore`. It takes a logical warehouse backup, restores into a newly named database, and compares published fact row count and exact monetary total. The backup and evidence are copied to `artifacts/`. It does not overwrite the working warehouse.

Snapshot retention is not backup. A complete lakehouse disaster-recovery plan must preserve object data, Iceberg metadata, catalog pointers, credentials/configuration and coherent release references. Recovery from total object-volume loss is a documented follow-up boundary, not a test falsely claimed by the warehouse restore drill.
''')
code('''
restore_evidence=ARTIFACTS/'restore-verification.json'
if restore_evidence.exists():
    display(json.loads(restore_evidence.read_text(encoding='utf-8-sig')))
else:
    print('Restore drill not yet recorded. Run scripts/lab.cmd restore in PowerShell.')
display(pd.DataFrame([{'table':k,'published_snapshot':v} for k,v in release['lakehouse']['snapshots'].items()]))
''')
md('''
### Maintenance exercise

Inspect `lake.silver.transactions.files` and `.snapshots`. Compare file counts to query planning time. A lab with a few large files may not need compaction. A production compaction operation must consider concurrent writers and retention requirements. Never delete arbitrary old Parquet/metadata files from an Iceberg table directory.

Suggested recovery objectives for this experiment: zero loss of the frozen input snapshot; restore the small warehouse within 15 minutes. Measure the actual restore time and state whether that target was met. These are lab targets, not an SLA for a real business.
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

**For this dataset alone**, a PostgreSQL warehouse or a small local SQL engine could deliver the analytical questions. The larger Iceberg/Spark/Trino branch is installed here to learn the report's component interactions, not because one million retail rows require distributed infrastructure.

**Capstone exercises**

1. Trace one `source_row_id` from workbook sheet/row through bronze, silver and the published fact.
2. Replay the identical completed input. Prove row counts and grouped decimal totals do not change; snapshot IDs are allowed to change.
3. Add a malformed quantity to a separate synthetic batch. Show its quarantine reason and reconcile accepted + rejected rows.
4. Change a cancellation inclusion rule. Update both transformations and the contract, then explain the expected metric movement.
5. Inject an old update after a newer delete and show the tombstone prevents resurrection.
6. Restore the warehouse into a fresh database and verify counts, totals and recovery time.
7. Choose a warehouse-first design for a small SQL team, then name the measured trigger that would justify Trino, streaming or a managed MPP engine.

### What completion means

The core lab is complete when the Spark/Trino compatibility gate, full-input accounting, dbt checks, grouped reconciliation, publication failure gate, negative BI permissions, replay/delete exercises and restart/run-all execution pass. BI, CDC, orchestration, monitoring and restore have their own evidence files. Read actual evidence; a configuration file alone is not evidence that a service was installed or tested.
''')
code('''
from datetime import datetime,timezone
evidence={'executed_at':datetime.now(timezone.utc).isoformat(),'input_scope':manifest['scope'],'input_rows':manifest['rows'],
 'accepted_rows':lake_candidate['accepted_rows'],'quarantined_rows':lake_candidate['quarantined_rows'],
 'compatibility':'passed','cross_engine_reconciliation':reconciliation,'schema_evolution_and_time_travel':'passed',
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
* dbt PostgreSQL: https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup
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
