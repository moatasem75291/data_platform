"""Sync the local retail platform into OpenMetadata using its public REST API."""
import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime
from urllib.parse import quote

import psycopg
import requests
from confluent_kafka import KafkaException

from . import bi, catalog_sources as sources
from .catalog_auth import MetadataAPIError, local_api
from .config import ARTIFACTS, DATA, pg_kwargs
from .run_state import CATALOG_STATE, CURRENT_RELEASE, PIPELINE_STATE, read_json, write_json


DOMAIN = 'RetailAnalytics'
PRODUCT = 'RetailSales'
GOVERNANCE = {
    'Published': 'Consumer-facing Trino view released after validation of the Iceberg snapshots.',
    'Restricted': 'Source data, internal Iceberg tables or rejected observations. The BI identity reads published views.',
    'Pseudonymous': 'Source customer identifier. Missing identifiers use the explicit UNKNOWN member.',
}
GRAINS = {
    'transactions': 'One valid workbook observation with typed fields and explicit cancellation rules.',
    'fact_invoice_line': 'One accepted workbook observation. Cancellations and duplicate-looking source observations remain.',
    'daily_sales': 'One business date, GBP currency, and observed country. Signed sales sum quantity × unit price.',
    'dim_customer': 'One normalized customer key, including UNKNOWN. Country remains on the fact.',
    'dim_product': 'One source stock code with a deterministic example description.',
    'dim_date': 'One observed Europe/London business date.',
    'quarantine': 'One rejected source row, retained with its source identifiers and rejection reason.',
    'customer_state': 'One synthetic CDC customer, including retained delete markers and source order.',
    'customers': 'Synthetic PostgreSQL records used for the Debezium change stream only.',
    'lab_heartbeat': 'Synthetic connector heartbeat used to keep logical replication offsets advancing.',
}
COLUMN_NOTES = {
    'line_amount': 'Signed quantity × unit price, in GBP.',
    'net_sales': 'Sum of signed line amounts, in GBP. A source observation measure.',
    'gross_sales': 'Sum of positive line amounts on lines without a cancellation flag, in GBP.',
    'credits': 'Sum of the absolute value of negative line amounts, in GBP.',
    'source_row_id': 'SHA-256 identity derived from the workbook checksum, sheet name and source row number.',
    'invoice_date': 'Business date interpreted in the source-local Europe/London calendar.',
    'is_cancellation': 'True when the invoice starts with C or the quantity is negative.',
}
SQL_TYPES = {
    'text': 'TEXT', 'varchar': 'VARCHAR', 'character varying': 'VARCHAR', 'integer': 'INT',
    'bigint': 'BIGINT', 'smallint': 'SMALLINT', 'double': 'DOUBLE', 'double precision': 'DOUBLE',
    'real': 'FLOAT', 'boolean': 'BOOLEAN', 'date': 'DATE', 'timestamp': 'TIMESTAMP',
    'timestamp with time zone': 'TIMESTAMPZ', 'timestamp without time zone': 'TIMESTAMP',
    'numeric': 'NUMERIC', 'decimal': 'DECIMAL',
}
GRAFANA_LABELS = {'stat': 'Gauge', 'bargauge': 'Bar', 'timeseries': 'Line', 'table': 'Table'}


def tag(name):
    return {'tagFQN': f'RetailAccess.{name}', 'source': 'Classification', 'labelType': 'Manual', 'state': 'Confirmed'}


def column_metadata(raw):
    name, data_type, nullable = raw
    base = data_type.split('(', 1)[0].lower()
    if base == 'timestamp' and data_type.endswith('with time zone'):
        base = 'timestamp with time zone'
    column = {'name': name, 'dataType': SQL_TYPES.get(base, 'UNKNOWN'), 'dataTypeDisplay': data_type}
    if column['dataType'] == 'VARCHAR':
        if match := re.search(r'\((\d+)\)', data_type):
            column['dataLength'] = int(match[1])
        else:
            column['dataType'] = 'STRING'
    if nullable == 'NO':
        column['constraint'] = 'NOT_NULL'
    if name in COLUMN_NOTES:
        column['description'] = COLUMN_NOTES[name]
    if name in ('customer_key', 'customer_id'):
        column['tags'] = [tag('Pseudonymous')]
    return column


def millis(value):
    return int((value.timestamp() if isinstance(value, datetime) else value) * 1000)


class RetailCatalog:
    def __init__(self, api):
        self.api = api
        self.counts = Counter()
        self.owner = None

    def upsert(self, collection, entity_type, **body):
        entity = self.api.put(collection, body)
        self.counts[entity_type] += 1
        return {'id': entity['id'], 'type': entity_type, 'fullyQualifiedName': entity.get('fullyQualifiedName', entity['name'])}

    def owned(self, collection, entity_type, **body):
        return self.upsert(collection, entity_type, owners=[self.owner], domains=[DOMAIN], **body)

    def setup_governance(self):
        self.owner = self.upsert('teams', 'team', name='RetailDataStewards', teamType='Group',
                                 displayName='Retail Data Stewards',
                                 description='Accountable for the retail source contract, catalog and published data products.')
        self.upsert('domains', 'domain', name=DOMAIN, domainType='Aggregate', displayName='Retail Analytics',
                    description='Retail workbook, CDC, Iceberg lakehouse and analytics.', owners=[self.owner])
        self.owned('classifications', 'classification', name='RetailAccess', displayName='Retail Access',
                   description='Dataset stewardship and intended access in the learning platform.')
        for name, description in GOVERNANCE.items():
            self.owned('tags', 'tag', classification='RetailAccess', name=name, description=description)

    def service(self, category, service_type, name, title, description):
        return self.owned(f'services/{category}Services', f'{category}Service', name=name,
                          serviceType=service_type, displayName=title, description=description)

    def register_tables(self, service, database, tables, iceberg=False):
        database_ref = self.owned('databases', 'database', name=database, service=service['fullyQualifiedName'])
        schemas = {}
        result = {}
        for qualified, columns in tables.items():
            schema, name = qualified.split('.', 1)
            if schema not in schemas:
                schemas[schema] = self.owned('databaseSchemas', 'databaseSchema', name=schema,
                                             database=database_ref['fullyQualifiedName'])
            published = schema == 'published'
            properties = {'tableType': 'View' if published else 'Iceberg' if iceberg else 'Regular'}
            if iceberg:
                properties['sourceUrl'] = 'http://localhost:18080/ui/'
                if not published:
                    properties['fileFormat'] = 'parquet'
            result[qualified] = self.owned(
                'tables', 'table', name=name, description=GRAINS[name], columns=list(map(column_metadata, columns)),
                databaseSchema=schemas[schema]['fullyQualifiedName'], tags=[tag('Published' if published else 'Restricted')],
                **properties,
            )
        return result

    def pipeline(self, service, name, description, url=None, tasks=(), **body):
        if url:
            body['sourceUrl'] = url
        return self.owned('pipelines', 'pipeline', name=name, description=description,
                          service=service['fullyQualifiedName'], tasks=[{'name': task} for task in tasks], **body)

    def pipeline_status(self, pipeline, timestamp, run_id, status, ended=None):
        path = quote(pipeline['fullyQualifiedName'], safe='')
        body = {'timestamp': timestamp, 'executionId': run_id, 'executionStatus': status}
        if ended:
            body['endTime'] = ended
        self.api.put(f'pipelines/{path}/status', body)

    def link(self, upstream, downstream, description, source=None, pipeline=None, sql=None):
        edge = {'fromEntity': upstream, 'toEntity': downstream, 'description': description}
        if source:
            details = {'source': source}
            if pipeline:
                details['pipeline'] = pipeline
            if sql:
                details['sqlQuery'] = sql
            edge['lineageDetails'] = details
        self.api.put('lineage', {'edge': edge})
        self.counts['lineage'] += 1

    def storage(self, manifest, release, storage):
        service = self.service('storage', 'S3', 'RetailSeaweedFS', 'SeaweedFS S3',
                               'S3-compatible source evidence, bronze manifests and release metadata on the lab host.')

        def container(name, display, path, description, parent=None, **body):
            if parent:
                body['parent'] = parent
            return self.owned('containers', 'container', name=name, displayName=display, fullPath=path,
                              service=service['fullyQualifiedName'], description=description, **body)

        source = container('source-evidence', 'Source evidence bucket', 's3://source-evidence',
                           'The original checksummed workbook, licensed CC BY 4.0.')
        lake = container('lakehouse', 'Lakehouse bucket', 's3://lakehouse',
                         'Bronze, Iceberg objects and versioned publication manifests.')
        workbook = container('online_retail_ii', 'UCI Online Retail II workbook',
                             's3://source-evidence/'+storage['workbook'], manifest['attribution'],
                             parent=source, numberOfObjects=1, size=storage['workbook_bytes'],
                             sourceUrl='https://archive.ics.uci.edu/dataset/502/online+retail+ii')
        bronze = container('bronze_retail', 'Full-workbook bronze', 's3://lakehouse/'+manifest['s3_prefix'],
                           f"Checksummed immutable bronze with {manifest['rows']:,} rows in {len(manifest['files'])} Parquet files.",
                           parent=lake, numberOfObjects=len(manifest['files'])+1, fileFormats=['parquet', 'json'])
        catalog_releases = container('releases', 'Retail release manifests', 's3://lakehouse/releases',
                                     f"Latest cataloged release `{release['release_id']}` records the validated Iceberg snapshots.",
                                     parent=lake, fileFormats=['json'])
        self.link(workbook, bronze, 'Python extracts workbook observations into checksummed bronze parts.')
        cdc = None
        if storage['cdc_present']:
            cdc = container('bronze_cdc', 'Customer CDC bronze', 's3://lakehouse/bronze/cdc',
                            'Bounded Kafka reads persist Debezium row events before committing offsets.', parent=lake)
        return bronze, catalog_releases, cdc

    def dashboard(self, service, name, title, url, description, charts):
        chart_refs = [self.owned('charts', 'chart', service=service['fullyQualifiedName'], **chart) for chart in charts]
        return self.owned('dashboards', 'dashboard', name=name, displayName=title, sourceUrl=url,
                          service=service['fullyQualifiedName'], description=description,
                          charts=[ref['fullyQualifiedName'] for ref in chart_refs])

    def quality(self, release, tables):
        lake = release['lakehouse']
        validation = release['validation']
        groups = validation['daily_groups_checked']
        checks = [
            ('full_input_accounting', 'Full workbook row accounting', 'Completeness', 'fact_invoice_line',
             f"{lake['accepted_rows']:,} accepted + {lake['quarantined_rows']:,} quarantined = {lake['input_rows']:,} workbook rows.",
             {'input_rows': lake['input_rows'], 'accepted_rows': lake['accepted_rows'], 'quarantined_rows': lake['quarantined_rows']}),
            ('iceberg_daily_totals', 'Daily Iceberg totals match the invoice fact', 'Consistency', 'daily_sales',
             f'{groups:,} business keys match fact line counts, gross sales, credits and signed sales.',
             {'daily_groups_checked': groups}),
            ('iceberg_fact_integrity', 'Iceberg invoice fact integrity', 'Integrity', 'fact_invoice_line',
             'Source row IDs are unique, customer keys resolve, and the fact total matches the Spark result.',
             {'signed_net_sales_gbp': validation['signed_net_sales']}),
        ]
        timestamp = millis(datetime.fromisoformat(release['published_at']))
        recorded = []
        for name, title, dimension, table, result, values in checks:
            self.upsert('dataQuality/testDefinitions', 'testDefinition', name=name, displayName=title,
                        description='Result reported by the full retail publication workflow.', entityType='TABLE',
                        testPlatforms=['Other'], dataQualityDimension=dimension)
            fqn = tables[f'gold.{table}']['fullyQualifiedName']
            test = self.upsert('dataQuality/testCases', 'testCase', name=name, displayName=title,
                               description='Evidence from the source pipeline quality gate.', testDefinition=name,
                               entityLink=f'<#E::table::{fqn}>', owners=[self.owner])
            path = f"dataQuality/testCases/testCaseResults/{quote(test['fullyQualifiedName'], safe='')}"
            self.post_quality_result(path, timestamp, 'Success', result, values)
            recorded.append(test['fullyQualifiedName'])
        return recorded

    def post_quality_result(self, path, timestamp, status, result, values):
        body = {'timestamp': timestamp, 'testCaseStatus': status, 'result': result,
                'testResultValue': [{'name': key, 'value': str(value)} for key, value in values.items()]}
        try:
            self.api.request('POST', path, body)
        except MetadataAPIError as error:
            if error.status != 409:
                raise
            existing = self.api.get(path, {'startTimestamp': timestamp-1, 'endTimestamp': timestamp+1})['data']
            if not any(item['timestamp'] == timestamp and item['testCaseStatus'] == status for item in existing):
                raise


def publish_superset(catalog, dashboards, lake):
    service = catalog.service('dashboard', 'Superset', 'RetailSuperset', 'Apache Superset',
                              'Business dashboards over published Trino views of the full Iceberg release.')
    results = []
    for slug, dashboard in dashboards.items():
        charts = [{'name': f"chart-{chart['id']}", 'displayName': chart['slice_name'],
                   'sourceUrl': bi.SUPERSET_PUBLIC_URL+f"/explore/?slice_id={chart['id']}",
                   'chartType': {'table': 'Table', 'big_number_total': 'Gauge', 'echarts_timeseries_line': 'Line',
                                 'echarts_timeseries_bar': 'Bar'}.get(chart.get('viz_type'), 'Other')}
                  for chart in dashboard['charts']]
        ref = catalog.dashboard(service, slug, dashboard['title'], bi.SUPERSET_PUBLIC_URL+f'/superset/dashboard/{slug}/',
                                'Editable charts on the latest validated full-workbook Iceberg release.', charts)
        results.append(ref)
        for table in sorted(dashboard['tables']):
            catalog.link(lake[f'published.{table}'], ref, 'Superset chart reads this published Trino view.', 'DashboardLineage')
    return results


def publish_monitoring(catalog, grafana, rules):
    service = catalog.service('dashboard', 'Grafana', 'RetailGrafana', 'Grafana',
                              'Operational dashboards backed by the lab Prometheus datasource.')
    refs = []
    for uid, dashboard in grafana.items():
        charts = [{'name': f'{uid}-{panel["id"]}', 'displayName': panel['title'],
                   'description': panel.get('description') or 'Prometheus expressions: '+', '.join(
                       f"`{target['expr']}`" for target in panel['targets']),
                   'sourceUrl': f'http://localhost:13000/d/{uid}?viewPanel={panel["id"]}',
                   'chartType': GRAFANA_LABELS.get(panel['type'], 'Other')} for panel in dashboard['panels']]
        refs.append(catalog.dashboard(service, uid, dashboard['title'], 'http://localhost:13000/d/'+uid,
                                      'Live metrics and history from the Lab Prometheus datasource.', charts))
    if rules:
        prometheus = catalog.service('dashboard', 'CustomDashboard', 'RetailPrometheus', 'Prometheus',
                                     'Scrapes release, service and CDC metrics and evaluates platform alerts.')
        charts = [{'name': rule['name'], 'displayName': rule['name'], 'chartType': 'Other',
                   'sourceUrl': 'http://localhost:19090/alerts',
                   'description': f"{rule['annotations']['summary']}\n\nPromQL: `{rule['query']}`"} for rule in rules]
        refs.append(catalog.dashboard(prometheus, 'alerts', 'Retail alert rules', 'http://localhost:19090/alerts',
                                      'Rule definitions discovered from the running Prometheus API.', charts))
    return refs


def publish_orchestration(catalog, airflow):
    batch_description = 'Ingests the full workbook, validates the Iceberg snapshots and publishes the Trino views.'
    airflow_service = catalog.service('pipeline', 'Airflow', 'RetailAirflow', 'Apache Airflow', batch_description)
    batch = catalog.pipeline(airflow_service, airflow['name'], batch_description,
                             'http://localhost:18081/dags/'+airflow['name'], ('run_and_validate',),
                             scheduleInterval='@daily', state='Inactive' if airflow['paused'] else 'Active')
    for run_id, state, start, end in airflow['runs']:
        status = {'success': 'Successful', 'failed': 'Failed', 'running': 'Pending', 'queued': 'Pending'}.get(state)
        if status and start:
            catalog.pipeline_status(batch, millis(start), run_id, status, millis(end) if end else None)
    notebook_service = catalog.service('pipeline', 'CustomPipeline', 'RetailJupyter', 'JupyterLab',
                                       'Interactive teaching notebook and source code for the platform.')
    notebook = catalog.pipeline(notebook_service, 'retail_lab_notebook',
                                'Walkthrough and exercises for ingestion, reconciliation, BI, CDC and recovery.',
                                'http://localhost:18888/lab/tree/data_platform_architecture_lab.ipynb')
    return notebook


def publish_cdc(catalog, topics, connector, operational, cdc_bronze, lake):
    kafka = catalog.service('messaging', 'Kafka', 'RetailKafka', 'Apache Kafka',
                            'Synthetic customer CDC broker. [Browse topics](http://localhost:18084).')
    refs = {}
    for name, topic in topics.items():
        refs[name] = catalog.owned('topics', 'topic', name=name, service=kafka['fullyQualifiedName'],
                                   partitions=len(topic.partitions), replicationFactor=len(next(iter(topic.partitions.values())).replicas),
                                   sourceUrl='http://localhost:18084', description='Debezium PostgreSQL logical change stream or connector heartbeat.')
    if connector and sources.CDC_TOPIC in refs:
        service = catalog.service('pipeline', 'KafkaConnect', 'RetailDebezium', 'Debezium Kafka Connect',
                                  'PostgreSQL logical replication for the synthetic customer change stream.')
        pipeline = catalog.pipeline(service, 'retail-customers', 'Reads PostgreSQL WAL using the lab_slot logical slot.',
                                    'http://localhost:18083/connectors/retail-customers/status', ('postgres_logical_replication',),
                                    state='Active' if connector['connector']['state'] == 'RUNNING' else 'Inactive')
        topic = refs[sources.CDC_TOPIC]
        catalog.link(operational['public.customers'], topic, 'Debezium streams customer changes to Kafka.', 'PipelineLineage', pipeline)
        if cdc_bronze:
            catalog.link(topic, cdc_bronze, 'The bounded Kafka consumer persists events before advancing offsets.')
            if 'silver.customer_state' in lake:
                catalog.link(cdc_bronze, lake['silver.customer_state'], 'Spark merges the newest event per customer into Iceberg.')
    return list(refs.values())


def collect_optional(unavailable, name, function, fallback):
    try:
        result = function()
        if not result:
            unavailable.append(name)
        return result or fallback
    except (requests.ConnectionError, requests.Timeout, KafkaException):
        unavailable.append(name)
        return fallback


def index_release(api, release, attempt, strict):
    lake_run = release['lakehouse']
    if release.get('engine') != 'lakehouse' or not lake_run['input_run_id'].endswith('-full') or not release['validation']['passed']:
        raise RuntimeError('A validated full-workbook Iceberg release is required')
    catalog = RetailCatalog(api)
    manifest = read_json(DATA/'bronze'/lake_run['input_run_id']/'manifest.json')
    catalog.setup_governance()
    storage = sources.storage_metadata(manifest, release['release_id'])
    bronze, release_manifests, cdc_bronze = catalog.storage(manifest, release, storage)
    postgres = catalog.service('database', 'Postgres', 'RetailPostgres', 'PostgreSQL CDC source',
                               'Synthetic operational source and heartbeat for the Iceberg CDC demonstration.')
    operational = catalog.register_tables(postgres, 'source_demo', sources.postgres_columns('source_demo', sources.SOURCE_TABLES))
    trino = catalog.service('database', 'Trino', 'RetailTrino', 'Trino / Iceberg lakehouse',
                            'Queries Iceberg managed by Spark using the PostgreSQL JDBC catalog and SeaweedFS S3.')
    lake = catalog.register_tables(trino, 'lake', sources.iceberg_columns(), iceberg=True)
    for name in lake_run['snapshots']:
        if name not in lake:
            raise RuntimeError(f'Released Iceberg table {name} was not discovered through Trino')
    spark_service = catalog.service('pipeline', 'Spark', 'RetailSpark', 'Apache Spark',
                                    'PySpark writes typed silver and analytical gold tables using the Iceberg catalog.')
    spark = catalog.pipeline(spark_service, 'retail_iceberg_build', 'Transforms bronze and writes Iceberg.',
                             'http://localhost:18888/lab/tree/lab/lakehouse.py', ('normalize', 'quarantine', 'build_gold'))
    for target in ('silver.transactions', 'silver.quarantine'):
        catalog.link(bronze, lake[target], 'Spark applies the source contract and routes rows by validation result.')
    for target in sources.ICEBERG_TABLES['gold']:
        catalog.link(lake['silver.transactions'], lake[f'gold.{target}'], 'Spark builds this Iceberg gold table.', 'SparkLineage', spark)
    catalog.link(lake['gold.daily_sales'], release_manifests, 'The release gate compares Iceberg daily totals with the invoice facts.')
    catalog.link(lake['gold.fact_invoice_line'], lake['published.fact_invoice_line'],
                 'Pinned Iceberg snapshot selected by the full release.', 'ViewLineage')
    catalog.link(lake['published.fact_invoice_line'], lake['published.daily_sales'],
                 'Trino rolls up the same published invoice snapshot.', 'ViewLineage')
    if attempt.get('finished_at') and 'iceberg' in attempt['stage_seconds']:
        failed = attempt['result'] == 'failure' and attempt['stage'] == 'iceberg'
        execution_id = attempt.get('release_id') or f"full-{millis(attempt['started_at'])}"
        catalog.pipeline_status(spark, millis(attempt['started_at']), execution_id,
                                'Failed' if failed else 'Successful', millis(attempt['finished_at']))
    tests = catalog.quality(release, lake)

    unavailable = []
    airflow = collect_optional(unavailable, 'Airflow DAG', sources.airflow_dag, None)
    notebook = publish_orchestration(catalog, airflow) if airflow else None
    superset = collect_optional(unavailable, 'Superset dashboards', sources.superset_dashboards, {})
    if len(superset) != len(bi.DASHBOARDS) and 'Superset dashboards' not in unavailable:
        unavailable.append('Some Superset dashboards')
    dashboards = publish_superset(catalog, superset, lake)
    grafana = collect_optional(unavailable, 'Grafana dashboards', sources.grafana_dashboards, {})
    rules = collect_optional(unavailable, 'Prometheus rules', sources.prometheus_rules, [])
    dashboards.extend(publish_monitoring(catalog, grafana, rules))
    topics = collect_optional(unavailable, 'Kafka CDC topics', sources.kafka_topics, {})
    connector = collect_optional(unavailable, 'Debezium connector', sources.debezium_status, None)
    topics = publish_cdc(catalog, topics, connector, operational, cdc_bronze, lake)

    assets = [*dashboards, *topics, bronze, release_manifests, *lake.values()]
    if notebook:
        assets.append(notebook)
    product = catalog.owned('dataProducts', 'dataProduct', name=PRODUCT, displayName='Retail Sales',
                            description=f"Validated Iceberg data and analytics for full-workbook release `{release['release_id']}`.\n\n"
                            f"{lake_run['input_rows']:,} input rows, {lake_run['accepted_rows']:,} accepted and "
                            f"{lake_run['quarantined_rows']:,} quarantined. Signed sales are a GBP source-observation measure.\n\n"
                            '[Pipeline and quality dashboard](http://localhost:13000/d/learning-pipeline) · '
                            '[Business overview](http://localhost:18088/superset/dashboard/retail-overview/)')
    api.put(f"dataProducts/{quote(product['fullyQualifiedName'], safe='')}/assets/add", {'assets': assets})
    if strict and unavailable:
        raise RuntimeError('Catalog sources missing: '+', '.join(unavailable))
    return {'assets_synced': dict(catalog.counts), 'missing_optional_sources': unavailable,
            'quality_tests': tests, 'published_daily_sales': lake['published.daily_sales']['fullyQualifiedName'],
            'iceberg_daily_sales': lake['gold.daily_sales']['fullyQualifiedName'], 'data_product': product['fullyQualifiedName']}


def sync(strict=False):
    with psycopg.connect(**pg_kwargs('iceberg_catalog'), autocommit=True) as lock:
        lock.execute("SET lock_timeout='60s'")
        lock.execute('SELECT pg_advisory_lock(824453122)')
        previous = read_json(CATALOG_STATE, {})
        now = time.time()
        release = read_json(CURRENT_RELEASE)
        attempt = read_json(PIPELINE_STATE, {}).get('attempt', {})
        try:
            if not release:
                raise RuntimeError('Run the full pipeline before syncing OpenMetadata')
            with local_api() as api:
                evidence = index_release(api, release, attempt, strict)
        except Exception as error:
            write_json(CATALOG_STATE, {**previous, 'attempted_at': now, 'result': 'failure', 'error': type(error).__name__})
            raise
        result = {**evidence, 'engine': 'lakehouse', 'attempted_at': now, 'result': 'success', 'last_success_at': time.time(),
                  'release_id': release['release_id'], 'published_at': datetime.fromisoformat(release['published_at']).timestamp(),
                  'source_attempt_finished_at': attempt.get('finished_at')}
        write_json(CATALOG_STATE, result)
        write_json(ARTIFACTS/'openmetadata-verification.json', result)
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--strict', action='store_true', help='Require all optional profiles and their assets')
    print(json.dumps(sync(strict=parser.parse_args().strict), indent=2))
