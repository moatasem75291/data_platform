"""Read catalog metadata and run evidence from the lab's running services."""
import os
from contextlib import closing

import psycopg
import requests
from confluent_kafka.admin import AdminClient

from . import bi
from .config import pg_kwargs, s3_client, trino_connection
from .monitor import CDC_TOPIC


PG_TABLES = {
    'staging': ('raw_lines',),
    'core': ('typed_lines', 'transactions'),
    'mart': ('fact_invoice_line', 'dim_customer', 'dim_product', 'dim_date', 'daily_sales'),
}
SOURCE_TABLES = {'public': ('customers', 'lab_heartbeat')}
ICEBERG_TABLES = {
    'silver': ('transactions', 'quarantine', 'customer_state'),
    'gold': PG_TABLES['mart'],
}
GRAFANA_DASHBOARDS = ('learning-pipeline', 'learning-services', 'learning-cdc')


def get_json(url, session=requests, **kwargs):
    response = session.get(url, timeout=20, **kwargs)
    response.raise_for_status()
    return response.json()


def postgres_columns(database, allowed):
    with psycopg.connect(**pg_kwargs(database=database), connect_timeout=4) as conn:
        conn.read_only = True
        cols = conn.execute('''SELECT table_schema, table_name, column_name, data_type, is_nullable
            FROM information_schema.columns WHERE table_schema = ANY(%s)
            ORDER BY table_schema, table_name, ordinal_position''', (list(allowed),)).fetchall()
    return group_columns(cols, allowed)


def iceberg_columns():
    with closing(trino_connection()) as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT table_schema, table_name, column_name, data_type, is_nullable
                           FROM lake.information_schema.columns WHERE table_schema IN ('silver', 'gold')
                           ORDER BY table_schema, table_name, ordinal_position""")
        return group_columns(cursor.fetchall(), ICEBERG_TABLES)


def group_columns(rows, allowed):
    tables = {}
    for schema, table, column, data_type, nullable in rows:
        if table in allowed.get(schema, ()):
            tables.setdefault(f'{schema}.{table}', []).append((column, data_type, nullable))
    return tables


def airflow_dag():
    dag_id = 'retail_learning_pipeline'
    with psycopg.connect(**pg_kwargs(database='airflow'), connect_timeout=4) as conn:
        conn.read_only = True
        dag = conn.execute('SELECT is_paused FROM dag WHERE dag_id=%s', (dag_id,)).fetchone()
        if not dag:
            return None
        runs = conn.execute('''SELECT run_id, state, coalesce(start_date, queued_at, created_at), end_date
            FROM dag_run WHERE dag_id=%s ORDER BY id DESC LIMIT 20''', (dag_id,)).fetchall()
    return {'name': dag_id, 'paused': dag[0], 'runs': runs}


def superset_dashboards():
    with bi.superset_session() as session:
        dashboards = bi.request(session, 'GET', 'dashboard/', params={'q': '(page_size:100)'})['result']
        result = {}
        for dashboard in dashboards:
            slug = dashboard.get('slug')
            if slug in bi.DASHBOARDS and dashboard.get('published'):
                charts = bi.request(session, 'GET', f"dashboard/{dashboard['id']}/charts")['result']
                dependencies = {chart['dataset'] for row in bi.DASHBOARDS[slug][1] for chart in row}
                result[slug] = {'title': dashboard['dashboard_title'], 'charts': charts, 'tables': dependencies}
        return result


def grafana_dashboards():
    auth = ('admin', os.environ['GRAFANA_PASSWORD'])
    return {uid: get_json(f'http://grafana:3000/api/dashboards/uid/{uid}', auth=auth)['dashboard']
            for uid in GRAFANA_DASHBOARDS}


def prometheus_rules():
    result = get_json('http://prometheus:9090/api/v1/rules')['data']
    return [rule for group in result['groups'] if group['name'].startswith('lab_') for rule in group['rules']]


def kafka_topics():
    admin = AdminClient({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')})
    topics = admin.list_topics(timeout=5).topics
    return {name: metadata for name, metadata in topics.items()
            if name in (CDC_TOPIC, '__debezium-heartbeat.labcdc') and not metadata.error}


def debezium_status():
    status = requests.get('http://connect:8083/connectors/retail-customers/status', timeout=8)
    if status.status_code == 404:
        return None
    status.raise_for_status()
    return status.json()


def storage_metadata(manifest, release_id):
    with closing(s3_client()) as client:
        workbook = f"{manifest['source_sha256']}/{manifest['source_file']}"
        source = client.head_object(Bucket='source-evidence', Key=workbook)
        client.head_object(Bucket='lakehouse', Key=f"{manifest['s3_prefix']}/manifest.json")
        client.head_object(Bucket='lakehouse', Key=f'releases/{release_id}.json')
        cdc = client.list_objects_v2(Bucket='lakehouse', Prefix='bronze/cdc/', MaxKeys=1)
    return {'workbook': workbook, 'workbook_bytes': source['ContentLength'], 'cdc_present': bool(cdc['KeyCount'])}
