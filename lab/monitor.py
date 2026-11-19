"""Prometheus metrics for the current release, full-run attempts, and service probes."""
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime

import psycopg
import requests
from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from .config import ARTIFACTS, pg_kwargs
from .run_state import CATALOG_STATE, CURRENT_RELEASE, PIPELINE_STATE, read_json


URLS = {
    'trino': 'http://trino:8080/v1/info',
    'seaweedfs': 'http://seaweedfs:8333/healthz',
    'superset': 'http://superset:8088/health',
    'airflow': 'http://airflow:8080/api/v2/monitor/health',
    'debezium': 'http://connect:8083/connectors',
    'kafka-ui': 'http://kafka-ui:8080/actuator/health',
    'prometheus': 'http://prometheus:9090/-/ready',
    'grafana': 'http://grafana:3000/api/health',
    'openmetadata': 'http://openmetadata:8586/healthcheck',
    'jupyter': 'http://localhost:8888/api',
}
CDC_TOPIC = 'labcdc.public.customers'
CDC_GROUP = 'learning-bronze-v1'


def probe(service):
    start = time.perf_counter()
    up = False
    try:
        if service == 'postgres':
            with psycopg.connect(**pg_kwargs('iceberg_catalog'), connect_timeout=3) as conn:
                up = conn.execute('SELECT 1').fetchone()[0] == 1
        elif service == 'kafka':
            host, port = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092').split(':', 1)
            with socket.create_connection((host, int(port)), timeout=3):
                up = True
        else:
            response = requests.get(URLS[service], timeout=3, allow_redirects=False)
            up = response.ok
            if up and service == 'airflow':
                health = response.json()
                up = all(health.get(component, {}).get('status') == 'healthy'
                         for component in ('metadatabase', 'scheduler'))
            if up and service == 'trino':
                up = not response.json().get('starting', False)
    except (OSError, ValueError, psycopg.Error, requests.RequestException):
        pass
    return service, int(up), time.perf_counter() - start


def cdc_metrics(kafka_up, connect_up):
    result = {}
    try:
        with psycopg.connect(**pg_kwargs(database='source_demo'), connect_timeout=3) as conn:
            slot = conn.execute('''SELECT active,
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),
                pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                FROM pg_replication_slots WHERE slot_name='lab_slot' ''').fetchone()
            if slot:
                result['replication_slot_active'] = int(slot[0])
                for key, value in zip(('retained_wal_bytes', 'unconfirmed_wal_bytes'), slot[1:]):
                    if value is not None:
                        result[key] = float(value)
            if conn.execute("SELECT to_regclass('public.lab_heartbeat')").fetchone()[0]:
                last = conn.execute('SELECT extract(epoch FROM updated_at) FROM public.lab_heartbeat WHERE id=1').fetchone()
                if last:
                    result['heartbeat_timestamp_seconds'] = float(last[0])
    except psycopg.Error:
        pass
    if connect_up:
        try:
            response = requests.get('http://connect:8083/connectors/retail-customers/status', timeout=3)
            if response.ok:
                status = response.json()
                result['connector_running'] = int(status['connector']['state'] == 'RUNNING'
                                                  and bool(status['tasks'])
                                                  and all(task['state'] == 'RUNNING' for task in status['tasks']))
        except (ValueError, requests.RequestException):
            pass
    if kafka_up:
        try:
            with closing(Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
                                   'group.id': CDC_GROUP, 'enable.auto.commit': False})) as consumer:
                topic = consumer.list_topics(timeout=3).topics.get(CDC_TOPIC)
                if topic and not topic.error:
                    partitions = [TopicPartition(CDC_TOPIC, partition) for partition in topic.partitions]
                    lags = []
                    for offset in consumer.committed(partitions, timeout=3):
                        low, high = consumer.get_watermark_offsets(offset, timeout=3)
                        lags.append(max(high - max(offset.offset, low), 0))
                    result['unconsumed_messages'] = sum(lags)
                    result['topic_partitions'] = len(partitions)
        except KafkaException:
            pass
    return result


def full_gauge(name, description, value):
    family = GaugeMetricFamily(name, description, labels=['scope', 'engine'])
    family.add_metric(['full', 'lakehouse'], value)
    return family


class LabMetrics:
    def __init__(self):
        self.services = []
        self.cdc = {}

    def refresh_health(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            while True:
                self.services = list(pool.map(probe, (*URLS, 'postgres', 'kafka')))
                up = {name: ready for name, ready, _ in self.services}
                self.cdc = cdc_metrics(up['kafka'], up['debezium'])
                time.sleep(15)

    def collect(self):
        services = GaugeMetricFamily('lab_service_up', 'Local service probe result', labels=['service'])
        durations = GaugeMetricFamily('lab_service_probe_seconds', 'Most recent probe duration', labels=['service'])
        for name, up, seconds in self.services:
            services.add_metric([name], up)
            durations.add_metric([name], seconds)
        yield services
        yield durations
        for name, value in self.cdc.items():
            yield GaugeMetricFamily(f'lab_cdc_{name}', f'CDC {name.replace("_", " ")}', value=value)

        release = read_json(CURRENT_RELEASE, {})
        if release.get('engine') == 'lakehouse' and release['scope'] == 'full':
            lake = release['lakehouse']
            values = {
                'input_rows': lake['input_rows'], 'accepted_rows': lake['accepted_rows'],
                'quarantined_rows': lake['quarantined_rows'],
                'publication_timestamp_seconds': datetime.fromisoformat(release['published_at']).timestamp(),
                'validated_daily_groups': release['validation']['daily_groups_checked'],
                'iceberg_checks_passed': sum(release['validation']['checks'].values()),
            }
            for name, value in values.items():
                yield full_gauge(f'lab_{name}', f'Latest full release {name.replace("_", " ")}', value)

        state = read_json(PIPELINE_STATE, {})
        if state.get('engine') != 'lakehouse':
            state = {}
        attempt = state.get('attempt', {})
        active = attempt.get('result') == 'running'
        yield full_gauge('lab_pipeline_running', 'A full pipeline attempt is running', int(active))
        if attempt.get('finished_at'):
            yield full_gauge('lab_pipeline_last_attempt_success', 'Last completed full run passed',
                             int(attempt['result'] == 'success'))
        counters = CounterMetricFamily('lab_pipeline_attempts', 'Completed full run attempts', labels=['result', 'engine'])
        for result, field in [('success', 'successful_runs'), ('failure', 'failed_runs')]:
            counters.add_metric([result, 'lakehouse'], state.get(field, 0))
        yield counters
        last_release = state.get('last_success', {})
        seconds = last_release.get('seconds')
        if seconds is not None:
            yield full_gauge('lab_pipeline_duration_seconds', 'Last successful full run duration', seconds)
            stages = GaugeMetricFamily('lab_pipeline_stage_duration_seconds', 'Last successful full run by stage', labels=['stage', 'engine'])
            for stage, seconds in last_release['stage_seconds'].items():
                stages.add_metric([stage, 'lakehouse'], seconds)
            yield stages

        catalog = read_json(CATALOG_STATE, {})
        if catalog.get('engine') == 'lakehouse':
            yield GaugeMetricFamily('lab_catalog_last_sync_success', 'Most recent catalog sync succeeded',
                                    value=int(catalog['result'] == 'success'))
            if 'last_success_at' in catalog:
                yield GaugeMetricFamily('lab_catalog_sync_timestamp_seconds', 'Most recent successful sync time',
                                        value=catalog['last_success_at'])
            if 'published_at' in catalog:
                yield full_gauge('lab_catalog_release_timestamp_seconds', 'Latest full release indexed in OpenMetadata',
                                 catalog['published_at'])
