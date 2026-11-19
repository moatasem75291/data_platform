"""Build versioned Grafana layout JSON for the lab's Prometheus metrics."""
import json
from pathlib import Path


OUTPUT = Path(__file__).resolve().parents[1]/'monitoring/grafana/dashboards'
GRAFANA = 'http://localhost:13000/d/'
LINKS = {
    'OpenMetadata': 'http://localhost:8585',
    'Superset': 'http://localhost:18088/superset/dashboard/retail-overview/',
    'Airflow': 'http://localhost:18081/dags/retail_learning_pipeline',
    'Prometheus alerts': 'http://localhost:19090/alerts',
    'Kafka UI': 'http://localhost:18084',
}
STATUS = [{'type': 'value', 'options': {
    '0': {'text': 'Down', 'color': 'red'}, '1': {'text': 'Up', 'color': 'green'},
}}]
PASS = [{'type': 'value', 'options': {
    '0': {'text': 'Failed', 'color': 'red'}, '1': {'text': 'Passed', 'color': 'green'},
}}]
RUNNING = [{'type': 'value', 'options': {
    '0': {'text': 'Idle', 'color': 'green'}, '1': {'text': 'Running', 'color': 'blue'},
}}]


def panel(title, metric, unit='short', kind='stat', *, description='', mappings=None, warn=None, bad=None):
    expressions = metric if isinstance(metric, list) else [(metric, title if kind == 'timeseries' else '')]
    options = {'legend': {'showLegend': True, 'displayMode': 'list', 'placement': 'bottom'}}
    if kind in ('stat', 'bargauge'):
        options = {'reduceOptions': {'calcs': ['lastNotNull']}, 'colorMode': 'value',
                   'graphMode': 'none', 'orientation': 'auto'}
    if kind == 'bargauge':
        options.update(displayMode='gradient', orientation='horizontal', showUnfilled=True)
    defaults = {'unit': unit, 'mappings': mappings or [], 'color': {'mode': 'thresholds' if warn or bad else 'palette-classic'}}
    if warn or bad:
        defaults['thresholds'] = {'mode': 'absolute', 'steps': [{'color': 'green', 'value': None}]}
        if warn:
            defaults['thresholds']['steps'].append({'color': 'orange', 'value': warn})
        if bad:
            defaults['thresholds']['steps'].append({'color': 'red', 'value': bad})
    if kind == 'timeseries':
        defaults['custom'] = {'drawStyle': 'line', 'lineWidth': 2, 'fillOpacity': 8, 'showPoints': 'never'}
    return {'title': title, 'description': description, 'type': kind, 'datasource': None,
            'targets': [{'expr': expr, 'legendFormat': legend, 'refId': chr(65+i),
                         'instant': kind in ('stat', 'bargauge'), 'range': kind == 'timeseries'}
                        for i, (expr, legend) in enumerate(expressions)],
            'options': options, 'fieldConfig': {'defaults': defaults, 'overrides': []}}


def dashboard(filename, uid, title, rows, additional_links):
    y = 0
    panels = []
    for height, row in rows:
        width = 24 // len(row)
        for i, item in enumerate(row):
            item.update(id=len(panels)+1, gridPos={'h': height, 'w': width, 'x': i*width, 'y': y})
            panels.append(item)
        y += height
    links = {**LINKS, **additional_links}
    output = {'title': title, 'uid': uid, 'schemaVersion': 39, 'version': 1,
              'tags': ['retail-lab', 'observability'], 'time': {'from': 'now-24h', 'to': 'now'},
              'refresh': '30s', 'timezone': 'browser', 'panels': panels,
              'links': [{'type': 'link', 'title': name, 'url': url, 'targetBlank': True} for name, url in links.items()]}
    (OUTPUT/filename).write_text(json.dumps(output, indent=2)+'\n', encoding='utf-8')


def full(metric):
    return metric+'{scope="full",engine="lakehouse"}'


dashboard('pipeline.json', 'learning-pipeline', 'Retail Lakehouse | Releases and Quality', [
    (5, [panel('Full workbook input', full('lab_input_rows'), 'locale'),
         panel('Accepted invoice lines', full('lab_accepted_rows'), 'locale'),
         panel('Quarantined lines', full('lab_quarantined_rows'), 'locale'),
         panel('Daily groups validated', full('lab_validated_daily_groups'), 'locale')]),
    (5, [panel('Last full run', full('lab_pipeline_last_attempt_success'), mappings=PASS),
         panel('Current run', full('lab_pipeline_running'), mappings=RUNNING),
         panel('Iceberg checks passed', full('lab_iceberg_checks_passed')),
         panel('Release age', 'time() - '+full('lab_publication_timestamp_seconds'), 's', warn=86400, bad=93600)]),
    (8, [panel('Full execution duration', full('lab_pipeline_duration_seconds'), 's', 'timeseries'),
         panel('Last successful run by stage', [('lab_pipeline_stage_duration_seconds{engine="lakehouse"}', '{{stage}}')], 's', 'bargauge')]),
    (8, [panel('Successful and failed attempts', [('lab_pipeline_attempts_total{engine="lakehouse"}', '{{result}}')], 'short', 'timeseries',
               description='Counters start with this instrumentation and persist across service restarts.'),
         panel('Quarantine rate', f'100 * {full("lab_quarantined_rows")} / {full("lab_input_rows")}', 'percent', 'timeseries'),
         panel('Unaccounted input rows', ' - '.join(map(full, ('lab_input_rows', 'lab_accepted_rows', 'lab_quarantined_rows'))), 'short', 'timeseries')]),
], {'Services and catalog': GRAFANA+'learning-services', 'CDC and WAL': GRAFANA+'learning-cdc'})

dashboard('services.json', 'learning-services', 'Retail Platform | Services and Catalog', [
    (5, [panel('Services responding', 'sum(lab_service_up)'),
         panel('Metrics exporter', 'up{job="learning_pipeline"}', mappings=STATUS),
         panel('Last catalog sync', 'lab_catalog_last_sync_success', mappings=PASS),
         panel('Catalog sync age', 'time() - lab_catalog_sync_timestamp_seconds', 's', warn=86400)]),
    (8, [panel('Dependency status', [('lab_service_up', '{{service}}')], 'short', 'bargauge', mappings=STATUS,
               description='An optional profile also appears down while its containers are stopped.'),
         panel('Probe latency', [('lab_service_probe_seconds', '{{service}}')], 's', 'timeseries')]),
    (8, [panel('Release version gap', f'clamp_min({full("lab_publication_timestamp_seconds")} - {full("lab_catalog_release_timestamp_seconds")}, 0)',
               's', 'timeseries', description='Zero means the newest full release is cataloged.'),
         panel('Firing alerts', 'count(ALERTS{alertstate="firing"}) or vector(0)', 'short', 'timeseries', bad=1)]),
], {'Releases and quality': GRAFANA+'learning-pipeline', 'CDC and WAL': GRAFANA+'learning-cdc'})

dashboard('cdc.json', 'learning-cdc', 'Retail CDC | Kafka, Debezium and WAL', [
    (5, [panel('Debezium task', 'lab_cdc_connector_running', mappings=STATUS),
         panel('PostgreSQL slot', 'lab_cdc_replication_slot_active', mappings=STATUS),
         panel('Heartbeat age', 'time() - lab_cdc_heartbeat_timestamp_seconds', 's', warn=30, bad=60),
         panel('Kafka partitions', 'lab_cdc_topic_partitions')]),
    (9, [panel('PostgreSQL WAL', [('lab_cdc_retained_wal_bytes', 'Retained'),
                                  ('lab_cdc_unconfirmed_wal_bytes', 'Unconfirmed')], 'bytes', 'timeseries',
               description='The retained reading measures what the logical replication slot holds on disk.'),
         panel('Unconsumed customer change messages', 'lab_cdc_unconsumed_messages', 'short', 'timeseries',
               description='End offsets minus committed bronze consumer offsets, bounded by the log start.')]),
], {'Releases and quality': GRAFANA+'learning-pipeline', 'Services and catalog': GRAFANA+'learning-services'})
