"""Verify full release values and every provisioned Grafana panel against live services."""
import json
import os
import sys
from pathlib import Path
from urllib.parse import quote

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lab.catalog_sources import GRAFANA_DASHBOARDS, prometheus_rules
from lab.config import ARTIFACTS
from lab.monitor import URLS
from lab.run_state import CURRENT_RELEASE, read_json, write_json


PROMETHEUS = 'http://prometheus:9090'
GRAFANA = 'http://grafana:3000'
FULL = '{scope="full",engine="lakehouse"}'


def get_json(base, path, session=requests, **params):
    response = session.get(f'{base}/{path}', params=params or None, timeout=15)
    response.raise_for_status()
    return response.json()


def query(expression, base=PROMETHEUS, session=requests):
    result = get_json(base, 'api/v1/query', session, query=expression)
    if result['status'] != 'success' or not result['data']['result']:
        raise RuntimeError(f'No live monitoring data for {expression}')
    return result['data']['result']


def verify():
    release = read_json(CURRENT_RELEASE)
    lake = release['lakehouse']
    assert release['scope'] == 'full' and release['engine'] == 'lakehouse' and lake['input_run_id'].endswith('-full')
    checks = {'lab_input_rows': lake['input_rows'], 'lab_accepted_rows': lake['accepted_rows'],
              'lab_quarantined_rows': lake['quarantined_rows'],
              'lab_validated_daily_groups': release['validation']['daily_groups_checked'],
              'lab_iceberg_checks_passed': sum(release['validation']['checks'].values())}
    for metric, expected in checks.items():
        assert float(query(metric+FULL)[0]['value'][1]) == expected, metric
    services = {item['metric']['service']: int(item['value'][1]) for item in query('lab_service_up')}
    assert set(services) == {*URLS, 'postgres', 'kafka'} and all(services.values()), services
    assert float(query('lab_catalog_last_sync_success')[0]['value'][1]) == 1
    assert float(query(f'lab_publication_timestamp_seconds{FULL} - lab_catalog_release_timestamp_seconds{FULL}')[0]['value'][1]) == 0
    assert float(query('lab_cdc_connector_running')[0]['value'][1]) == 1
    assert float(query('time() - lab_cdc_heartbeat_timestamp_seconds')[0]['value'][1]) < 60
    assert len(prometheus_rules()) == 9
    verified = {}
    with requests.Session() as session:
        session.auth = ('admin', os.environ['GRAFANA_PASSWORD'])
        datasource = get_json(GRAFANA, 'api/datasources/name/Learning%20Prometheus', session)
        proxy = f"{GRAFANA}/api/datasources/proxy/uid/{quote(datasource['uid'], safe='')}"
        for uid in GRAFANA_DASHBOARDS:
            dashboard = get_json(GRAFANA, f'api/dashboards/uid/{uid}', session)['dashboard']
            checked = []
            for panel in dashboard['panels']:
                for target in panel['targets']:
                    query(target['expr'], proxy, session)
                checked.append(panel['title'])
            verified[uid] = checked
    denied = requests.post('http://notebook:9000/run', headers={'Authorization': 'Bearer invalid-demo-token'}, timeout=10)
    assert denied.status_code == 403
    evidence = {'release_id': release['release_id'], 'full_release_metrics': checks, 'services': services,
                'alerts': len(prometheus_rules()), 'grafana_panels_verified': verified,
                'cdc_retained_wal_bytes': int(query('lab_cdc_retained_wal_bytes')[0]['value'][1]),
                'cdc_unconsumed_messages': int(query('lab_cdc_unconsumed_messages')[0]['value'][1]),
                'pipeline_api_unauthorized_status': denied.status_code}
    write_json(ARTIFACTS/'monitoring-verification.json', evidence)
    return evidence


if __name__ == '__main__':
    print(json.dumps(verify(), indent=2))
