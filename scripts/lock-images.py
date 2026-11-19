"""Resolve seed tags to immutable repository digests without exposing credentials."""
import json,subprocess
from pathlib import Path
root=Path(__file__).resolve().parents[1]
mapping={'POSTGRES_IMAGE':'postgres:17','SEAWEEDFS_IMAGE':'chrislusf/seaweedfs:latest',
 'TRINO_IMAGE':'trinodb/trino:484','AIRFLOW_IMAGE':'apache/airflow:3.3.1-python3.12',
 'KAFKA_IMAGE':'apache/kafka:4.1.0','DEBEZIUM_IMAGE':'quay.io/debezium/connect:3.3',
 'PROMETHEUS_IMAGE':'prom/prometheus:v3.5.0','GRAFANA_IMAGE':'grafana/grafana:12.2.0'}
result={};env=dict(line.split('=',1) for line in (root/'.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
for key,image in mapping.items():
    process=subprocess.run(['docker','image','inspect',env.get(key,image)],text=True,capture_output=True)
    if process.returncode:
        result[key]={'seed':image,'status':'not installed'};continue
    info=json.loads(process.stdout)[0];digests=info.get('RepoDigests',[])
    result[key]={'seed':image,'id':info['Id'],'repo_digests':digests}
    if digests:env[key]=digests[0]
(root/'artifacts/images.lock.json').write_text(json.dumps(result,indent=2))
(root/'images.lock.env').write_text('\n'.join(f'{k}={env[k]}' for k in mapping if k in env)+'\n')
(root/'.env').write_text('\n'.join(f'{k}={v}' for k,v in env.items())+'\n')
print('Recorded resolved images and pinned installed service images in the local configuration.')
