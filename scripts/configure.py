"""Create local-only credentials and engine configuration. Never prints secrets."""
import json, os, secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
env_file = ROOT / '.env'
if env_file.exists():
    values = dict(line.split('=', 1) for line in env_file.read_text().splitlines() if '=' in line and not line.startswith('#'))
else:
    values = {k: secrets.token_hex(24) for k in (
        'POSTGRES_PASSWORD', 'S3_SECRET_KEY', 'JUPYTER_TOKEN',
        'SUPERSET_SECRET_KEY', 'SUPERSET_ADMIN_PASSWORD', 'GRAFANA_PASSWORD')}
    values['S3_ACCESS_KEY'] = 'learning-lab'
    env_file.write_text('\n'.join(f'{k}={v}' for k,v in values.items())+'\n', encoding='utf-8')
    try: env_file.chmod(0o600)
    except OSError: pass
for key in ['CDC_PASSWORD','PIPELINE_API_TOKEN']:
    values.setdefault(key,secrets.token_hex(24))
image_lock=ROOT/'images.lock.env'
if image_lock.exists():
    for line in image_lock.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            key,value=line.split('=',1)
            if key.endswith('_IMAGE'):values.setdefault(key,value)
env_file.write_text('\n'.join(f'{k}={v}' for k,v in values.items())+'\n',encoding='utf-8')
for name in ['data/source','data/bronze','data/releases','data/quarantine','artifacts','infra/trino']:
    (ROOT/name).mkdir(parents=True, exist_ok=True)
(ROOT/'infra/seaweedfs.json').write_text(json.dumps({'identities': [
    {'name':'lab-writer', 'credentials':[{'accessKey':values['S3_ACCESS_KEY'],'secretKey':values['S3_SECRET_KEY']}], 'actions':['Admin','Read','List','Tagging','Write']}
]}, indent=2))
(ROOT/'infra/trino/iceberg.properties').write_text(f'''connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.catalog-name=lake
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres:5432/iceberg_catalog
iceberg.jdbc-catalog.connection-user=lab_admin
iceberg.jdbc-catalog.connection-password={values['POSTGRES_PASSWORD']}
iceberg.jdbc-catalog.default-warehouse-dir=s3://lakehouse/warehouse
iceberg.jdbc-catalog.schema-version=V1
fs.native-s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key={values['S3_ACCESS_KEY']}
s3.aws-secret-key={values['S3_SECRET_KEY']}
iceberg.register-table-procedure.enabled=true
''')
print('Configured credentials, Trino catalogs and S3. Existing credentials retained.')
