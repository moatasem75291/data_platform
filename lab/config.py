import os
from pathlib import Path
from dotenv import load_dotenv
ROOT = Path(os.environ.get('LAB_ROOT', Path(__file__).resolve().parents[1]))
load_dotenv(ROOT/'.env', override=False)
DATA = ROOT/'data'
ARTIFACTS = ROOT/'artifacts'
for p in [DATA, ARTIFACTS]: p.mkdir(parents=True, exist_ok=True)

def pg_kwargs(database=None):
    return dict(host=os.getenv('PGHOST','localhost'), port=int(os.getenv('PGPORT','15432')),
                user=os.getenv('PGUSER','lab_admin'),
                password=os.environ['PGPASSWORD'] if 'PGPASSWORD' in os.environ else os.environ['POSTGRES_PASSWORD'],
                dbname=database or os.getenv('PGDATABASE', 'iceberg_catalog'))

def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT','http://localhost:18333'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID',os.environ.get('S3_ACCESS_KEY')),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY',os.environ.get('S3_SECRET_KEY')),
        region_name='us-east-1',config=Config(request_checksum_calculation='when_required',response_checksum_validation='when_required'))

def trino_connection(user='learning', schema='gold'):
    import trino
    return trino.dbapi.connect(host=os.getenv('TRINO_HOST','localhost'),port=int(os.getenv('TRINO_PORT','18080')),
        user=user,catalog='lake',schema=schema,http_scheme='http')
