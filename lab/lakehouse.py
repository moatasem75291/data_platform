"""Spark is the sole writer; Trino independently reads the shared Iceberg catalog."""
import json, os
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from .config import DATA, ARTIFACTS, pg_kwargs, trino_connection
from .ingest import validate_manifest

def spark_session():
    from pyspark.sql import SparkSession
    pg=pg_kwargs('iceberg_catalog')
    builder=(SparkSession.builder.appName('data-platform-learning').master('local[4]')
      .config('spark.driver.memory','3g').config('spark.sql.shuffle.partitions','4')
      .config('spark.sql.session.timeZone','UTC')
      .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
      .config('spark.jars',','.join(str(p) for p in Path('/opt/jars').glob('*.jar')))
      .config('spark.sql.catalog.lake','org.apache.iceberg.spark.SparkCatalog')
      .config('spark.sql.catalog.lake.catalog-impl','org.apache.iceberg.jdbc.JdbcCatalog')
      .config('spark.sql.catalog.lake.uri',f"jdbc:postgresql://{pg['host']}:{pg['port']}/iceberg_catalog")
      .config('spark.sql.catalog.lake.jdbc.user',pg['user']).config('spark.sql.catalog.lake.jdbc.password',pg['password'])
      .config('spark.sql.catalog.lake.jdbc.schema-version','V1')
      .config('spark.sql.catalog.lake.warehouse','s3://lakehouse/warehouse')
      .config('spark.sql.catalog.lake.io-impl','org.apache.iceberg.aws.s3.S3FileIO')
      .config('spark.sql.catalog.lake.s3.endpoint',os.getenv('S3_ENDPOINT','http://seaweedfs:8333'))
      .config('spark.sql.catalog.lake.s3.path-style-access','true')
      .config('spark.sql.catalog.lake.s3.access-key-id',os.environ['AWS_ACCESS_KEY_ID'])
      .config('spark.sql.catalog.lake.s3.secret-access-key',os.environ['AWS_SECRET_ACCESS_KEY'])
      .config('spark.sql.catalog.lake.client.region','us-east-1'))
    spark=builder.getOrCreate(); spark.sparkContext.setLogLevel('ERROR')
    for ns in ['silver','gold','demo']:spark.sql(f'CREATE NAMESPACE IF NOT EXISTS lake.{ns}')
    return spark

def smoke(spark):
    spark.sql('CREATE TABLE IF NOT EXISTS lake.demo.compatibility (id BIGINT, amount DECIMAL(18,2)) USING iceberg TBLPROPERTIES (\'format-version\'=\'2\')')
    spark.sql('INSERT OVERWRITE lake.demo.compatibility VALUES (1, 12.34)')
    cur=trino_connection().cursor();cur.execute('SELECT id, amount FROM lake.demo.compatibility')
    result=cur.fetchall();assert str(result[0][1])=='12.34'
    return {'spark_write_trino_read':'passed','rows':result}

def build(spark,manifest):
    validate_manifest(manifest)
    full = manifest['scope'] == 'full'
    silver, gold = ('silver', 'gold') if full else ('sample_silver', 'sample_gold')
    for namespace in (silver, gold):
        spark.sql(f'CREATE NAMESPACE IF NOT EXISTS lake.{namespace}')
    raw=spark.read.parquet(*[str(DATA/manifest['local_path']/f['name']) for f in manifest['files']])
    raw.createOrReplaceTempView('raw_lines')
    typed=spark.sql('''SELECT *,
      try_cast(quantity AS DECIMAL(20,6)) q,
      try_cast(unit_price AS DECIMAL(20,6)) p,
      try_cast(invoice_date AS TIMESTAMP) local_ts
      FROM raw_lines''')
    typed.createOrReplaceTempView('typed_lines')
    annotated=spark.sql('''SELECT *, CASE
      WHEN invoice IS NULL OR trim(invoice)='' THEN 'missing_invoice'
      WHEN stock_code IS NULL OR trim(stock_code)='' THEN 'missing_stock_code'
      WHEN local_ts IS NULL THEN 'invalid_date'
      WHEN q IS NULL OR q=0 OR q<>floor(q) OR abs(q)>1000000000 THEN 'invalid_quantity'
      WHEN p IS NULL OR p<0 THEN 'invalid_price'
      ELSE NULL END rejection_reason FROM typed_lines''').cache()
    good=annotated.where('rejection_reason IS NULL')
    bad=annotated.where('rejection_reason IS NOT NULL')
    annotated_count=annotated.count(); accepted=good.count();rejected=bad.count()
    assert annotated_count==manifest['rows']==accepted+rejected
    bad.writeTo(f'lake.{silver}.quarantine').tableProperty('format-version','2').createOrReplace()
    good.createOrReplaceTempView('valid_lines')
    canonical=spark.sql('''SELECT source_row_id,invoice,stock_code,description,
      cast(q AS BIGINT) quantity, cast(p AS DECIMAL(20,6)) unit_price,
      to_utc_timestamp(local_ts,'Europe/London') invoice_ts,
      cast(local_ts AS DATE) invoice_date,
      coalesce(nullif(regexp_replace(trim(customer_id),'[.]0$',''),''),'UNKNOWN') customer_key,
      coalesce(country,'UNKNOWN') country,
      'GBP' currency,
      (upper(invoice) LIKE 'C%' OR q<0) is_cancellation,
      cast(q*p AS DECIMAL(28,6)) line_amount,
      source_sheet,source_row
      FROM valid_lines''')
    canonical.writeTo(f'lake.{silver}.transactions').tableProperty('format-version','2').createOrReplace()
    spark.sql(f'SELECT * FROM lake.{silver}.transactions').writeTo(f'lake.{gold}.fact_invoice_line').tableProperty('format-version','2').createOrReplace()
    for name,query in {
      'dim_customer':f"SELECT customer_key FROM lake.{silver}.transactions GROUP BY customer_key",
      'dim_product':f"SELECT stock_code, min(description) description FROM lake.{silver}.transactions GROUP BY stock_code",
      'dim_date':f"SELECT invoice_date,year(invoice_date) calendar_year,month(invoice_date) calendar_month FROM lake.{silver}.transactions GROUP BY invoice_date",
      'daily_sales':f'''SELECT invoice_date,currency,country,count(*) line_count,
        sum(CASE WHEN line_amount>0 AND NOT is_cancellation THEN line_amount ELSE 0 END) gross_sales,
        sum(CASE WHEN line_amount<0 THEN -line_amount ELSE 0 END) credits,
        sum(line_amount) net_sales
        FROM lake.{silver}.transactions GROUP BY invoice_date,currency,country'''
    }.items():
        spark.sql(query).writeTo(f'lake.{gold}.{name}').tableProperty('format-version','2').createOrReplace()
    snapshots={}
    for name in [f'{silver}.transactions',f'{silver}.quarantine',f'{gold}.fact_invoice_line',f'{gold}.daily_sales',f'{gold}.dim_customer',f'{gold}.dim_product',f'{gold}.dim_date']:
        snapshots[name]=spark.sql(f'SELECT snapshot_id FROM lake.{name}.snapshots ORDER BY committed_at DESC LIMIT 1').first()[0]
    totals=spark.sql(f'SELECT count(*) rows,cast(sum(line_amount) AS STRING) net_sales FROM lake.{silver}.transactions').first().asDict()
    assert totals['rows']==accepted
    assert spark.sql(f'SELECT count(*)-count(DISTINCT source_row_id) FROM lake.{silver}.transactions').first()[0]==0
    assert spark.sql(f'SELECT count(*) FROM lake.{gold}.fact_invoice_line f LEFT ANTI JOIN lake.{gold}.dim_customer d ON f.customer_key=d.customer_key').first()[0]==0
    release={'input_run_id':manifest['run_id'],'published_at':datetime.now(timezone.utc).isoformat(),
      'scope':manifest['scope'],'snapshots':snapshots,'input_rows':manifest['rows'],'accepted_rows':accepted,'quarantined_rows':rejected,'totals':totals,
      'timezone_assumption':'Europe/London for naive source invoice timestamps; business date remains source-local',
      'metric_definition':'net_sales is signed quantity*unit_price, not audited accounting revenue'}
    filename = 'lakehouse_candidate.json' if full else 'sample_lakehouse_candidate.json'
    (ARTIFACTS/filename).write_text(json.dumps(release,indent=2))
    annotated.unpersist()
    return release

def published_query(table='gold.daily_sales'):
    release=json.loads((DATA/'releases/current.json').read_text())
    snapshot=release['lakehouse']['snapshots'][table]
    return f'SELECT * FROM lake.{table} FOR VERSION AS OF {int(snapshot)}'

def trino_daily(release):
    table = 'gold.daily_sales' if release.get('scope', 'full') == 'full' else 'sample_gold.daily_sales'
    with closing(trino_connection()) as conn:
        cur = conn.cursor()
        cur.execute(f'SELECT invoice_date,currency,country,line_count,gross_sales,credits,net_sales FROM {snapshot_relation(release, table)} ORDER BY 1,2,3')
        return cur.fetchall()


def snapshot_relation(lake, table):
    return f"lake.{table} FOR VERSION AS OF {int(lake['snapshots'][table])}"


def daily_from_fact(relation):
    return f'''SELECT invoice_date, currency, country, count(*) AS line_count,
        sum(line_amount) FILTER (WHERE line_amount > 0 AND NOT is_cancellation) AS positive_sales,
        sum(-line_amount) FILTER (WHERE line_amount < 0) AS negative_amounts,
        sum(line_amount) AS net_sales
        FROM {relation} GROUP BY invoice_date, currency, country'''


def daily_sales_query(relation):
    return f'''SELECT invoice_date, currency, country, line_count,
        coalesce(positive_sales, DECIMAL '0.000000') AS gross_sales,
        coalesce(negative_amounts, DECIMAL '0.000000') AS credits, net_sales
        FROM ({daily_from_fact(relation)}) AS daily'''


def publish_views(lake):
    """The invoice fact view is the single snapshot pointer for both BI datasets."""
    fact = snapshot_relation(lake, 'gold.fact_invoice_line')
    fact_ddl = f'CREATE OR REPLACE VIEW lake.published.fact_invoice_line SECURITY DEFINER AS SELECT * FROM {fact}'
    daily_ddl = 'CREATE OR REPLACE VIEW lake.published.daily_sales SECURITY DEFINER AS ' + daily_sales_query('lake.published.fact_invoice_line')
    with closing(trino_connection()) as conn:
        cur = conn.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS lake.published').fetchall()
        cur.execute("SELECT table_name FROM lake.information_schema.views WHERE table_schema='published'")
        has_fact = any(row[0] == 'fact_invoice_line' for row in cur.fetchall())
        # On release updates, leave the old fact live until the daily view definition is ready.
        commands = (daily_ddl, fact_ddl) if has_fact else (fact_ddl, daily_ddl)
        for command in commands:
            cur.execute(command).fetchall()


def verify_bi_access():
    from trino.exceptions import TrinoUserError

    with closing(trino_connection('lake_bi', 'published')) as conn:
        cur = conn.cursor()
        cur.execute('SELECT sum(line_count), sum(net_sales) FROM lake.published.daily_sales')
        count, total = cur.fetchone()
        cur.execute('SELECT source_row_id FROM lake.published.fact_invoice_line LIMIT 1')
        if not cur.fetchone():
            raise RuntimeError('The published invoice fact view was empty')
        denied = []
        for table in ('silver.quarantine', 'silver.transactions', 'gold.fact_invoice_line'):
            try:
                cur.execute(f'SELECT 1 FROM lake.{table} LIMIT 1').fetchall()
            except TrinoUserError as error:
                if error.error_name != 'PERMISSION_DENIED':
                    raise
                denied.append(table)
            else:
                raise RuntimeError(f'BI Trino identity can read {table}')
    return {'rows': count, 'signed_net_sales': str(total), 'unpublished_access_denied': denied}
