import json, os, subprocess, uuid
import psycopg
from psycopg import sql
from .config import ROOT, DATA, ARTIFACTS, pg_kwargs
from .ingest import COLUMNS, iter_rows

def load(manifest):
    raw_schema = sql.Identifier('staging' if manifest['scope'] == 'full' else 'staging_sample')
    table = sql.SQL('{}.raw_lines').format(raw_schema)
    columns = sql.SQL(', ').join(map(sql.Identifier, COLUMNS))
    with psycopg.connect(**pg_kwargs()) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(raw_schema))
        definitions = sql.SQL(', ').join(sql.SQL('{} TEXT').format(sql.Identifier(c)) for c in COLUMNS)
        conn.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(table, definitions))
        conn.execute(sql.SQL('TRUNCATE {}').format(table))
        with conn.cursor().copy(sql.SQL('COPY {} ({}) FROM STDIN').format(table, columns)) as copy:
            for row in iter_rows(manifest): copy.write_row([row[c] for c in COLUMNS])
        rows=conn.execute(sql.SQL('SELECT count(*) FROM {}').format(table)).fetchone()[0]
        assert rows==manifest['rows']
    return rows

def build(manifest):
    schema='candidate_'+uuid.uuid4().hex[:12]
    full = manifest['scope'] == 'full'
    target = ROOT/'dbt/target' if full else DATA/'dbt-sample/target'
    log_path = ARTIFACTS/('dbt-build.log' if full else 'dbt-sample-build.log')
    env=dict(os.environ,DBT_BUILD_SCHEMA=schema,DBT_RAW_SCHEMA='staging' if full else 'staging_sample')
    proc=subprocess.run(['dbt','build','--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(ROOT/'dbt'),
                         '--target-path',str(target)],env=env,capture_output=True,text=True)
    log_path.write_text(proc.stdout+'\n'+proc.stderr, encoding='utf-8')
    if proc.returncode: raise RuntimeError(f'dbt failed. Inspect {log_path.name}.')
    with psycopg.connect(**pg_kwargs()) as conn:
        count,total=conn.execute(sql.SQL('SELECT count(*),sum(line_amount) FROM {}.fact_invoice_line').format(sql.Identifier(schema))).fetchone()
        bad=conn.execute(sql.SQL('SELECT count(*) FROM {}.stg_typed WHERE rejection_reason IS NOT NULL').format(sql.Identifier(schema))).fetchone()[0]
        assert count+bad==manifest['rows']
    outcomes = json.loads((target/'run_results.json').read_text())['results']
    result={'build_schema':schema,'input_run_id':manifest['run_id'],'accepted_rows':count,'quarantined_rows':bad,
            'net_sales':str(total),'dbt_build':'passed',
            'dbt_models':sum(item['unique_id'].startswith('model.') for item in outcomes),
            'dbt_tests_passed':sum(item['unique_id'].startswith('test.') and item['status']=='pass' for item in outcomes)}
    filename = 'warehouse_candidate.json' if full else 'sample_warehouse_candidate.json'
    (ARTIFACTS/filename).write_text(json.dumps(result,indent=2))
    return result

def daily(candidate):
    with psycopg.connect(**pg_kwargs()) as conn:
        return conn.execute(sql.SQL('SELECT invoice_date,currency,country,line_count,gross_sales,credits,net_sales FROM {}.daily_sales ORDER BY 1,2,3').format(sql.Identifier(candidate['build_schema']))).fetchall()

def publish(candidate):
    schema=candidate['build_schema']
    with psycopg.connect(**pg_kwargs()) as conn:
        for name in ['fact_invoice_line','dim_customer','dim_product','dim_date','daily_sales']:
            conn.execute(sql.SQL('CREATE OR REPLACE VIEW mart.{} AS SELECT * FROM {}.{}').format(sql.Identifier(name),sql.Identifier(schema),sql.Identifier(name)))
        conn.execute(sql.SQL('CREATE OR REPLACE VIEW core.transactions AS SELECT * FROM {}.stg_valid_lines').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('CREATE OR REPLACE VIEW core.typed_lines AS SELECT * FROM {}.stg_typed').format(sql.Identifier(schema)))
        conn.execute('GRANT SELECT ON ALL TABLES IN SCHEMA mart TO bi_reader')
        conn.execute('CREATE TABLE IF NOT EXISTS audit.publication (published_at timestamptz DEFAULT now(), input_run_id text, build_schema text)')
        conn.execute('INSERT INTO audit.publication (input_run_id,build_schema) VALUES (%s,%s)',(candidate['input_run_id'],schema))

def negative_access_test():
    results={}
    with psycopg.connect(**pg_kwargs(reader=True),autocommit=True) as conn:
        assert conn.execute('SELECT count(*) FROM mart.daily_sales').fetchone()[0]>0
        for name in ['staging.raw_lines','core.transactions','core.typed_lines']:
            try: conn.execute('SELECT * FROM '+name+' LIMIT 1')
            except psycopg.errors.InsufficientPrivilege: results[name]='denied as expected'
            else: raise AssertionError(f'Unexpected BI access: {name}')
    return results
