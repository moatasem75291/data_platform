"""Build, validate and publish the full retail Iceberg release."""
import json
import time
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal

import psycopg

from . import ingest, lakehouse
from .config import ARTIFACTS, pg_kwargs, s3_client, trino_connection
from .run_state import CURRENT_RELEASE, PIPELINE_STATE, read_json, write_json


class PipelineBusy(RuntimeError):
    pass


class QualityGateError(RuntimeError):
    pass


def compare_daily(gold_rows, fact_rows):
    def index(rows):
        keyed = {tuple(row[:3]): (row[3], *(Decimal(str(value)) for value in row[4:])) for row in rows}
        if len(keyed) != len(rows):
            raise QualityGateError('Duplicate daily sales keys')
        return keyed

    gold = index(gold_rows)
    fact = index(fact_rows)
    if not gold or gold.keys() != fact.keys():
        raise QualityGateError('Missing or different daily sales keys')
    for key in gold:
        if gold[key] != fact[key]:
            raise QualityGateError(f'Daily line count or decimal total differs at {key}')
    return len(gold)


def validate(lake):
    gold, silver = ('gold', 'silver') if lake['scope'] == 'full' else ('sample_gold', 'sample_silver')
    fact = lakehouse.snapshot_relation(lake, f'{gold}.fact_invoice_line')
    customer = lakehouse.snapshot_relation(lake, f'{gold}.dim_customer')
    rejected = lakehouse.snapshot_relation(lake, f'{silver}.quarantine')
    with closing(trino_connection()) as connection:
        cur = connection.cursor()
        cur.execute(f'''SELECT count(*), count(DISTINCT source_row_id), sum(line_amount) FROM {fact}''')
        accepted, distinct_rows, total = cur.fetchone()
        cur.execute(f'SELECT count(*) FROM {rejected}')
        quarantined = cur.fetchone()[0]
        cur.execute(f'''SELECT count(*) FROM {fact} f LEFT JOIN {customer} d
                       ON f.customer_key=d.customer_key WHERE d.customer_key IS NULL''')
        orphan_customers = cur.fetchone()[0]
        cur.execute(lakehouse.daily_sales_query(fact))
        fact_groups = cur.fetchall()
    checks = {
        'source_row_accounting': (accepted == lake['accepted_rows']
                                  and quarantined == lake['quarantined_rows']
                                  and accepted + quarantined == lake['input_rows']),
        'unique_source_rows': accepted > 0 and accepted == distinct_rows,
        'fact_decimal_total': total is not None and total == Decimal(lake['totals']['net_sales']),
        'customer_keys_resolve': orphan_customers == 0,
    }
    if not all(checks.values()):
        raise QualityGateError('Failed Iceberg validation: '+', '.join(key for key, ok in checks.items() if not ok))
    groups = compare_daily(lakehouse.trino_daily(lake), fact_groups)
    checks['daily_totals_match_fact'] = True
    return {'input_run_id': lake['input_run_id'], 'passed': True, 'checks': checks,
            'daily_groups_checked': groups, 'signed_net_sales': str(total)}


def publish(lake, validation):
    if lake['scope'] != 'full' or not lake['input_run_id'].endswith('-full'):
        raise QualityGateError('Publishing requires a full workbook build')
    if not validation['passed'] or validation['input_run_id'] != lake['input_run_id']:
        raise QualityGateError('Publishing requires the matching Iceberg validation result')
    now = datetime.now(timezone.utc)
    release = {
        'release_id': now.strftime('%Y%m%dT%H%M%S%fZ'), 'published_at': now.isoformat(),
        'scope': 'full', 'engine': 'lakehouse', 'lakehouse': lake, 'validation': validation,
    }
    s3_client().put_object(Bucket='lakehouse', Key=f"releases/{release['release_id']}.json",
                          Body=json.dumps(release, indent=2).encode())
    lakehouse.publish_views(lake)
    write_json(CURRENT_RELEASE.parent/f"{release['release_id']}.json", release)
    write_json(CURRENT_RELEASE, release)
    return release


def run(limit=None):
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError('Sample limit must be a positive integer')
    with psycopg.connect(**pg_kwargs('iceberg_catalog'), autocommit=True) as lock:
        if not lock.execute('SELECT pg_try_advisory_lock(824453121)').fetchone()[0]:
            raise PipelineBusy('A retail pipeline run is already active')
        return _run(limit)


def _run(limit):
    full = limit is None
    started = time.time()
    timings = {}
    state = read_json(PIPELINE_STATE, {}) if full else {}
    if state.get('engine') != 'lakehouse':
        state = {'engine': 'lakehouse'}
    attempt = {'started_at': started, 'result': 'running', 'stage_seconds': timings}

    def mark_state():
        if full:
            state['attempt'] = attempt
            write_json(PIPELINE_STATE, state)

    def stage(name, action, *args):
        attempt['stage'] = name
        mark_state()
        start = time.perf_counter()
        try:
            return action(*args)
        finally:
            timings[name] = round(time.perf_counter() - start, 3)

    spark = None
    try:
        manifest = stage('bronze', ingest.make_bronze, limit)
        spark = stage('spark_start', lakehouse.spark_session)
        compatibility = stage('compatibility', lakehouse.smoke, spark)
        lake = stage('iceberg', lakehouse.build, spark, manifest)
        validation = stage('validation', validate, lake)
        release = stage('publication', publish, lake, validation) if full else None
        access = stage('bi_access', lakehouse.verify_bi_access) if full else None
        if access and (access['rows'] != lake['accepted_rows']
                       or Decimal(access['signed_net_sales']) != Decimal(validation['signed_net_sales'])):
            raise QualityGateError('Published Trino views differ from the validated Iceberg fact')
        result = {
            'engine': 'lakehouse', 'scope': manifest['scope'], 'published': full, 'compatibility': compatibility,
            'release_id': release['release_id'] if release else None,
            'input_rows': manifest['rows'], 'accepted_rows': lake['accepted_rows'], 'quarantined_rows': lake['quarantined_rows'],
            'validation': validation, 'seconds': round(time.time() - started, 2), 'stage_seconds': timings,
        }
        if access:
            result['bi_access'] = access
        write_json(ARTIFACTS/('verification.json' if full else 'sample-verification.json'), result)
        if full:
            state['successful_runs'] = state.get('successful_runs', 0) + 1
            state['last_success'] = result
        attempt.update(result='success', release_id=result['release_id'])
        return result
    except Exception as exc:
        state['failed_runs'] = state.get('failed_runs', 0) + 1
        attempt.update(result='failure', error=type(exc).__name__)
        raise
    finally:
        attempt['finished_at'] = time.time()
        mark_state()
        if spark is not None:
            spark.stop()
