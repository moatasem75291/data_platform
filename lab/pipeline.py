import json
import shutil
import time
from datetime import datetime, timezone
from decimal import Decimal

import psycopg

from . import ingest, lakehouse, warehouse
from .config import ARTIFACTS, DATA, ROOT, pg_kwargs, s3_client
from .run_state import CURRENT_RELEASE, PIPELINE_STATE, read_json, write_json


class PipelineBusy(RuntimeError):
    pass


class QualityGateError(RuntimeError):
    pass


def reconcile(lake, wh):
    left = lakehouse.trino_daily(lake)
    right = warehouse.daily(wh)
    left_by_key = {tuple(row[:3]): tuple(row[3:]) for row in left}
    right_by_key = {tuple(row[:3]): tuple(row[3:]) for row in right}
    if len(left_by_key) != len(left) or len(right_by_key) != len(right):
        raise QualityGateError('Duplicate aggregate keys')
    if not left or left_by_key.keys() != right_by_key.keys():
        raise QualityGateError('Missing or different daily aggregate keys')
    for key, warehouse_row in right_by_key.items():
        lake_row = left_by_key[key]
        if lake_row[0] != warehouse_row[0]:
            raise QualityGateError(f'Line count mismatch at {key}')
        if any(Decimal(str(a)) != Decimal(str(b)) for a, b in zip(lake_row[1:], warehouse_row[1:])):
            raise QualityGateError(f'Monetary mismatch at {key}')
    total = sum(row[3] for row in right)
    for candidate in (lake, wh):
        if total != candidate['accepted_rows']:
            raise QualityGateError('Aggregate line counts do not account for all accepted rows')
    if lake['quarantined_rows'] != wh['quarantined_rows']:
        raise QualityGateError('Different quarantine counts')
    return {'daily_groups_checked': len(left), 'exact_decimal_match': True, 'accepted_rows': wh['accepted_rows']}


def publish(lake, wh, checks):
    source_run = lake['input_run_id']
    if lake.get('scope', 'full') != 'full' or not source_run.endswith('-full'):
        raise QualityGateError('Only a full workbook build can publish to the shared marts')
    if not checks['exact_decimal_match'] or source_run != wh['input_run_id']:
        raise QualityGateError('Both branches must reconcile against the same input')
    now = datetime.now(timezone.utc)
    release = {
        'release_id': now.strftime('%Y%m%dT%H%M%S%fZ'),
        'published_at': now.isoformat(), 'scope': 'full',
        'lakehouse': lake, 'warehouse': wh, 'reconciliation': checks,
    }
    if 'dbt_tests_passed' in wh:
        archive = DATA / 'releases' / f"{release['release_id']}-dbt"
        archive.mkdir(parents=True, exist_ok=True)
        for filename in ('manifest.json', 'run_results.json'):
            shutil.copyfile(ROOT/'dbt/target'/filename, archive/filename)

    # PostgreSQL publishes in one transaction. The release manifest records the Iceberg versions.
    warehouse.publish(wh)
    body = json.dumps(release, indent=2).encode()
    s3_client().put_object(Bucket='lakehouse', Key=f"releases/{release['release_id']}.json", Body=body)
    write_json(CURRENT_RELEASE.parent/f"{release['release_id']}.json", release)
    write_json(CURRENT_RELEASE, release)
    write_json(ARTIFACTS/'metrics.json', {
        'input_rows': lake['input_rows'], 'accepted_rows': lake['accepted_rows'],
        'quarantined_rows': lake['quarantined_rows'], 'publication_timestamp_seconds': now.timestamp(),
    })
    return release


def run(limit=None):
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError('Sample limit must be a positive integer')
    # The advisory lock also covers CLI and notebook runs in other processes.
    with psycopg.connect(**pg_kwargs(), autocommit=True) as lock:
        if not lock.execute('SELECT pg_try_advisory_lock(824453121)').fetchone()[0]:
            raise PipelineBusy('A retail pipeline run is already active')
        return _run(limit)


def _run(limit):
    full = limit is None
    started = time.time()
    timings = {}
    state = read_json(PIPELINE_STATE, {}) if full else {}
    attempt = {'started_at': started, 'result': 'running', 'stage_seconds': timings}
    stage_name = 'bronze'

    def mark_state():
        if full:
            state['attempt'] = attempt
            write_json(PIPELINE_STATE, state)

    def stage(name, action, *args):
        nonlocal stage_name
        stage_name = name
        attempt['stage'] = name
        mark_state()
        start = time.perf_counter()
        try:
            return action(*args)
        finally:
            timings[name] = round(time.perf_counter() - start, 3)

    spark = None
    mark_state()
    try:
        manifest = stage('bronze', ingest.make_bronze, limit)
        spark = stage('spark_start', lakehouse.spark_session)
        compatibility = stage('compatibility', lakehouse.smoke, spark)
        lake = stage('iceberg', lakehouse.build, spark, manifest)
        stage('postgres_load', warehouse.load, manifest)
        wh = stage('dbt', warehouse.build, manifest)
        checks = stage('reconciliation', reconcile, lake, wh)
        release = stage('publication', publish, lake, wh, checks) if full else None
        permissions = stage('permissions', warehouse.negative_access_test) if full else {}
        result = {
            'scope': manifest['scope'], 'published': full, 'compatibility': compatibility,
            'release_id': release['release_id'] if release else None,
            'input_rows': manifest['rows'], 'accepted_rows': lake['accepted_rows'],
            'quarantined_rows': lake['quarantined_rows'], 'reconciliation': checks,
            'dbt_models': wh['dbt_models'], 'dbt_tests_passed': wh['dbt_tests_passed'],
            'seconds': round(time.time() - started, 2), 'negative_permissions': permissions,
            'stage_seconds': timings,
        }
        write_json(ARTIFACTS/('verification.json' if full else 'sample-verification.json'), result)
        if full:
            state['successful_runs'] = state.get('successful_runs', 0) + 1
            state['last_success'] = result
            metrics = read_json(ARTIFACTS/'metrics.json', {})
            metrics['pipeline_duration_seconds'] = result['seconds']
            write_json(ARTIFACTS/'metrics.json', metrics)
        attempt.update(result='success', release_id=result['release_id'])
        return result
    except Exception as exc:
        state['failed_runs'] = state.get('failed_runs', 0) + 1
        attempt.update(result='failure', stage=stage_name, error=type(exc).__name__)
        raise
    finally:
        attempt['finished_at'] = time.time()
        mark_state()
        if spark is not None:
            spark.stop()
