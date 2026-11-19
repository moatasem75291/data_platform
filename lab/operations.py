"""Exercises against live Iceberg snapshots and the publication gate."""
from decimal import Decimal

from . import lakehouse
from .pipeline import QualityGateError, compare_daily, validate
from .run_state import CURRENT_RELEASE, read_json


def publication_gate_experiment():
    before = CURRENT_RELEASE.read_bytes()
    rows = lakehouse.trino_daily(read_json(CURRENT_RELEASE)['lakehouse'])
    changed = list(rows[0])
    changed[-1] += Decimal('1.000000')
    try:
        compare_daily(rows, [changed, *rows[1:]])
    except QualityGateError:
        pass
    else:
        raise AssertionError('A modified decimal total passed the release gate')
    assert CURRENT_RELEASE.read_bytes() == before
    return {'modified_group_total': 'blocked', 'release_pointer_preserved': True}


def verify_pinned_release(release):
    result = validate(release['lakehouse'])
    if result != release['validation']:
        raise QualityGateError('The retained Iceberg snapshots differ from the saved validation')
    return {'release_id': release['release_id'], 'retained_snapshots': 'readable', **result}
