import json, subprocess, time
from datetime import datetime,timezone
from .config import DATA,ARTIFACTS,pg_kwargs

def backup_restore():
    """Logical backup and restore of published warehouse plus candidate dependencies."""
    import psycopg
    from psycopg import sql
    # Run from the postgres container (CLI tools are deliberately not assumed in notebook).
    return {'command':'docker compose exec -T postgres bash /lab-scripts/restore-drill.sh',
      'evidence':'artifacts/restore-verification.json',
      'note':'Use scripts/lab.cmd restore; it exports evidence from the database container.'}

def publication_gate_experiment():
    import uuid,psycopg
    from psycopg import sql
    from .pipeline import QualityGateError,reconcile,publish
    current=DATA/'releases/current.json'
    before=current.read_bytes()
    release=json.loads(before);schema='failure_demo_'+uuid.uuid4().hex[:10]
    with psycopg.connect(**pg_kwargs()) as conn:
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        conn.execute(sql.SQL('CREATE TABLE {}.daily_sales AS SELECT * FROM {}.daily_sales').format(sql.Identifier(schema),sql.Identifier(release['warehouse']['build_schema'])))
        conn.execute(sql.SQL('UPDATE {}.daily_sales SET net_sales=net_sales+1 WHERE ctid=(SELECT ctid FROM {}.daily_sales LIMIT 1)').format(sql.Identifier(schema),sql.Identifier(schema)))
    injected=dict(release['warehouse'],build_schema=schema)
    try:
        checks=reconcile(release['lakehouse'],injected)
        publish(release['lakehouse'],injected,checks)
    except QualityGateError:
        pass
    else:raise AssertionError('Corrupt candidate was not blocked')
    finally:
        with psycopg.connect(**pg_kwargs()) as conn:
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
    assert current.read_bytes()==before
    return {'injected_bad_total':'blocked','last_valid_release_unchanged':True}

from decimal import Decimal
