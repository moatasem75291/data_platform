"""Small, deterministic correctness experiments independent of the live dataset."""
from datetime import datetime, timezone, timedelta
from decimal import Decimal

def apply_ordered_events(events,state=None):
    state=dict(state or {});seen={}
    for event in events:
        key=event['id'];version=event['source_seq'];token=(key,version)
        if token in seen and seen[token]!=event: raise ValueError('Conflicting payload for same source version')
        seen[token]=event
        old=state.get(key)
        if old is None or version>old['source_seq']:state[key]=dict(event)
        elif version==old['source_seq'] and event!=old:raise ValueError('Conflicting payload for same source version')
    return state

def replay_experiment():
    events=[{'id':1,'source_seq':1,'deleted':False,'amount':'10.00'},
      {'id':1,'source_seq':3,'deleted':True,'amount':None},
      {'id':1,'source_seq':2,'deleted':False,'amount':'20.00'}]
    once=apply_ordered_events(events);twice=apply_ordered_events(events,once)
    assert once==twice and once[1]['deleted']
    try:apply_ordered_events([events[0],dict(events[0],amount='99')])
    except ValueError: conflict='blocked'
    else:raise AssertionError('Conflict not blocked')
    return {'duplicate_replay':'passed','late_update_after_delete':'passed','same_version_conflict':conflict,'retained_tombstone':once[1]}

def scd2_experiment():
    import psycopg
    from .config import pg_kwargs
    with psycopg.connect(**pg_kwargs()) as conn:
        conn.execute('CREATE SCHEMA IF NOT EXISTS learning')
        conn.execute('DROP TABLE IF EXISTS learning.customer_history')
        conn.execute('''CREATE TABLE learning.customer_history (
          customer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          customer_id TEXT NOT NULL, segment TEXT NOT NULL,
          valid_from timestamptz NOT NULL,valid_to timestamptz,
          CHECK(valid_to IS NULL OR valid_to>valid_from))''')
        conn.execute("INSERT INTO learning.customer_history(customer_id,segment,valid_from,valid_to) VALUES ('SYNTH-1','new','2010-01-01','2010-06-01'),('SYNTH-1','loyal','2010-06-01',NULL)")
        rows=conn.execute("SELECT segment FROM learning.customer_history WHERE customer_id='SYNTH-1' AND valid_from<='2010-06-01' AND (valid_to>'2010-06-01' OR valid_to IS NULL)").fetchall()
        assert rows==[('loyal',)]
        overlap=conn.execute('''SELECT count(*) FROM learning.customer_history a JOIN learning.customer_history b
          ON a.customer_id=b.customer_id AND a.customer_sk<b.customer_sk
          AND tstzrange(a.valid_from,a.valid_to,'[)') && tstzrange(b.valid_from,b.valid_to,'[)')''').fetchone()[0]
        assert overlap==0
    return {'half_open_interval_join':'passed','overlapping_versions':overlap,'data_origin':'synthetic, not observed retail customer history'}

def temporal_features():
    import pandas as pd
    observations=pd.DataFrame([
      {'event_time':'2010-01-01','available_at':'2010-01-01','amount':10},
      {'event_time':'2010-01-02','available_at':'2010-01-10','amount':90},
      {'event_time':'2010-01-11','available_at':'2010-01-11','amount':200}])
    for c in ['event_time','available_at']: observations[c]=pd.to_datetime(observations[c],utc=True)
    prediction_time=pd.Timestamp('2010-01-05',tz='UTC')
    wrong=observations.loc[observations.event_time<prediction_time,'amount'].sum()
    correct=observations.loc[(observations.event_time<prediction_time)&(observations.available_at<=prediction_time),'amount'].sum()
    assert int(wrong)==100 and int(correct)==10
    return {'event_time_only_leaky_feature':int(wrong),'point_in_time_feature':int(correct),'late_arriving_record_excluded':True,'origin':'synthetic'}

def governed_retrieval():
    docs=[{'id':'public-1','tenant':'public','version':1,'text':'Daily sales are published after reconciliation.'},
      {'id':'finance-1','tenant':'finance','version':2,'text':'Restricted internal margin assumptions.'}]
    def retrieve(tenant):return [d for d in docs if d['tenant'] in ['public',tenant]]
    public=retrieve('visitor');finance=retrieve('finance')
    assert [d['id'] for d in public]==['public-1']
    assert len(finance)==2
    return {'public_prompt_document_ids':[d['id'] for d in public], 'finance_prompt_document_ids':[d['id'] for d in finance],
      'authorization_before_prompt':'passed','scope':'toy retrieval boundary; does not certify an enterprise RAG system'}

def cost_model(storage_gib,storage_rate,queries_per_day,gib_per_query,scan_rate_per_tib,days=30):
    scan_tib=Decimal(str(queries_per_day))*Decimal(str(gib_per_query))*Decimal(days)/Decimal(1024)
    return {'monthly_scan_tib':str(scan_tib),'storage_cost':str(Decimal(str(storage_gib))*Decimal(str(storage_rate))),
      'scan_cost':str(scan_tib*Decimal(str(scan_rate_per_tib))), 'note':'User-supplied tariffs; excludes other services and operations labor.'}
