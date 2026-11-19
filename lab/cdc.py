"""Real PostgreSQL logical CDC; bounded consumption commits only after bronze persistence."""
import json,os,time,uuid
from pathlib import Path
import psycopg,requests
from psycopg import sql
from confluent_kafka import Consumer
from .config import DATA,ARTIFACTS,pg_kwargs,s3_client

def configure_connector():
    with psycopg.connect(**pg_kwargs(database='source_demo'),autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='lab_cdc'").fetchone():
            conn.execute(sql.SQL('CREATE ROLE lab_cdc LOGIN REPLICATION PASSWORD {}').format(sql.Literal(os.environ['CDC_PASSWORD'])))
        conn.execute('''CREATE TABLE IF NOT EXISTS public.customers (id BIGINT PRIMARY KEY,name TEXT NOT NULL,segment TEXT NOT NULL)''')
        conn.execute('ALTER TABLE public.customers REPLICA IDENTITY FULL')
        conn.execute('CREATE TABLE IF NOT EXISTS public.lab_heartbeat (id INTEGER PRIMARY KEY,updated_at TIMESTAMPTZ NOT NULL)')
        conn.execute("INSERT INTO public.customers VALUES (1,'SYNTHETIC customer one','new'),(2,'SYNTHETIC customer two','new') ON CONFLICT DO NOTHING")
        conn.execute('GRANT CONNECT ON DATABASE source_demo TO lab_cdc')
        conn.execute('GRANT USAGE ON SCHEMA public TO lab_cdc')
        conn.execute('GRANT SELECT ON public.customers TO lab_cdc')
        conn.execute('GRANT SELECT,INSERT,UPDATE ON public.lab_heartbeat TO lab_cdc')
        if not conn.execute("SELECT 1 FROM pg_publication WHERE pubname='lab_publication'").fetchone():
            conn.execute('CREATE PUBLICATION lab_publication FOR TABLE public.customers')
        if not conn.execute("SELECT 1 FROM pg_publication_tables WHERE pubname='lab_publication' AND tablename='lab_heartbeat'").fetchone():
            conn.execute('ALTER PUBLICATION lab_publication ADD TABLE public.lab_heartbeat')
    body={'connector.class':'io.debezium.connector.postgresql.PostgresConnector',
      'database.hostname':os.getenv('PGHOST','postgres'),'database.port':str(pg_kwargs()['port']),
      'database.user':'lab_cdc','database.password':os.environ['CDC_PASSWORD'],
      'database.dbname':'source_demo','topic.prefix':'labcdc','plugin.name':'pgoutput',
      'slot.name':'lab_slot','publication.name':'lab_publication','publication.autocreate.mode':'disabled',
      'table.include.list':'public.customers','snapshot.mode':'initial',
      'heartbeat.interval.ms':'10000',
      'heartbeat.action.query':'INSERT INTO public.lab_heartbeat(id,updated_at) VALUES (1,now()) ON CONFLICT(id) DO UPDATE SET updated_at=EXCLUDED.updated_at',
      'key.converter':'org.apache.kafka.connect.json.JsonConverter','key.converter.schemas.enable':'false',
      'value.converter':'org.apache.kafka.connect.json.JsonConverter','value.converter.schemas.enable':'false'}
    base=os.getenv('DEBEZIUM_URL','http://connect:8083')
    response=requests.put(base+'/connectors/retail-customers/config',json=body,timeout=30);response.raise_for_status()
    (DATA/'cdc-enabled').write_text('Resume Kafka/Connect with the core lab to keep source offsets advancing.\n')
    for _ in range(60):
        status=requests.get(base+'/connectors/retail-customers/status',timeout=10).json()
        states=[t['state'] for t in status.get('tasks',[])]
        if states and all(s=='RUNNING' for s in states):return {'connector':'RUNNING','tasks':states}
        if 'FAILED' in states:raise RuntimeError('CDC connector failed; inspect Docker connect logs without exposing credentials.')
        time.sleep(2)
    raise TimeoutError('CDC tasks did not become ready')

def mutate_source():
    with psycopg.connect(**pg_kwargs(database='source_demo')) as conn:
        conn.execute("UPDATE public.customers SET segment='loyal' WHERE id=1")
        conn.execute("INSERT INTO public.customers VALUES (3,'SYNTHETIC customer three','new') ON CONFLICT(id) DO UPDATE SET segment='new'")
    with psycopg.connect(**pg_kwargs(database='source_demo')) as conn:
        conn.execute('DELETE FROM public.customers WHERE id=3')
    return 'Source changes committed: update 1, insert/update 3, delete 3'

def consume_bronze(seconds=15):
    consumer=Consumer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','kafka:9092'),
      'group.id':'learning-bronze-v1','auto.offset.reset':'earliest','enable.auto.commit':False})
    consumer.subscribe(['labcdc.public.customers']);events=[];messages=[];deadline=time.monotonic()+seconds
    try:
        while time.monotonic()<deadline:
            message=consumer.poll(1)
            if message is None:continue
            if message.error():raise RuntimeError(str(message.error()))
            if message.value() is None:
                # Kafka compaction tombstones carry no Debezium row payload; the preceding op=d is retained.
                messages.append(message);continue
            payload=json.loads(message.value());payload=payload.get('payload',payload)
            events.append({'event_id':f'{message.topic()}:{message.partition()}:{message.offset()}',
              'topic':message.topic(),'partition':message.partition(),'offset':message.offset(),'payload':payload})
            messages.append(message)
        if not messages:return {'events':0,'path':None}
        path=DATA/'bronze/cdc'/uuid.uuid4().hex;path.mkdir(parents=True)
        data=''.join(json.dumps(e)+'\n' for e in events).encode()
        from hashlib import sha256
        (path/'events.jsonl').write_bytes(data)
        manifest={'events':len(events),'sha256':sha256(data).hexdigest(),'event_ids':[e['event_id'] for e in events]}
        client=s3_client();prefix='bronze/cdc/'+path.name
        client.put_object(Bucket='lakehouse',Key=prefix+'/events.jsonl',Body=data)
        client.put_object(Bucket='lakehouse',Key=prefix+'/manifest.json',Body=json.dumps(manifest).encode())
        (path/'manifest.json').write_text(json.dumps(manifest,indent=2))
        consumer.commit(asynchronous=False)
        return {'events':len(events),'path':str(path),'bronze_before_offset_commit':True}
    finally:consumer.close()

def apply_to_iceberg(spark):
    records=[];seen=set()
    for path in sorted((DATA/'bronze/cdc').glob('*/events.jsonl')):
        for line in path.read_text().splitlines():
            event=json.loads(line)
            if event['event_id'] in seen:continue
            seen.add(event['event_id']);p=event['payload'];row=p.get('after') or p.get('before')
            if not row:continue
            records.append((int(row['id']),row['name'],row['segment'],int(p['source'].get('lsn') or 0),int(event['offset']),p['op']=='d'))
    if not records:raise AssertionError('No CDC row events landed')
    frame=spark.createDataFrame(records,'id long,name string,segment string,source_lsn long,kafka_offset long,is_deleted boolean')
    frame.createOrReplaceTempView('cdc_events')
    spark.sql('''CREATE TABLE IF NOT EXISTS lake.silver.customer_state (
      id BIGINT,name STRING,segment STRING,source_lsn BIGINT,kafka_offset BIGINT,is_deleted BOOLEAN)
      USING iceberg TBLPROPERTIES ('format-version'='2')''')
    spark.sql('''MERGE INTO lake.silver.customer_state t USING (
      SELECT id,name,segment,source_lsn,kafka_offset,is_deleted FROM (
      SELECT *,row_number() OVER(PARTITION BY id ORDER BY source_lsn DESC,kafka_offset DESC) rn FROM cdc_events) WHERE rn=1) s
      ON t.id=s.id
      WHEN MATCHED AND (s.source_lsn>t.source_lsn OR (s.source_lsn=t.source_lsn AND s.kafka_offset>t.kafka_offset)) THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *''')
    state=[r.asDict() for r in spark.sql('SELECT * FROM lake.silver.customer_state ORDER BY id').collect()]
    by_id={r['id']:r for r in state}
    assert by_id[1]['segment']=='loyal'
    assert by_id[3]['is_deleted']
    result={'unique_source_events':len(records),'state':state,'delete_tombstone_retained':True,
      'ordering_scope':'Single Kafka partition in this lab; integer source LSN with offset tie-break. Not a universal multi-partition order.'}
    (ARTIFACTS/'cdc-verification.json').write_text(json.dumps(result,indent=2))
    return result
