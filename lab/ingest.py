"""Immutable source evidence, stable row identity, bounded batches and manifests."""
import hashlib, json, shutil, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
import requests
from .config import DATA, s3_client

URL='https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip'
COLUMNS=['invoice','stock_code','description','quantity','invoice_date','unit_price','customer_id','country','source_row_id','source_sheet','source_row']

def sha256(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''): h.update(block)
    return h.hexdigest()

def download():
    path=DATA/'source/online_retail_II.xlsx'; path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists(): return path
    archive=path.with_suffix('.zip.part')
    with requests.get(URL,stream=True,timeout=(30,180)) as response:
        response.raise_for_status()
        with archive.open('wb') as f:
            for chunk in response.iter_content(1024*1024): f.write(chunk)
    with zipfile.ZipFile(archive) as z:
        member=next(n for n in z.namelist() if n.lower().endswith('.xlsx'))
        with z.open(member) as src, path.with_suffix('.part').open('wb') as dst: shutil.copyfileobj(src,dst)
    path.with_suffix('.part').replace(path); archive.unlink()
    return path

def make_bronze(limit=None):
    import openpyxl, pyarrow as pa, pyarrow.parquet as pq
    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError('Sample limit must be a positive integer')
    source=download(); digest=sha256(source)
    run_id=f'retail-{digest[:12]}-{limit or "full"}'
    dest=DATA/'bronze'/run_id; dest.mkdir(parents=True,exist_ok=True)
    manifest_file=dest/'manifest.json'
    if manifest_file.exists():
        manifest=json.loads(manifest_file.read_text()); validate_manifest(manifest)
        land(source,manifest);return manifest
    candidate_file=dest/'candidate-manifest.json'
    if candidate_file.exists():
        manifest=json.loads(candidate_file.read_text());validate_manifest(manifest)
        land(source,manifest)
        manifest_file.write_text(json.dumps(manifest,indent=2));return manifest
    wb=openpyxl.load_workbook(source,read_only=True,data_only=True)
    schema=pa.schema([(c,pa.string()) for c in COLUMNS])
    rows=[]; files=[]; total=0; part=0
    def flush():
        nonlocal rows,part
        path=dest/f'part-{part:05d}.parquet'
        pq.write_table(pa.Table.from_pylist(rows,schema=schema),path,compression='zstd')
        files.append({'name':path.name,'rows':len(rows),'sha256':sha256(path),'bytes':path.stat().st_size})
        rows=[]; part+=1
    for sheet in wb:
        iterator=sheet.iter_rows(values_only=True); headers=next(iterator)
        normalized=[''.join(ch for ch in str(v).lower() if ch.isalnum()) for v in headers]
        required=['invoice','stockcode','description','quantity','invoicedate','price','customerid','country']
        positions=[normalized.index(h) for h in required]
        for row_number, values in enumerate(iterator,2):
            if limit and total>=limit: break
            record={}
            for key,pos in zip(COLUMNS[:8],positions):
                val=values[pos] if pos<len(values) else None
                record[key]=val.isoformat() if isinstance(val,datetime) else None if val is None else str(val)
            record.update(source_row_id=hashlib.sha256(f'{digest}:{sheet.title}:{row_number}'.encode()).hexdigest(),source_sheet=sheet.title,source_row=str(row_number))
            rows.append(record);total+=1
            if len(rows)>=25000: flush()
        if limit and total>=limit: break
    if rows: flush()
    wb.close()
    manifest={'run_id':run_id,'created_at':datetime.now(timezone.utc).isoformat(),'source_url':URL,
      'source_file':source.name,'source_sha256':digest,'rows':total,'files':files,'columns':COLUMNS,
      'license':'CC BY 4.0','attribution':'Chen, D. (2012). Online Retail II. UCI. DOI:10.24432/C5CG6D',
      'scope':'full' if limit is None else f'first {limit} source rows in workbook order',
      'local_path':f'bronze/{run_id}','s3_prefix':f'bronze/{run_id}'}
    validate_manifest(manifest)
    candidate_file.write_text(json.dumps(manifest,indent=2))
    land(source,manifest)
    temp=manifest_file.with_suffix('.tmp');temp.write_text(json.dumps(manifest,indent=2));temp.replace(manifest_file)
    return manifest

def land(source,manifest):
    from boto3.s3.transfer import TransferConfig
    transfer=TransferConfig(multipart_threshold=256*1024*1024,max_concurrency=2)
    digest=manifest['source_sha256'];dest=DATA/manifest['local_path']
    client=s3_client()
    for attempt in range(30):
        try:
            client.list_buckets();break
        except Exception:
            if attempt==29: raise
            time.sleep(2)
    buckets=[b['Name'] for b in client.list_buckets()['Buckets']]
    for bucket in ['lakehouse','source-evidence']:
        if bucket not in buckets: client.create_bucket(Bucket=bucket)
    source_key=f'{digest}/{source.name}'
    upload_source=True
    try:
        head=client.head_object(Bucket='source-evidence',Key=source_key)
        upload_source=not(head['ContentLength']==source.stat().st_size and head.get('Metadata',{}).get('sha256')==digest)
    except client.exceptions.ClientError as exc:
        if str(exc.response['Error']['Code']) not in ['404','NoSuchKey','NotFound']:raise
    if upload_source:
        client.upload_file(str(source),'source-evidence',source_key,Config=transfer,ExtraArgs={'Metadata':{'sha256':digest}})
    for file in manifest['files']:
        key=f'{manifest["s3_prefix"]}/{file["name"]}'
        try:
            head=client.head_object(Bucket='lakehouse',Key=key)
            if head['ContentLength']==file['bytes'] and head.get('Metadata',{}).get('sha256')==file['sha256']:continue
        except client.exceptions.ClientError as exc:
            if str(exc.response['Error']['Code']) not in ['404','NoSuchKey','NotFound']:raise
        client.upload_file(str(dest/file['name']),'lakehouse',key,Config=transfer,ExtraArgs={'Metadata':{'sha256':file['sha256']}})
    # A manifest is the final commit marker; a folder's existence is never completion.
    client.put_object(Bucket='lakehouse',Key=f'{manifest["s3_prefix"]}/manifest.json',Body=json.dumps(manifest).encode())

def validate_manifest(manifest):
    base=DATA/manifest['local_path']
    assert sum(f['rows'] for f in manifest['files'])==manifest['rows']
    for item in manifest['files']:
        assert sha256(base/item['name'])==item['sha256'],f"Checksum mismatch: {item['name']}"
    return True

def iter_rows(manifest):
    import pyarrow.parquet as pq
    validate_manifest(manifest)
    for file in manifest['files']:
        for batch in pq.ParquetFile(DATA/manifest['local_path']/file['name']).iter_batches(batch_size=10000):
            yield from batch.to_pylist()
