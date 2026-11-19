"""Create a portable project without local credentials, source data or database dumps."""
import json,zipfile,base64,hashlib
from pathlib import Path
root=Path(__file__).resolve().parents[1]
excluded_parts={'data','docker-data','__pycache__','.ipynb_checkpoints','.venv','.git','logs','target'}
excluded_names={'.env','seaweedfs.json','iceberg.properties','notebook_template.ipynb'}
allowed_evidence={
 'verification.json','notebook-verification.json','runtime-lock.json','images.lock.json','requirements.lock.txt',
 'cdc-verification.json','bi-verification.json','orchestration-verification.json',
 'monitoring-verification.json','openmetadata-verification.json','sample-verification.json',
 'health-verification.json','unit-tests.txt','notebook-validation.json','daily-sales.png'}
output=root/'artifacts/data_platform_learning_lab.zip'
files=[]
secrets=[]
for line in (root/'.env').read_text().splitlines():
    if '=' in line:
        key,value=line.split('=',1)
        if any(word in key for word in ['PASSWORD','SECRET','TOKEN']) and len(value)>=16:secrets.append(value)
with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(root.rglob('*')):
        relative=path.relative_to(root)
        if not path.is_file() or any(p in excluded_parts for p in relative.parts) or path.name in excluded_names:continue
        if path.suffix in ['.exe','.b64','.zip','.dump','.ps1']:continue
        if relative.parts[0]=='artifacts' and path.name not in allowed_evidence:continue
        if path.name.endswith('.executed.ipynb') and (root/'data_platform_architecture_lab.ipynb').exists():continue
        if path.suffix in ['.py','.md','.json','.ipynb','.html','.yaml','.yml','.txt','.in','.properties','.env','.cmd','.sh']:
            body=path.read_text(errors='replace')
            assert not any(secret in body for secret in secrets),f'Credential found in export candidate: {relative}'
        archive.write(path,relative);files.append(str(relative))
encoded=base64.b64encode(output.read_bytes()).decode()
(root/'artifacts/deliverable-transfer.txt').write_text('\n'.join(encoded[i:i+100] for i in range(0,len(encoded),100)),encoding='ascii')
print(json.dumps({'archive':str(output),'bytes':output.stat().st_size,'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'files':len(files),'base64_lines':(len(encoded)+99)//100}))
