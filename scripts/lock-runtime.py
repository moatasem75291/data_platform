import importlib.metadata,json,platform,subprocess
from pathlib import Path
root=Path(__file__).resolve().parents[1]
packages={d.metadata['Name']:d.version for d in importlib.metadata.distributions()}
result={'python':platform.python_version(),'packages':dict(sorted(packages.items())),
 'java':subprocess.run(['java','-version'],text=True,capture_output=True).stderr,
 'jar_sha256':{}}
from hashlib import sha256
for p in Path('/opt/jars').glob('*.jar'):result['jar_sha256'][p.name]=sha256(p.read_bytes()).hexdigest()
(root/'artifacts/runtime-lock.json').write_text(json.dumps(result,indent=2))
print('Runtime versions and JAR checksums recorded in artifacts/runtime-lock.json')
