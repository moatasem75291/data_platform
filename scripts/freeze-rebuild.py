"""Freeze the proven notebook environment and base-image digest for future rebuilds."""
import re,subprocess,sys
from pathlib import Path
root=Path(__file__).resolve().parents[1]
locked=subprocess.check_output([sys.executable,'-m','pip','freeze'],text=True)
(root/'requirements.lock.txt').write_text(locked)
(root/'artifacts/requirements.lock.txt').write_text(locked)
dockerfile=(root/'Dockerfile').read_text()
build_log=root/'artifacts/runtime-final-build.log'
log=build_log.read_text(errors='replace') if build_log.exists() else ''
digests=re.findall(r'python:3\.12-slim-bookworm@sha256:([0-9a-f]{64})',log)
if digests:
    dockerfile=re.sub(r'^FROM python:3\.12-slim-bookworm(?:@sha256:[0-9a-f]+)?',
        'FROM python:3.12-slim-bookworm@sha256:'+digests[-1],dockerfile,flags=re.M)
dockerfile=dockerfile.replace('COPY requirements.in /opt/requirements.in','COPY requirements.in requirements.lock.txt /opt/')
dockerfile=dockerfile.replace('install -r /opt/requirements.in','install -r /opt/requirements.lock.txt')
(root/'Dockerfile').write_text(dockerfile)
print('Frozen exact Python packages and the resolved base image for reproducible rebuilds.')

