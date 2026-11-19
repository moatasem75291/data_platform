"""Portable launcher; works without changing Windows PowerShell execution policy."""
import argparse,json,os,subprocess,sys,time,webbrowser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
ROOT=Path(__file__).resolve().parents[1]
if os.name=='nt':
    binary=Path(os.environ['LOCALAPPDATA'])/'Programs/DockerDesktop/resources/bin'
    os.environ['PATH']=str(binary)+os.pathsep+os.environ['PATH']
def compose(*args,**kwargs):
    return subprocess.run(['docker','compose',*args],cwd=ROOT,check=True,**kwargs)

CORE = ('postgres', 'seaweedfs', 'notebook', 'trino')
PROFILES = tuple(arg for profile in ('bi', 'cdc', 'orchestration', 'monitoring', 'metadata')
                 for arg in ('--profile', profile))


def in_notebook(*args):
    compose('exec', '-T', 'notebook', 'python', *args)


def wait_http(url, timeout=120):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return
        except (OSError, URLError):
            pass
        time.sleep(2)
    raise RuntimeError(f'Service did not become ready at {url}')


def start_metadata():
    in_notebook('-m', 'lab.catalog_auth', 'prepare')
    compose('--profile', 'metadata', 'up', '-d', '--wait', '--wait-timeout', '300', 'openmetadata')
    in_notebook('-m', 'lab.catalog_auth', 'bootstrap-admin')
    print('OpenMetadata: http://localhost:8585', flush=True)


def cdc_available():
    try:
        with urlopen('http://localhost:18083/connectors/retail-customers/status', timeout=10) as response:
            status = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return False
        raise
    return (status['connector']['state'] == 'RUNNING' and bool(status['tasks'])
            and all(task['state'] == 'RUNNING' for task in status['tasks']))


def start_platform():
    compose(*PROFILES, 'up', '-d', '--build', '--wait', '--wait-timeout', '300', *CORE,
            'superset', 'airflow', 'kafka', 'connect', 'kafka-ui', 'prometheus', 'grafana')
    for endpoint in ('http://localhost:18088/health', 'http://localhost:18080/v1/info',
                     'http://localhost:18081/api/v2/monitor/health', 'http://localhost:18083/connectors',
                     'http://localhost:13000/api/health', 'http://localhost:19090/-/ready'):
        wait_http(endpoint)
    release_file = ROOT/'data/releases/current.json'
    release = json.loads(release_file.read_text()) if release_file.is_file() else {}
    if release.get('engine') != 'lakehouse' or not release.get('validation', {}).get('passed'):
        in_notebook('-m', 'lab.cli', 'run', '--full')
    in_notebook('-m', 'lab.bi')
    if not cdc_available() or not (ROOT/'artifacts/cdc-verification.json').is_file():
        in_notebook('scripts/check-cdc.py')
    start_metadata()
    in_notebook('-m', 'lab.catalog', '--strict')


parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('action',choices=['start','platform','stop','status','sample','full','notebook','notebook-full','notebook-all','bi','bi-refresh','cdc','orchestration','monitoring','metadata','metadata-sync','verify','lock','open'])
args=parser.parse_args()
if args.action=='start':
    if (ROOT/'data/cdc-enabled').exists():compose('--profile','cdc','up','-d','--build',*CORE,'kafka','connect')
    else:compose('up','-d','--build',*CORE)
elif args.action=='platform': start_platform()
elif args.action=='stop':compose(*PROFILES,'stop')
elif args.action=='status':compose(*PROFILES,'ps')
elif args.action in ['sample','full']:
    compose('exec','-T','notebook','python','-m','lab.cli','run',*(['--full'] if args.action=='full' else ['--limit','50000']))
elif args.action.startswith('notebook'):
    flags=[]
    if args.action=='notebook-all':flags+=['-e','LAB_BI=1','-e','LAB_CDC=1']
    compose('exec','-T',*flags,'notebook','jupyter','nbconvert','--to','notebook','--execute','data_platform_architecture_lab.ipynb','--output','data_platform_architecture_lab.executed.ipynb','--ExecutePreprocessor.timeout=1800')
elif args.action=='bi':compose('--profile','bi','up','-d','--build','superset')
elif args.action=='bi-refresh':in_notebook('-m','lab.bi')
elif args.action=='cdc':compose('--profile','cdc','up','-d','kafka','connect','kafka-ui')
elif args.action=='orchestration':compose('--profile','orchestration','up','-d','airflow')
elif args.action=='monitoring':compose('--profile','monitoring','up','-d','prometheus','grafana')
elif args.action=='metadata':start_metadata()
elif args.action=='metadata-sync':in_notebook('-m','lab.catalog')
elif args.action=='verify':compose('exec','-T','notebook','python','-m','lab.cli','exercises')
elif args.action=='lock':
    compose('exec','-T','notebook','python','scripts/lock-runtime.py')
    subprocess.run([sys.executable,str(ROOT/'scripts/lock-images.py')],cwd=ROOT,check=True)
elif args.action=='open':
    values=dict(line.split('=',1) for line in (ROOT/'.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
    webbrowser.open('http://localhost:18888/lab/tree/data_platform_architecture_lab.ipynb?token='+values['JUPYTER_TOKEN'])
    print('Opened the notebook in your default browser.')
