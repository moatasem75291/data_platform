"""Daily full-workbook run with reconciliation and publication checks."""
from datetime import datetime,timedelta,timezone
import json,os,urllib.request
from airflow.sdk import dag,task
@dag(dag_id='retail_learning_pipeline',schedule='@daily',start_date=datetime(2026,11,1,tzinfo=timezone.utc),catchup=False,
     max_active_runs=1,default_args={'retries':2,'retry_delay':timedelta(seconds=30)},tags=['learning'])
def retail_pipeline():
    @task(execution_timeout=timedelta(minutes=30))
    def run_and_validate():
        request=urllib.request.Request('http://notebook:9000/run',data=b'{}',headers={'Authorization':'Bearer '+os.environ['LAB_TOKEN']},method='POST')
        with urllib.request.urlopen(request,timeout=1700) as response:result=json.load(response)
        if (result['scope'] != 'full' or not result['published'] or not result['release_id']
                or not result['reconciliation']['exact_decimal_match']
                or result['accepted_rows'] + result['quarantined_rows'] != result['input_rows']):
            raise ValueError('The full workbook was not verified and published')
        return {key: result[key] for key in ('release_id', 'input_rows', 'accepted_rows', 'quarantined_rows')}
    run_and_validate()
retail_pipeline()
