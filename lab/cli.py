import argparse,json
from .pipeline import run
parser=argparse.ArgumentParser()
parser.add_argument('action',choices=['run','download','exercises'])
parser.add_argument('--full',action='store_true')
parser.add_argument('--limit',type=int,help='Run a debug slice without publishing')
args=parser.parse_args()
if args.action=='run':result=run(None if args.full else args.limit)
elif args.action=='download':
    from .ingest import make_bronze
    result=make_bronze(None if args.full else args.limit)
else:
    from .exercises import replay_experiment,scd2_experiment,temporal_features,governed_retrieval
    result={'replay':replay_experiment(),'scd2':scd2_experiment(),'features':temporal_features(),'retrieval':governed_retrieval()}
print(json.dumps(result,indent=2,default=str))
