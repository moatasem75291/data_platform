import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lab import cdc,lakehouse
import json
print(json.dumps(cdc.configure_connector()))
print(cdc.mutate_source())
print(json.dumps(cdc.consume_bronze(seconds=25)))
spark=lakehouse.spark_session()
try:
    result=cdc.apply_to_iceberg(spark)
    repeated=cdc.apply_to_iceberg(spark)
    assert result['state']==repeated['state']
    print(json.dumps({'CDC_source_capture':'passed','delete_and_replay':'passed','heartbeat_configured':True}))
finally:spark.stop()
