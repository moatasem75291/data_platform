"""Read back search, lineage, governance and quality results in OpenMetadata."""
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lab.catalog import millis
from lab.catalog_auth import local_api
from lab.config import ARTIFACTS
from lab.run_state import CATALOG_STATE, CURRENT_RELEASE, read_json, write_json


def verify():
    release = read_json(CURRENT_RELEASE)
    evidence = read_json(CATALOG_STATE)
    assert evidence['result'] == 'success' and not evidence['missing_optional_sources']
    assert evidence['release_id'] == release['release_id']
    expected_tables = {evidence['warehouse_daily_sales'], evidence['iceberg_daily_sales']}
    with local_api() as api:
        indexed = set()
        for _ in range(8):
            result = api.get('search/query', {'q': 'daily_sales', 'index': 'table_search_index', 'size': 25})
            indexed = {item['_source']['fullyQualifiedName'] for item in result['hits']['hits']}
            if expected_tables <= indexed:
                break
            time.sleep(1)
        assert expected_tables <= indexed, indexed
        verified_tables = {}
        for name in sorted(expected_tables):
            table = api.by_name('tables', name, {'fields': 'owners,tags,domains,dataProducts'})
            assert any(item['name'] == 'RetailDataStewards' for item in table['owners'])
            assert any(item['name'] == 'RetailAnalytics' for item in table['domains'])
            assert any(item['tagFQN'] == 'RetailAccess.Published' for item in table['tags'])
            assert any(item['name'] == 'RetailSales' for item in table['dataProducts'])
            lineage = api.get(f"lineage/table/{table['id']}", {'upstreamDepth': 3, 'downstreamDepth': 2})
            assert lineage['upstreamEdges'] and lineage['downstreamEdges'], name
            verified_tables[name] = {'columns': len(table['columns']), 'upstream_edges': len(lineage['upstreamEdges']),
                                     'downstream_edges': len(lineage['downstreamEdges'])}
        product = api.by_name('dataProducts', evidence['data_product'], {'fields': 'owners,domains'})
        assert product['name'] == 'RetailSales' and product['owners'] and product['domains']
        stamp = millis(datetime.fromisoformat(release['published_at']))
        test_results = {}
        for name in evidence['quality_tests']:
            path = f'dataQuality/testCases/testCaseResults/{quote(name, safe="")}'
            results = api.get(path, {'startTimestamp': stamp-1, 'endTimestamp': stamp+1})['data']
            match = [result for result in results if result['timestamp'] == stamp and result['testCaseStatus'] == 'Success']
            assert len(match) == 1, name
            test_results[name] = match[0]['result']
    result = {**evidence, 'verified_search_and_lineage': verified_tables, 'verified_quality_results': test_results}
    write_json(ARTIFACTS/'openmetadata-verification.json', result)
    return result


if __name__ == '__main__':
    print(json.dumps(verify(), indent=2))
