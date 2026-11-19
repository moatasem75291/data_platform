"""Provision and verify the editable Superset retail dashboards."""
import json
import os
from decimal import Decimal

import requests

from .config import ARTIFACTS
from .run_state import CURRENT_RELEASE, read_json, write_json


SUPERSET_URL = os.getenv('SUPERSET_URL', 'http://superset:8088').rstrip('/')
SUPERSET_PUBLIC_URL = os.getenv('SUPERSET_PUBLIC_URL', 'http://localhost:18088').rstrip('/')
DATABASE_NAME = 'LearningWarehouse'
DAILY = 'daily_sales'
FACT = 'fact_invoice_line'


def superset_session(write=False):
    session = requests.Session()
    response = session.post(f'{SUPERSET_URL}/api/v1/security/login', timeout=30, json={
        'username': 'admin', 'password': os.environ['SUPERSET_ADMIN_PASSWORD'], 'provider': 'db', 'refresh': False,
    })
    response.raise_for_status()
    session.headers['Authorization'] = 'Bearer ' + response.json()['access_token']
    if write:
        csrf = session.get(f'{SUPERSET_URL}/api/v1/security/csrf_token/', timeout=30)
        csrf.raise_for_status()
        session.headers.update({'X-CSRFToken': csrf.json()['result'], 'Referer': SUPERSET_URL+'/'})
    return session


def request(session, method, path, body=None, params=None):
    response = session.request(method, f'{SUPERSET_URL}/api/v1/{path}', json=body, params=params, timeout=90)
    if not response.ok:
        detail = '' if path.startswith(('database', 'security/')) else response.text[:500]
        raise RuntimeError(f'Superset {method} {path} returned {response.status_code}. {detail}')
    return response.json()


def listing(session, collection, filter_field, value):
    query = f'(filters:!((col:{filter_field},opr:eq,value:{value})),page_size:100)'
    return request(session, 'GET', f'{collection}/', params={'q': query})['result']


def save(session, entity, body, entity_id=None):
    if entity_id is None:
        return request(session, 'POST', entity+'/', body)['id']
    request(session, 'PUT', f'{entity}/{entity_id}', body)
    return entity_id


def metric(label, sql, identifier):
    return {'expressionType': 'SQL', 'sqlExpression': sql, 'label': label,
            'optionName': f'metric_{identifier}', 'hasCustomLabel': True}


GROSS = metric('Gross sales (GBP)', 'SUM(gross_sales)', 'gross_gbp')
NET = metric('Signed net sales (GBP)', 'SUM(net_sales)', 'net_gbp')
CREDIT = metric('Credits (GBP)', 'SUM(credits)', 'credit_gbp')
LINES = metric('Accepted lines', 'SUM(line_count)', 'lines')
CREDIT_SHARE = metric('Credits / gross (%)', '100.0 * SUM(credits) / NULLIF(SUM(gross_sales), 0)', 'credit_share')
PURCHASE = 'quantity > 0 AND NOT is_cancellation'
KNOWN = "customer_key <> 'UNKNOWN'"
ORDERS = f'COUNT(DISTINCT CASE WHEN {PURCHASE} THEN invoice END)'
CUSTOMERS = f'COUNT(DISTINCT CASE WHEN {PURCHASE} AND {KNOWN} THEN customer_key END)'
KNOWN_ORDERS = f'COUNT(DISTINCT CASE WHEN {PURCHASE} AND {KNOWN} THEN invoice END)'
ORDER_COUNT = metric('Purchase invoices', ORDERS, 'purchase_invoices')
KNOWN_CUSTOMERS = metric('Known purchasing customers', CUSTOMERS, 'known_customers')
STOCK_CODES = metric('Purchased stock codes', f'COUNT(DISTINCT CASE WHEN {PURCHASE} THEN stock_code END)', 'stock_codes')
AVG_ORDER = metric('Average purchase invoice (GBP)',
                   f'SUM(CASE WHEN {PURCHASE} THEN line_amount ELSE 0 END) / NULLIF({ORDERS}, 0)', 'avg_invoice')
FREQUENCY = metric('Purchase invoices per known customer', f'1.0 * {KNOWN_ORDERS} / NULLIF({CUSTOMERS}, 0)', 'purchase_frequency')
PRODUCT_NET = metric('Signed net sales (GBP)', 'SUM(line_amount)', 'product_net')
PURCHASE_UNITS = metric('Purchased units', f'SUM(CASE WHEN {PURCHASE} THEN quantity ELSE 0 END)', 'purchase_units')
NEGATIVE = metric('Negative-amount lines', 'COUNT(*) FILTER (WHERE line_amount < 0)', 'negative_lines')
CANCELLATIONS = metric('Cancellation-flagged lines', 'COUNT(*) FILTER (WHERE is_cancellation)', 'cancellations')
UNKNOWN = metric('Lines with unknown customer', f'COUNT(*) FILTER (WHERE NOT ({KNOWN}))', 'unknown')
UNKNOWN_SHARE = metric('Unknown-customer lines (%)', f'100.0 * ({UNKNOWN["sqlExpression"]}) / NULLIF(COUNT(*), 0)', 'unknown_share')
ROW_COUNT = metric('Accepted lines', 'COUNT(*)', 'accepted_lines')
FIRST = metric('First invoice date', 'MIN(invoice_date)', 'first_date')
LAST = metric('Last invoice date', 'MAX(invoice_date)', 'last_date')


def chart(title, dataset, kind, metrics, description, columns=(), limit=100):
    return dict(title=title, dataset=dataset, kind=kind, metrics=metrics, description=description,
                columns=list(columns), limit=limit)


DASHBOARDS = {
    'retail-overview': ('Retail Sales | Overview and Markets', [
        [chart('Total gross sales', DAILY, 'kpi', [GROSS], 'Positive amounts on lines without a cancellation flag.'),
         chart('Total signed net sales', DAILY, 'kpi', [NET], 'Signed quantity times unit price in GBP for all accepted lines.'),
         chart('Total credits', DAILY, 'kpi', [CREDIT], 'Absolute value of negative line amounts.'),
         chart('Total accepted invoice lines', DAILY, 'kpi', [LINES], 'Valid workbook observations, including cancellations.')],
        [chart('Monthly sales and credits', DAILY, 'line', [GROSS, NET, CREDIT], 'Business month in Europe/London. The source begins and ends partway through calendar years.')],
        [chart('Market sales breakdown', DAILY, 'table', [NET, GROSS, CREDIT, CREDIT_SHARE, LINES],
               'Largest 25 observed markets ranked by signed sales.', ['country'], 25),
         chart('Monthly accepted invoice lines', DAILY, 'bar', [LINES], 'Counts accepted invoice lines by business month.')],
    ]),
    'retail-products-customers': ('Retail Sales | Products and Customers', [
        [chart('Purchase invoices', FACT, 'kpi', [ORDER_COUNT], 'Distinct invoice codes on positive, non-cancellation lines.'),
         chart('Known purchasing customers', FACT, 'kpi', [KNOWN_CUSTOMERS], 'Distinct known customer keys on positive purchases.'),
         chart('Purchased stock codes', FACT, 'kpi', [STOCK_CODES], 'Distinct source product codes on positive purchases.'),
         chart('Average purchase invoice value', FACT, 'kpi', [AVG_ORDER], 'Positive purchase value divided by distinct purchase invoices.')],
        [chart('Monthly known purchasing customers', FACT, 'line', [KNOWN_CUSTOMERS], 'Distinct known purchasing customers per calendar month.'),
         chart('Monthly purchase invoices', FACT, 'bar', [ORDER_COUNT], 'Distinct qualifying invoices per calendar month.')],
        [chart('Product leaders by signed sales', FACT, 'table', [PRODUCT_NET, PURCHASE_UNITS, ORDER_COUNT],
               'Largest 30 source stock codes. Codes can include fees and other adjustments.', ['stock_code'], 30),
         chart('Purchasing customers by market', FACT, 'table', [KNOWN_CUSTOMERS, ORDER_COUNT, FREQUENCY, AVG_ORDER],
               'Largest 25 markets by known purchasing customers. Purchase frequency uses known customers and their distinct invoices.', ['country'], 25)],
    ]),
    'retail-returns-coverage': ('Retail Sales | Credits and Data Coverage', [
        [chart('Negative-amount invoice lines', FACT, 'kpi', [NEGATIVE], 'Line amounts less than zero.'),
         chart('Cancellation-flagged invoice lines', FACT, 'kpi', [CANCELLATIONS], 'Invoice prefix C or negative quantity.'),
         chart('Unidentified customer lines', FACT, 'kpi', [UNKNOWN], 'Accepted lines that have no known customer ID.'),
         chart('Unidentified customer line share', FACT, 'kpi', [UNKNOWN_SHARE], 'Percent of accepted lines with an unknown customer.')],
        [chart('Monthly negative and cancellation lines', FACT, 'line', [NEGATIVE, CANCELLATIONS], 'Counts by business month. The two groups can overlap.'),
         chart('Monthly unidentified customer share', FACT, 'line', [UNKNOWN_SHARE], 'Unknown customer lines as a percent of accepted lines by business month.')],
        [chart('Credits by market', DAILY, 'table', [CREDIT, CREDIT_SHARE, GROSS, NET],
               'Largest 25 markets ranked by value of credits.', ['country'], 25),
         chart('Coverage by workbook sheet', FACT, 'table', [ROW_COUNT, FIRST, LAST, UNKNOWN],
               'Accepted observations and date ranges for both original sheets. Their December 2010 overlap is retained.', ['source_sheet'])],
    ]),
}

VIZ_TYPES = {'kpi': 'big_number_total', 'table': 'table', 'line': 'echarts_timeseries_line', 'bar': 'echarts_timeseries_bar'}


def chart_body(spec, dataset_id, dashboard_id):
    viz = VIZ_TYPES[spec['kind']]
    metrics = spec['metrics']
    base = {'datasource': f'{dataset_id}__table', 'viz_type': viz, 'adhoc_filters': [], 'time_range': 'No filter',
            'row_limit': spec['limit'], 'color_scheme': 'supersetColors'}
    query = {'filters': [], 'extras': {}, 'granularity': 'invoice_date', 'time_range': 'No filter',
             'metrics': metrics, 'columns': spec['columns'], 'row_limit': spec['limit']}
    if spec['kind'] == 'kpi':
        base.update(metric=metrics[0], header_font_size=0.4, y_axis_format=',.2f' if 'GBP' in metrics[0]['label'] or '%' in metrics[0]['label'] else ',.0f')
    elif spec['kind'] == 'table':
        base.update(groupby=spec['columns'], metrics=metrics, query_mode='aggregate', order_desc=True,
                    timeseries_limit_metric=metrics[0], include_search=True, page_length=15)
        query['orderby'] = [[metrics[0], False]]
    else:
        base.update(x_axis='invoice_date', granularity_sqla='invoice_date', time_grain_sqla='P1M',
                    groupby=[], metrics=metrics, show_legend=len(metrics)>1, rich_tooltip=True)
        query['columns'] = [{'expressionType': 'SQL', 'sqlExpression': 'invoice_date',
                             'columnType': 'BASE_AXIS', 'timeGrain': 'P1M', 'label': 'invoice_date'}]
        query['extras'] = {'time_grain_sqla': 'P1M'}
        query['is_timeseries'] = True
    context = {'datasource': {'id': dataset_id, 'type': 'table'}, 'force': False, 'queries': [query],
               'form_data': base, 'result_format': 'json', 'result_type': 'full'}
    return {'slice_name': spec['title'], 'viz_type': viz, 'description': spec['description'],
            'datasource_id': dataset_id, 'datasource_type': 'table', 'dashboards': [dashboard_id],
            'params': json.dumps(base), 'query_context': json.dumps(context)}


def dashboard_body(title, slug, rows, daily_id):
    grid = [f'ROW-{i}' for i in range(len(rows))]
    layout = {'DASHBOARD_VERSION_KEY': 'v2',
              'ROOT_ID': {'type': 'ROOT', 'id': 'ROOT_ID', 'children': ['GRID_ID']},
              'GRID_ID': {'type': 'GRID', 'id': 'GRID_ID', 'children': grid, 'parents': ['ROOT_ID']}}
    for row_id, row in zip(grid, rows):
        parents = ['ROOT_ID', 'GRID_ID']
        layout[row_id] = {'type': 'ROW', 'id': row_id, 'children': [f'CHART-{chart_id}' for chart_id, _ in row],
                          'parents': parents, 'meta': {'background': 'BACKGROUND_TRANSPARENT'}}
        for chart_id, spec in row:
            key = f'CHART-{chart_id}'
            layout[key] = {'type': 'CHART', 'id': key, 'children': [], 'parents': [*parents, row_id],
                           'meta': {'chartId': chart_id, 'sliceName': spec['title'], 'width': 12//len(row),
                                    'height': 17 if spec['kind'] == 'kpi' else 45}}
    filters = []
    for name, kind, target, controls in [
        ('Country', 'filter_select', {'datasetId': daily_id, 'column': {'name': 'country'}}, {'multiSelect': True, 'searchAllOptions': True}),
        ('Invoice date', 'filter_time', {}, {}),
    ]:
        filters.append({'id': 'NATIVE_FILTER-'+name.replace(' ', '-'), 'name': name, 'filterType': kind,
                        'controlValues': {'enableEmptyFilter': False, **controls}, 'targets': [target],
                        'defaultDataMask': {'extraFormData': {}, 'filterState': {}, 'ownState': {}},
                        'cascadeParentIds': [], 'scope': {'rootPath': ['ROOT_ID'], 'excluded': []}, 'type': 'NATIVE_FILTER'})
    return {'dashboard_title': title, 'slug': slug, 'published': True, 'position_json': json.dumps(layout),
            'json_metadata': json.dumps({'native_filter_configuration': filters, 'refresh_frequency': 0})}


def provision():
    release = read_json(CURRENT_RELEASE, {})
    if not release.get('lakehouse', {}).get('input_run_id', '').endswith('-full'):
        raise RuntimeError('Publish a full workbook release before building the retail dashboards.')
    with superset_session(write=True) as session:
        databases = listing(session, 'database', 'database_name', DATABASE_NAME)
        db_id = databases[0]['id'] if databases else save(session, 'database', {
            'database_name': DATABASE_NAME, 'expose_in_sqllab': True,
            'sqlalchemy_uri': f"postgresql+psycopg2://bi_reader:{os.environ['READER_PASSWORD']}@postgres:5432/warehouse",
        })
        datasets = {}
        for table in (DAILY, FACT):
            matches = [item for item in listing(session, 'dataset', 'table_name', table)
                       if item['database']['id'] == db_id and item['schema'] == 'mart']
            datasets[table] = matches[0]['id'] if matches else save(session, 'dataset', {
                'database': db_id, 'schema': 'mart', 'table_name': table,
            })
        all_charts = request(session, 'GET', 'chart/', params={'q': '(page_size:100)'})['result']
        all_dashboards = request(session, 'GET', 'dashboard/', params={'q': '(page_size:100)'})['result']
        results = []
        for slug, (title, rows) in DASHBOARDS.items():
            dashboard = next((item for item in all_dashboards if item.get('slug') == slug), None)
            dashboard_id = dashboard['id'] if dashboard else save(session, 'dashboard', {
                'dashboard_title': title, 'slug': slug, 'published': False,
            })
            chart_rows = []
            for row in rows:
                ids = []
                for spec in row:
                    dataset_id = datasets[spec['dataset']]
                    old = next((item for item in all_charts if item['slice_name'] == spec['title']
                                and item['datasource_id'] == dataset_id), None)
                    body = chart_body(spec, dataset_id, dashboard_id)
                    chart_id = save(session, 'chart', body, old['id'] if old else None)
                    ids.append((chart_id, spec))
                    context = json.loads(body['query_context'])
                    context['force'] = True
                    data = request(session, 'POST', 'chart/data', context)['result'][0]['data']
                    if not data:
                        raise RuntimeError(f"Chart {spec['title']} returned no rows")
                    if spec['title'] == 'Total accepted invoice lines' and data[0][LINES['label']] != release['lakehouse']['accepted_rows']:
                        raise RuntimeError('Superset accepted-line KPI differs from the full release')
                    if spec['title'] == 'Total signed net sales' and Decimal(str(data[0][NET['label']])) != Decimal(release['warehouse']['net_sales']):
                        raise RuntimeError('Superset signed-sales KPI differs from the full release')
                chart_rows.append(ids)
            save(session, 'dashboard', dashboard_body(title, slug, chart_rows, datasets[DAILY]), dashboard_id)
            results.append({'dashboard_id': dashboard_id, 'slug': slug,
                            'url': f'{SUPERSET_PUBLIC_URL}/superset/dashboard/{slug}/',
                            'chart_ids': [chart_id for row in chart_rows for chart_id, _ in row]})
    evidence = {'release_id': release['release_id'], 'accepted_rows': release['lakehouse']['accepted_rows'],
                'dashboards': results, 'chart_count': sum(len(item['chart_ids']) for item in results),
                'warehouse_connection': 'Read-only bi_reader over published mart views'}
    write_json(ARTIFACTS/'bi-verification.json', evidence)
    return evidence


if __name__ == '__main__':
    print(json.dumps(provision(), indent=2))
