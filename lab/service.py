"""Internal full-run API and Prometheus exporter."""
import hmac
import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import REGISTRY, start_http_server

from .monitor import LabMetrics
from .pipeline import PipelineBusy, run
from .run_state import CATALOG_STATE, PIPELINE_STATE, read_json


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        supplied = self.headers.get('Authorization', '').removeprefix('Bearer ')
        if not hmac.compare_digest(supplied, os.environ['PIPELINE_API_TOKEN']):
            self.send_error(403)
            return
        if self.path != '/run':
            self.send_error(404)
            return
        try:
            result = run()
        except PipelineBusy:
            self.send_error(409, 'A retail pipeline run is already active')
            return
        except Exception as error:
            logging.exception('Full retail pipeline run failed')
            self.send_error(500, type(error).__name__)
            return
        body = json.dumps(result, default=str).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def refresh_catalog_after_runs():
    from .catalog_auth import READY_FILE

    while True:
        time.sleep(30)
        if not READY_FILE.is_file():
            continue
        state = read_json(PIPELINE_STATE, {})
        if state.get('engine') != 'lakehouse':
            continue
        attempt = state.get('attempt', {})
        catalog = read_json(CATALOG_STATE, {})
        finished = attempt.get('finished_at')
        if not finished or catalog.get('source_attempt_finished_at') == finished:
            continue
        if time.time() - catalog.get('attempted_at', 0) < 90:
            continue
        try:
            from .catalog import sync
            sync()
        except Exception as error:
            logging.warning('Catalog refresh can be retried with lab.cmd metadata-sync: %s', error)


if __name__ == '__main__':
    metrics = LabMetrics()
    REGISTRY.register(metrics)
    start_http_server(9100)
    threading.Thread(target=metrics.refresh_health, daemon=True).start()
    threading.Thread(target=refresh_catalog_after_runs, daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', 9000), Handler).serve_forever()
