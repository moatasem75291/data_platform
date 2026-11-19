"""Small durable files shared by the publisher, catalog and metrics exporter."""
import json
import os
import tempfile

from .config import ARTIFACTS, DATA


PIPELINE_STATE = ARTIFACTS / 'pipeline-state.json'
CATALOG_STATE = ARTIFACTS / 'catalog-state.json'
CURRENT_RELEASE = DATA / 'releases/current.json'


def read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return default


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, mode='w', encoding='utf-8', delete=False) as file:
        temp = file.name
        json.dump(data, file, indent=2, default=str)
    try:
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)
