"""Local OpenMetadata bootstrap and REST authentication."""
import argparse
import base64
import os
import secrets
import subprocess
import tempfile
from contextlib import suppress
from urllib.parse import quote

import requests

from .config import DATA


PRIVATE_DIR = DATA / 'openmetadata'
ADMIN_FILE = PRIVATE_DIR / 'admin-password'
READY_FILE = PRIVATE_DIR / 'catalog-ready'
METADATA_URL = os.getenv('OPENMETADATA_URL', 'http://openmetadata:8585').rstrip('/')
METADATA_EMAIL = 'admin@open-metadata.org'


class MetadataAPIError(RuntimeError):
    def __init__(self, method, path, response):
        self.status = response.status_code
        details = ''
        if path not in ('users/login', 'users/changePassword', 'users/logout'):
            try:
                details = str(response.json().get('message', ''))[:400]
            except ValueError:
                pass
        super().__init__(f'OpenMetadata {method} {path} returned {self.status}. {details}'.strip())


class MetadataAPI:
    def __init__(self):
        self.session = requests.Session()
        self.refresh_token = None

    def request(self, method, path, body=None, params=None):
        response = self.session.request(method, f'{METADATA_URL}/api/v1/{path}', json=body,
                                        params=params, timeout=30)
        if not response.ok:
            raise MetadataAPIError(method, path, response)
        if path not in ('users/changePassword', 'users/logout') and response.content and 'json' in response.headers.get('Content-Type', ''):
            return response.json()
        return response.text or None

    def login(self, password):
        credentials = {'email': METADATA_EMAIL, 'password': base64.b64encode(password.encode()).decode()}
        result = self.request('POST', 'users/login', credentials)
        self.session.headers['Authorization'] = 'Bearer ' + result['accessToken']
        self.refresh_token = result.get('refreshToken')
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        token = self.session.headers['Authorization'].removeprefix('Bearer ')
        body = {'token': token, 'username': METADATA_EMAIL.split('@')[0]}
        if self.refresh_token:
            body['refreshToken'] = self.refresh_token
        with suppress(MetadataAPIError, requests.RequestException):
            self.request('POST', 'users/logout', body)
        self.session.close()

    def put(self, path, body):
        return self.request('PUT', path, body)

    def get(self, path, params=None):
        return self.request('GET', path, params=params)

    def by_name(self, collection, name, params=None):
        return self.get(f'{collection}/name/{quote(name, safe="")}', params)


def write_secret_once(path, text):
    try:
        with open(path, 'x', encoding='utf-8') as file:
            file.write(text)
        path.chmod(0o600)
    except FileExistsError:
        pass


def prepare():
    PRIVATE_DIR.mkdir(parents=True, exist_ok=True)
    secret = secrets.token_hex(24)
    fernet = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    write_secret_once(PRIVATE_DIR/'runtime.env',
                      f'POSTGRES_PASSWORD={secret}\nDB_USER_PASSWORD={secret}\nFERNET_KEY={fernet}\n')
    write_secret_once(ADMIN_FILE, f'Aa1!{secrets.token_urlsafe(24)}\n')
    keys = PRIVATE_DIR / 'keys'
    if not keys.exists():
        with tempfile.TemporaryDirectory(dir=PRIVATE_DIR) as work:
            pem = os.path.join(work, 'signing.pem')
            os.makedirs(keys, exist_ok=True)
            subprocess.run(['openssl', 'genpkey', '-quiet', '-algorithm', 'RSA', '-out', pem], check=True)
            subprocess.run(['openssl', 'pkey', '-in', pem, '-pubout', '-outform', 'DER',
                            '-out', str(keys/'public.der')], check=True)
            subprocess.run(['openssl', 'pkcs8', '-topk8', '-nocrypt', '-in', pem, '-outform', 'DER',
                            '-out', str(keys/'private.der')], check=True)
    if not all((keys/name).is_file() for name in ('public.der', 'private.der')):
        raise RuntimeError('OpenMetadata signing key files are incomplete in data/openmetadata/keys')
    # The server runs as a separate container UID and receives only this directory.
    (keys/'private.der').chmod(0o644)


def local_api():
    return MetadataAPI().login(ADMIN_FILE.read_text(encoding='utf-8').strip())


def bootstrap_admin():
    try:
        with local_api():
            pass
    except MetadataAPIError as error:
        if error.status not in (400, 401, 403):
            raise
        password = ADMIN_FILE.read_text(encoding='utf-8').strip()
        with MetadataAPI().login('admin') as api:
            api.put('users/changePassword', {'username': 'admin', 'requestType': 'SELF',
                                             'oldPassword': 'admin', 'newPassword': password,
                                             'confirmPassword': password})
        with local_api():
            pass
    READY_FILE.touch()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'bootstrap-admin'))
    if parser.parse_args().action == 'prepare':
        prepare()
    else:
        bootstrap_admin()
        print('OpenMetadata admin is ready. Local password file: data/openmetadata/admin-password')
