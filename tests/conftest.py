import base64
import gzip
import json
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path

import pytest
from werkzeug.security import generate_password_hash

from datums_warehouse import create_app

TEST_USER = "user"
TEST_PASSWORD = "pass"


@pytest.fixture
def credentials(tmp_path):
    d = tmp_path / "credentials"
    d.mkdir()
    crd = (d / "warehouse.passwd")
    crd.write_text(f"{TEST_USER}:{generate_password_hash(TEST_PASSWORD)}\n"
                   f"other_user:{generate_password_hash('other_pass')}\n")
    return str(crd)


@pytest.fixture
def warehouse_cfg(tmp_path):
    return {'TEST_SYM/30': {'storage': str(tmp_path / "csv"), 'interval': 30, 'pair': 'TEST_SYM'}}


@pytest.fixture
def symbol_datums(warehouse_cfg):
    def add_csv(cfg):
        cfg['csv'] = "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"
        return cfg

    return [add_csv(cfg) for cfg in warehouse_cfg.values()]


@pytest.fixture
def write_csv(symbol_datums):
    for datums in symbol_datums:
        d = Path(datums['storage']) / datums['pair']
        d.mkdir(parents=True)
        with gzip.open((d / f"{datums['interval']}__0_15000000.gz"), 'wb') as f:
            f.write(datums['csv'].encode())


@pytest.fixture
def app(tmp_path, credentials, write_csv, warehouse_cfg):
    cfg_file = tmp_path / "warehouse.cfg"
    with open(cfg_file, mode='w') as f:
        json.dump(warehouse_cfg, f)
    yield create_app({'TESTING': True, 'CREDENTIALS': credentials, 'WAREHOUSE': cfg_file})


@pytest.fixture
def client(app):
    with app.test_client() as c:
        yield c


def _make_auth_header(user, password):
    return {
        "Authorization": "Basic {}".format(base64.b64encode(user.encode() + b":" + password.encode()).decode("utf8"))
    }


class Query:
    def __init__(self, client):
        self._client = client
        self._auth_args = None
        self.default_auth()

    def default_auth(self):
        self._auth_args = dict(headers=_make_auth_header(TEST_USER, TEST_PASSWORD))

    def set_auth(self, auth):
        self._auth_args = auth

    @contextmanager
    def authentication(self, **auth):
        try:
            self.set_auth(auth)
            yield query
        finally:
            self.default_auth()

    def symbol(self, sym='TEST_SYM', interval=30):
        return self._client.get(f'/api/v1.0/csv/{sym}/{interval}', **self._auth_args)


@pytest.fixture
def query(client):
    return Query(client)


@pytest.fixture
def make_auth_header():
    return _make_auth_header
