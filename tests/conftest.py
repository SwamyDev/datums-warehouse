import base64
import json
from collections import namedtuple
from contextlib import contextmanager

import pytest
from werkzeug.security import generate_password_hash

from datums_warehouse import create_app

TEST_USER = "user"
TEST_PASSWORD = "pass"


@pytest.fixture
def write_credentials(tmp_path):
    credentials = tmp_path / "credentials"
    credentials.mkdir()
    (credentials / "warehouse.passwd").write_text(f"{TEST_USER}:{generate_password_hash(TEST_PASSWORD)}\n"
                                                  f"other_user:{generate_password_hash('other_pass')}\n")


@pytest.fixture
def test_sym():
    return namedtuple("TestSymbol", ["name", "data"])(name="TEST_SYM", data="c1,c2\n0,1\n")


@pytest.fixture
def warehouse_cfg(tmp_path):
    return {'TEST_SYM/30': {'storage': str(tmp_path / "csv"), 'interval': 30, 'pair': 'TEST_SYM'}}


@pytest.fixture
def write_csv(tmp_path, test_sym):
    csv = tmp_path / "csv"
    csv.mkdir()
    (csv / f"{test_sym.name}.csv").write_text(test_sym.data)


@pytest.fixture
def app(tmp_path, write_credentials, write_csv, warehouse_cfg):
    cfg_file = tmp_path / "warehouse.cfg"
    with open(cfg_file, mode='w') as f:
        json.dump(warehouse_cfg, f)
    yield create_app({'TESTING': True, 'DATA_DIR': str(tmp_path), 'WAREHOUSE': cfg_file})


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

    def symbol(self, sym='symbol', interval=30):
        return self._client.get(f'/api/v1.0/csv/{sym}/{interval}', **self._auth_args)


@pytest.fixture
def query(client):
    return Query(client)


@pytest.fixture
def make_auth_header():
    return _make_auth_header
