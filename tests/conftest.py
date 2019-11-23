import base64
import configparser
import gzip
import random
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path

import pytest
from werkzeug.security import generate_password_hash

from datums_warehouse import create_app

TEST_USER = "user"
TEST_PASSWORD = "pass"
TEST_RANGE = namedtuple('Range', ['min', 'max'])(1500000000, 1500003000)


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
    return {'TEST_SYM/30': {'storage': str(tmp_path / "csv"), 'interval': 30, 'pair': 'TEST_SYM'},
            'INVALID_SYM/5': {'storage': str(tmp_path / "csv"), 'interval': 5, 'pair': 'INVALID_SYM'}}


@pytest.fixture
def valid_datums(warehouse_cfg):
    return [add_csv_and_range(warehouse_cfg[key]) for key in warehouse_cfg if 'INVALID' not in key]


def add_csv_and_range(cfg):
    lines = [f"{t},{t + 1 % 10},{t + 2 % 10}" for t in range(TEST_RANGE.min, TEST_RANGE.max, cfg['interval'] * 60)]
    cfg['csv'] = "timestamp,c1,c2\n" + "\n".join(lines) + "\n"
    cfg['range'] = TEST_RANGE
    return cfg


@pytest.fixture
def fragmented_datums(warehouse_cfg):
    def create_gap_in_csv(cfg):
        lines = cfg['csv'].split()
        del lines[random.randint(3, len(lines) - 3)]
        cfg['csv'] = "\n".join(lines) + "\n"
        return cfg

    return [create_gap_in_csv(add_csv_and_range(warehouse_cfg[key])) for key in warehouse_cfg if 'INVALID' in key]


@pytest.fixture
def write_csv(valid_datums, fragmented_datums):
    for datums in valid_datums + fragmented_datums:
        d = Path(datums['storage']) / datums['pair']
        d.mkdir(parents=True)
        with gzip.open((d / f"{datums['interval']}__{TEST_RANGE[0]}_{TEST_RANGE[1]}.gz"), 'wb') as f:
            f.write(datums['csv'].encode())


@pytest.fixture
def default_validation_cfg():
    return {"exclude_outliers": "volume,count", "z_score_threshold": 20}

@pytest.fixture
def app(tmp_path, credentials, write_csv, warehouse_cfg, default_validation_cfg):
    cfg_file = tmp_path / "warehouse.ini"
    cfg = configparser.ConfigParser(defaults=default_validation_cfg)
    cfg.read_dict(warehouse_cfg, "test_warehouse_cfg")
    with open(cfg_file, mode='w') as f:
        cfg.write(f)
    yield create_app({'TESTING': True, 'CREDENTIALS': credentials, 'WAREHOUSE': cfg_file, 'SECRET_KEY': "dev"})


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

    def symbol(self, sym='TEST_SYM', interval=30, since=None, until=None):
        url = [f'/api/v1.0/csv/{sym}/{interval}']
        if since is not None:
            url.append(str(since))
        if until is not None:
            url.append(str(until))
        return self._client.get("/".join(url), **self._auth_args)


@pytest.fixture
def query(client):
    return Query(client)


@pytest.fixture
def make_auth_header():
    return _make_auth_header
