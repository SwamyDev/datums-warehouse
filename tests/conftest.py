import base64
from collections import namedtuple

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
def write_csv(tmp_path, test_sym):
    csv = tmp_path / "csv"
    csv.mkdir()
    (csv / f"{test_sym.name}.csv").write_text(test_sym.data)


@pytest.fixture
def app(tmp_path, write_credentials, write_csv):
    yield create_app({'TESTING': True, 'DATA_DIR': str(tmp_path)})


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

    def symbol(self, sym):
        return self._client.get(f'/api/v1.0/csv/{sym}', headers=_make_auth_header(TEST_USER, TEST_PASSWORD))


@pytest.fixture
def query(client):
    return Query(client)


@pytest.fixture
def make_auth_header():
    return _make_auth_header
