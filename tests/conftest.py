import pytest

from datums_warehouse import create_app


@pytest.fixture
def app():
    yield create_app({'TESTING': True, })


@pytest.fixture
def client(app):
    return app.test_client()
