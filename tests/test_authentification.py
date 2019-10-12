import pytest


@pytest.mark.parametrize('auth', [None, ('invalid', 'auth'), ("user", 'invalid')])
def test_requesting_data_with_invalid_auth(client, auth, make_auth_header):
    kwargs = {} if auth is None else dict(headers=make_auth_header(*auth))
    assert client.get('/api/v1.0/csv/symbol', **kwargs).status_code == 401


def test_multiple_users(client, make_auth_header):
    assert client.get('/api/v1.0/csv/symbol', headers=make_auth_header('user', 'pass')).status_code != 401
    assert client.get('/api/v1.0/csv/symbol', headers=make_auth_header('other_user', 'other_pass')).status_code != 401
