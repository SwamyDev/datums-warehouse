import pytest


@pytest.mark.parametrize('auth', [None, ('invalid', 'auth'), ("user", 'invalid')])
def test_requesting_data_with_invalid_auth(query, auth, make_auth_header):
    kwargs = {} if auth is None else dict(headers=make_auth_header(*auth))
    with query.authentication(**kwargs):
        assert query.symbol().status_code == 401


def test_multiple_users(query, make_auth_header):
    with query.authentication(headers=make_auth_header('user', 'pass')):
        assert query.symbol().status_code != 401
    with query.authentication(headers=make_auth_header('other_user', 'other_pass')):
        assert query.symbol().status_code != 401
