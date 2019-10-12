def test_requesting_non_existent_data(query):
    assert "unknown" in query.symbol("unknown").json['error']


def test_request_existing_data(query, test_sym):
    assert query.symbol(test_sym.name).json['csv'] == test_sym.data
