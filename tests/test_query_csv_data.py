def test_requesting_non_existent_data(query):
    assert "unknown" in query.symbol("unknown").json['error']


def test_request_existing_data(query, symbol_datums):
    for datums in symbol_datums:
        assert query.symbol(datums['pair'], datums['interval']).json['csv'] == datums['csv']
