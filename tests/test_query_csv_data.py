def test_requesting_non_existent_data(query):
    assert query.symbol("unknown").status_code == 204


def test_request_existing_data(query, test_sym):
    assert query.symbol(test_sym.name).json['csv'] == test_sym.data
