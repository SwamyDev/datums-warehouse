from datums_warehouse import create_app


def test_config():
    assert not create_app().testing
    assert create_app({'TESTING': True}).testing


def test_configure_logging(tmp_path):
    create_app({
        'LOG_FILE': str(tmp_path / "test.log"),
        'LOG_LEVEL': "error",
    })

    assert (tmp_path / "test.log").exists()
