from datums_warehouse.db import get_warehouse


def test_get_same_warehouse_in_app_context(app):
    with app.app_context():
        warehouse = get_warehouse()
        assert warehouse and warehouse is get_warehouse()
