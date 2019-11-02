import json

from flask import current_app, g

from datums_warehouse.broker.warehouse import Warehouse


def get_warehouse():
    if 'db' not in g:
        with open(current_app.config['WAREHOUSE'], mode='r') as f:
            cfg = json.load(f)
            g.db = Warehouse(cfg)

    return g.db
