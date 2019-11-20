import os

from flask import Flask

from datums_warehouse import query_csv
from datums_warehouse._version import __version__


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        WAREHOUSE=os.path.join(app.instance_path, 'warehouse.json')
    )

    if test_config is None:
        app.config.from_pyfile(os.path.join(app.instance_path, 'config.py'), silent=True)
    else:
        app.config.update(test_config)

    app.register_blueprint(query_csv.bp)

    return app
