import logging
from flask import Flask

from datums_warehouse import query_csv
from datums_warehouse._version import __version__


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)

    if test_config is None:
        app.config.from_envvar('DATUMS_WAREHOUSE_CONFIG', silent=True)
    else:
        app.config.update(test_config)

    log_cfg = dict(handlers=[])
    if 'LOG_FILE' in app.config:
        log_cfg['handlers'].append(logging.FileHandler(app.config['LOG_FILE']))
    if 'LOG_LEVEL' in app.config:
        log_cfg['level'] = getattr(logging, app.config['LOG_LEVEL'].upper())
    print(log_cfg)
    logging.basicConfig(**log_cfg)

    app.register_blueprint(query_csv.bp)

    return app
