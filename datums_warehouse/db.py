import configparser

from flask import current_app, g

from datums_warehouse.broker.warehouse import Warehouse


def get_warehouse():
    if 'db' not in g:
        cfg = configparser.ConfigParser()
        cfg.read(current_app.config['WAREHOUSE'])
        g.db = make_warehouse(cfg)

    return g.db


def make_warehouse(cfg):
    cfg_dict = dict()
    for pkt in [p for p in cfg if p != cfg.default_section]:
        cfg_dict[pkt] = dict()
        for key in cfg[pkt]:
            cfg_dict[pkt][key] = cfg[pkt][key]
            if key == "exclude_outliers":
                cfg_dict[pkt][key] = [c.strip() for c in cfg_dict[pkt][key].split(',')]
    return Warehouse(cfg_dict)
