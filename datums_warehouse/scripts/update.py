from threading import Thread

from datums_warehouse.db import make_warehouse


def update_pairs(cfg, pairs):
    def update_pair(wh_cfg, pair):
        wh = make_warehouse(wh_cfg)
        wh.update(pair)

    processes = [Thread(target=update_pair, name=f"process: {p}", args=(cfg, p)) for p in pairs]

    for prc in processes:
        prc.start()

    for prc in processes:
        prc.join()
