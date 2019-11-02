import functools
from pathlib import Path

from flask import Blueprint, current_app, request, jsonify
from werkzeug.security import check_password_hash

from datums_warehouse.broker.validation import DataError
from datums_warehouse.broker.warehouse import MissingPacketError
from datums_warehouse.db import get_warehouse

bp = Blueprint("query_csv", __name__, url_prefix="/api/v1.0/csv/")


def _invalid_auth():
    auth = request.authorization
    if auth is None:
        return True
    creds = _get_credentials()
    return auth.username not in creds or not check_password_hash(creds[auth.username], auth.password)


def _get_credentials():
    lines = Path(current_app.config['CREDENTIALS']).read_text().splitlines()

    def split_cred(raw):
        i = raw.find(':')
        return raw[:i], raw[i + 1:]

    return {u: p for u, p in map(split_cred, lines)}


def require_auth(route):
    @functools.wraps(route)
    def wrapped_route(*args, **kwargs):
        if _invalid_auth():
            return {"error": "unauthorized"}, 401
        return route(*args, **kwargs)

    return wrapped_route


@bp.route("<string:sym>/<int:interval>")
@require_auth
def query_symbols(sym, interval):
    return _retrieve_symbols(sym, interval)


@bp.route("<string:sym>/<int:interval>/<int:since>")
@require_auth
def query_symbols_since(sym, interval, since):
    return _retrieve_symbols(sym, interval, since)


@bp.route("<string:sym>/<int:interval>/<int:since>/<int:until>")
@require_auth
def query_symbols_range(sym, interval, since, until):
    return _retrieve_symbols(sym, interval, since, until)


def _retrieve_symbols(sym, interval, since=None, until=None):
    try:
        datums = get_warehouse().retrieve(f"{sym}/{interval}", since, until)
    except (MissingPacketError, DataError) as e:
        return jsonify({"csv": None, "error": str(e)}), 200
    return jsonify({"csv": datums.csv}), 200
