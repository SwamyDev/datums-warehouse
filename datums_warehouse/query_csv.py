import functools
from pathlib import Path

from flask import Blueprint, current_app, request, jsonify
from werkzeug.security import check_password_hash

bp = Blueprint("query_csv", __name__, url_prefix="/api/v1.0/csv/")


def _invalid_auth():
    auth = request.authorization
    if auth is None:
        return True
    creds = _get_credentials()
    return auth.username not in creds or not check_password_hash(creds[auth.username], auth.password)


def _get_credentials():
    lines = (_get_data_dir() / "creds/warehouse.passwd").read_text().splitlines()

    def split_cred(raw):
        i = raw.find(':')
        return raw[:i], raw[i + 1:]

    return {u: p for u, p in map(split_cred, lines)}


def _get_data_dir():
    return Path(current_app.config['DATA_DIR'])


def require_auth(route):
    @functools.wraps(route)
    def wrapped_route(*args, **kwargs):
        if _invalid_auth():
            return {"error": "unauthorized"}, 401
        return route(*args, **kwargs)

    return wrapped_route


@bp.route("<string:sym>")
@require_auth
def query_symbols(sym):
    csv = _get_data_dir() / f'csv/{sym}.csv'
    if not csv.exists():
        return jsonify({"csv": None, "warning": f"symbol {sym} does not exist"}), 204
    return jsonify({"csv": csv.read_text()}), 200
