"""Public meta routes: avatar proxy, titles/awards (future)."""

import os
import re

from flask import Blueprint, abort, redirect, request, send_file

try:
    import requests
except ImportError:
    requests = None

from gwadm.services.avatars import (
    build_dicebear_avatar_url,
    ensure_avatar_cached,
    get_avatar_cache_path,
    normalize_avatar_style,
)

bp = Blueprint('meta', __name__)


@bp.route('/avatars/image')
def avatar_image():
    """Прокси и дисковый кэш аватаров DiceBear (same-origin для браузера)."""
    seed = (request.args.get('seed') or '').strip()
    style = normalize_avatar_style(request.args.get('style'))
    try:
        size_value = int(request.args.get('size', 40))
    except (TypeError, ValueError):
        size_value = 40
    size_value = max(16, min(size_value, 256))

    if not seed or not re.fullmatch(r'[\w.\-]+', seed):
        abort(404)

    cache_path, fmt = get_avatar_cache_path(seed, style, size_value)
    mimetype = 'image/png' if fmt == 'png' else 'image/svg+xml'

    if not os.path.exists(cache_path):
        if not ensure_avatar_cached(seed, style, size_value):
            dicebear_url, _, _, _ = build_dicebear_avatar_url(seed, style, size_value)
            if requests:
                return redirect(dicebear_url)
            abort(502)

    return send_file(cache_path, mimetype=mimetype, max_age=604800)
