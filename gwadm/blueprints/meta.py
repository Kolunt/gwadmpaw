"""Public meta routes: avatar proxy, titles/awards/roles views."""

import os
import re

from flask import Blueprint, abort, flash, redirect, render_template, request, send_file, url_for

try:
    import requests
except ImportError:
    requests = None

from gwadm.db import get_db_connection
from gwadm.services.avatars import (
    build_dicebear_avatar_url,
    ensure_avatar_cached,
    get_avatar_cache_path,
    get_avatar_url,
    normalize_avatar_style,
    warm_user_avatars,
)
from gwadm.services.meta_lists import get_users_with_award, get_users_with_title

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


@bp.route('/titles/<int:title_id>')
def title_view(title_id):
    """Публичный список пользователей с конкретным званием."""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE id = ?', (title_id,)).fetchone()
    conn.close()

    if not title:
        flash('Звание не найдено', 'error')
        return redirect(url_for('participants'))

    users = get_users_with_title(title_id)
    warm_user_avatars(users, size=40)
    return render_template('title_view.html', title=dict(title), users=users, get_avatar_url=get_avatar_url)


@bp.route('/roles/<role_name>')
def role_view(role_name):
    """Публичный список пользователей с конкретной ролью."""
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE name = ?', (role_name,)).fetchone()

    if not role:
        conn.close()
        flash('Роль не найдена', 'error')
        return redirect(url_for('participants'))

    users = conn.execute('''
        SELECT
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.avatar_seed,
            u.avatar_style,
            u.created_at,
            u.last_login
        FROM users u
        INNER JOIN user_roles ur ON u.user_id = ur.user_id
        INNER JOIN roles r ON ur.role_id = r.id
        WHERE r.name = ?
        ORDER BY LOWER(u.username)
    ''', (role_name,)).fetchall()
    conn.close()

    user_dicts = [dict(user) for user in users]

    return render_template(
        'role_view.html',
        role=dict(role),
        users=user_dicts,
        get_avatar_url=get_avatar_url,
    )


@bp.route('/awards/<int:award_id>')
def award_view(award_id):
    """Публичный список пользователей с конкретной наградой."""
    conn = get_db_connection()
    award = conn.execute('SELECT * FROM awards WHERE id = ?', (award_id,)).fetchone()
    conn.close()

    if not award:
        flash('Награда не найдена', 'error')
        return redirect(url_for('participants'))

    users = get_users_with_award(award_id)
    warm_user_avatars(users, size=40)
    return render_template('award_view.html', award=dict(award), users=users, get_avatar_url=get_avatar_url)
