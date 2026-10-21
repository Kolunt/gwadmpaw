"""User account blocking: info queries and request gate."""

from __future__ import annotations

from flask import g, jsonify, redirect, request, session, url_for

from gwadm.db import get_db_connection
from gwadm.services.roles import get_user_roles

_BLOCKED_ALLOW_ENDPOINTS = frozenset({
    'static',
    'meta.health',
    'auth.account_blocked',
    'auth.logout',
})


def _row_is_blocked(row) -> bool:
    if not row:
        return False
    return bool(row['is_blocked'])


def get_user_block_info(user_id) -> dict | None:
    """Return block details if user is blocked, else None."""
    if not user_id:
        return None
    conn = get_db_connection()
    try:
        row = conn.execute(
            '''
            SELECT u.is_blocked, u.blocked_reason, u.blocked_at, u.blocked_by,
                   b.user_id AS blocker_user_id, b.username AS blocker_username
            FROM users u
            LEFT JOIN users b ON u.blocked_by = b.user_id
            WHERE u.user_id = ?
            ''',
            (user_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row or not _row_is_blocked(row):
        return None
    blocker = None
    if row['blocked_by']:
        blocker = {
            'user_id': row['blocker_user_id'] or row['blocked_by'],
            'username': row['blocker_username'],
        }
    return {
        'blocked_reason': row['blocked_reason'],
        'blocked_at': row['blocked_at'],
        'blocked_by': row['blocked_by'],
        'blocker': blocker,
    }


def is_user_blocked(user_id) -> bool:
    return get_user_block_info(user_id) is not None


def load_request_block_info():
    """Load block info for current session user into flask.g (once per request)."""
    if hasattr(g, 'user_block_info'):
        return g.user_block_info
    user_id = session.get('user_id')
    if not user_id or session.get('impersonation_original'):
        g.user_block_info = None
        return None
    info = get_user_block_info(user_id)
    g.user_block_info = info
    return info


def is_account_blocked_session() -> bool:
    """True if logged-in user (not impersonating) is blocked."""
    return load_request_block_info() is not None


def list_admins_moderators_for_contact(conn=None):
    """Users with admin or moderator role, with roles list for contact cards."""
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT DISTINCT u.*,
                   GROUP_CONCAT(DISTINCT r.name) as roles_list
            FROM users u
            INNER JOIN user_roles ur ON u.user_id = ur.user_id
            INNER JOIN roles r ON ur.role_id = r.id
            WHERE r.name IN ('admin', 'moderator')
            GROUP BY u.user_id
            ORDER BY
                CASE WHEN r.name = 'admin' THEN 1 ELSE 2 END,
                u.username
            ''',
        ).fetchall()
    finally:
        if own_conn:
            conn.close()
    result = []
    for user in rows:
        user_dict = dict(user)
        user_dict['roles'] = get_user_roles(user['user_id'])
        result.append(user_dict)
    return result


def _wants_json_response() -> bool:
    if request.path.startswith('/api/'):
        return True
    accept = request.headers.get('Accept', '')
    if 'application/json' in accept:
        return True
    return request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest'


def register_blocked_user_gate(app) -> None:
    """Redirect blocked users to account_blocked; JSON clients get 403."""

    @app.before_request
    def _blocked_user_gate():
        if request.endpoint and request.endpoint in _BLOCKED_ALLOW_ENDPOINTS:
            return None
        if not session.get('user_id'):
            return None
        if session.get('impersonation_original'):
            return None
        if not is_account_blocked_session():
            return None
        if _wants_json_response():
            return jsonify({
                'success': False,
                'error': 'Доступ к сайту ограничен: учётная запись заблокирована',
                'code': 'account_blocked',
            }), 403
        return redirect(url_for('auth.account_blocked'))
