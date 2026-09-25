"""Queries for public title/award user list views."""

from gwadm.db import get_db_connection


def get_users_with_title(title_id):
    """Получает список пользователей, имеющих указанное звание."""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.avatar_seed,
            u.avatar_style,
            u.created_at,
            u.last_login,
            ut.assigned_by,
            ut.assigned_at,
            COALESCE(admin.username, '') AS assigned_by_username
        FROM user_titles ut
        JOIN users u ON ut.user_id = u.user_id
        LEFT JOIN users admin ON ut.assigned_by = admin.user_id
        WHERE ut.title_id = ?
        ORDER BY u.username COLLATE NOCASE
    ''', (title_id,)).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_users_with_award(award_id):
    """Получает список пользователей, имеющих указанную награду."""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.avatar_seed,
            u.avatar_style,
            u.created_at,
            u.last_login,
            ua.assigned_by,
            ua.assigned_at,
            COALESCE(admin.username, '') AS assigned_by_username
        FROM user_awards ua
        JOIN users u ON ua.user_id = u.user_id
        LEFT JOIN users admin ON ua.assigned_by = admin.user_id
        WHERE ua.award_id = ?
        ORDER BY u.username COLLATE NOCASE
    ''', (award_id,)).fetchall()
    conn.close()

    return [dict(row) for row in rows]
