"""User awards (read-only helpers)."""

from gwadm.db import get_db_connection


def get_user_awards(user_id):
    """Получает список наград пользователя."""
    if not user_id:
        return []
    conn = get_db_connection()
    awards = conn.execute('''
        SELECT a.* FROM awards a
        INNER JOIN user_awards ua ON a.id = ua.award_id
        WHERE ua.user_id = ?
        ORDER BY a.sort_order, a.title
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(a) for a in awards]
