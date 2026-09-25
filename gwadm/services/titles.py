"""User titles (read-only helpers)."""

from gwadm.db import get_db_connection


def get_user_titles(user_id):
    """Получает список званий пользователя."""
    if not user_id:
        return []
    conn = get_db_connection()
    titles = conn.execute('''
        SELECT t.* FROM titles t
        INNER JOIN user_titles ut ON t.id = ut.title_id
        WHERE ut.user_id = ?
        ORDER BY t.display_name
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(t) for t in titles]
