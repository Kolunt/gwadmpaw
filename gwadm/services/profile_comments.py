"""Profile admin comments and recipient thanks."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_error
from gwadm.services.activity import log_activity


def get_user_admin_comments(user_id, viewer_is_admin=False):
    """Получает список комментариев к профилю пользователя с учетом прав доступа."""
    if not user_id:
        return []
    conn = get_db_connection()

    if viewer_is_admin:
        comments = conn.execute('''
            SELECT
                c.id,
                c.user_id,
                c.admin_user_id,
                c.comment,
                c.is_admin_only,
                c.is_thanks_from_recipient,
                c.assignment_id,
                c.created_at,
                c.updated_at,
                u.username AS admin_username
            FROM user_admin_comments c
            LEFT JOIN users u ON c.admin_user_id = u.user_id
            WHERE c.user_id = ?
            ORDER BY c.is_thanks_from_recipient DESC, c.created_at DESC
        ''', (user_id,)).fetchall()
    else:
        comments = conn.execute('''
            SELECT
                c.id,
                c.user_id,
                c.admin_user_id,
                c.comment,
                c.is_admin_only,
                c.is_thanks_from_recipient,
                c.assignment_id,
                c.created_at,
                c.updated_at,
                u.username AS admin_username
            FROM user_admin_comments c
            LEFT JOIN users u ON c.admin_user_id = u.user_id
            WHERE c.user_id = ? AND (c.is_admin_only = 0 OR c.is_thanks_from_recipient = 1)
            ORDER BY c.is_thanks_from_recipient DESC, c.created_at DESC
        ''', (user_id,)).fetchall()

    conn.close()
    return [dict(c) for c in comments]


def add_user_admin_comment(user_id, admin_user_id, comment, is_admin_only=False):
    """Добавляет комментарий администратора к профилю пользователя."""
    if not user_id or not admin_user_id or not comment:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT INTO user_admin_comments (user_id, admin_user_id, comment, is_admin_only)
            VALUES (?, ?, ?, ?)
        ''', (user_id, admin_user_id, comment.strip(), 1 if is_admin_only else 0))
        conn.commit()
        log_activity(
            'admin_comment_add',
            details=f'Добавлен комментарий к профилю пользователя {user_id}',
            metadata={'target_user_id': user_id, 'comment_length': len(comment), 'is_admin_only': is_admin_only}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error adding admin comment: {e}")
        conn.close()
        return False


def add_thanks_comment_from_recipient(user_id, assignment_id, thanks_message):
    """Добавляет автоматический комментарий "спасибо от внучка" при подтверждении получения подарка."""
    if not user_id or not assignment_id or not thanks_message:
        return False
    conn = get_db_connection()
    try:
        existing = conn.execute('''
            SELECT id FROM user_admin_comments
            WHERE user_id = ? AND assignment_id = ? AND is_thanks_from_recipient = 1
        ''', (user_id, assignment_id)).fetchone()

        if existing:
            conn.execute('''
                UPDATE user_admin_comments
                SET comment = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (thanks_message.strip(), existing['id']))
        else:
            conn.execute('''
                INSERT INTO user_admin_comments (user_id, admin_user_id, comment, is_admin_only, is_thanks_from_recipient, assignment_id)
                VALUES (?, NULL, ?, 0, 1, ?)
            ''', (user_id, thanks_message.strip(), assignment_id))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error adding thanks comment: {e}")
        conn.close()
        return False


def update_user_admin_comment(comment_id, admin_user_id, comment):
    """Обновляет комментарий администратора."""
    if not comment_id or not admin_user_id or not comment:
        return False
    conn = get_db_connection()
    try:
        existing = conn.execute('''
            SELECT admin_user_id FROM user_admin_comments WHERE id = ?
        ''', (comment_id,)).fetchone()
        if not existing or existing['admin_user_id'] != admin_user_id:
            conn.close()
            return False
        conn.execute('''
            UPDATE user_admin_comments
            SET comment = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (comment.strip(), comment_id))
        conn.commit()
        log_activity(
            'admin_comment_update',
            details=f'Обновлен комментарий {comment_id}',
            metadata={'comment_id': comment_id}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error updating admin comment: {e}")
        conn.close()
        return False


def delete_user_admin_comment(comment_id, admin_user_id):
    """Удаляет комментарий администратора (только свой)."""
    if not comment_id or not admin_user_id:
        return False
    conn = get_db_connection()
    try:
        existing = conn.execute('''
            SELECT admin_user_id, user_id FROM user_admin_comments WHERE id = ?
        ''', (comment_id,)).fetchone()
        if not existing or existing['admin_user_id'] != admin_user_id:
            conn.close()
            return False
        target_user_id = existing['user_id']
        conn.execute('DELETE FROM user_admin_comments WHERE id = ?', (comment_id,))
        conn.commit()
        log_activity(
            'admin_comment_delete',
            details=f'Удален комментарий {comment_id} к профилю пользователя {target_user_id}',
            metadata={'comment_id': comment_id, 'target_user_id': target_user_id}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error deleting admin comment: {e}")
        conn.close()
        return False
