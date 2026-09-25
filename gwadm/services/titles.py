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

def get_all_titles():
    """Получает список всех званий"""
    conn = get_db_connection()
    titles = conn.execute('''
        SELECT * FROM titles ORDER BY is_system DESC, display_name
    ''').fetchall()
    conn.close()
    return [dict(t) for t in titles]


def get_title_by_name(title_name):
    """Получает звание по имени"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE name = ?', (title_name,)).fetchone()
    conn.close()
    return dict(title) if title else None


def assign_title(user_id, title_id, assigned_by=None):
    """Назначает звание пользователю"""
    if not user_id or not title_id:
        return False
    conn = get_db_connection()
    try:
        # Проверяем, не назначено ли уже это звание
        existing = conn.execute('''
            SELECT id FROM user_titles WHERE user_id = ? AND title_id = ?
        ''', (user_id, title_id)).fetchone()
        
        if existing:
            # Звание уже назначено, не создаем событие повторно
            conn.close()
            return True
        
        conn.execute('''
            INSERT OR REPLACE INTO user_titles (user_id, title_id, assigned_by)
            VALUES (?, ?, ?)
        ''', (user_id, title_id, assigned_by))
        
        # Создаем событие бубенчика для звания
        source = f'title:{title_id}'
        reason = f'Назначено звание (ID: {title_id})'
        points = get_rating_setting(f'rating_title_{title_id}', 0)
        
        if points != 0:
            # Проверяем, нет ли уже такого события
            existing_event = conn.execute('''
                SELECT id, active FROM snowflake_events
                WHERE user_id = ? AND source = ?
            ''', (user_id, source)).fetchone()
            
            if existing_event:
                # Обновляем существующее событие
                conn.execute('''
                    UPDATE snowflake_events
                    SET points = ?, active = 1, manual_revoked = 0,
                        revoked_at = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (points, existing_event['id']))
            else:
                # Создаем новое событие
                conn.execute('''
                    INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                    VALUES (?, ?, ?, ?, 1, 0)
                ''', (user_id, source, reason, points))
        
        conn.commit()
        log_activity(
            'title_assign',
            details=f'Назначено звание {title_id} пользователю {user_id}',
            metadata={'target_user_id': user_id, 'title_id': title_id, 'assigned_by': assigned_by},
            user_id=assigned_by
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning title: {e}")
        conn.close()
        return False


def remove_title(user_id, title_id):
    """Удаляет звание у пользователя"""
    if not user_id or not title_id:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM user_titles
            WHERE user_id = ? AND title_id = ?
        ''', (user_id, title_id))
        
        # Деактивируем событие бубенчика для звания
        source = f'title:{title_id}'
        conn.execute('''
            UPDATE snowflake_events
            SET active = 0, revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND source = ? AND active = 1
        ''', (user_id, source))
        
        conn.commit()
        log_activity(
            'title_remove',
            details=f'Удалено звание {title_id} у пользователя {user_id}',
            metadata={'target_user_id': user_id, 'title_id': title_id}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing title: {e}")
        conn.close()
        return False

