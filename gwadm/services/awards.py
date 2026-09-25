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

def assign_award(user_id, award_id, assigned_by=None):
    """Назначает награду пользователю"""
    if not user_id or not award_id:
        return False
    conn = get_db_connection()
    try:
        # Проверяем, не назначена ли уже эта награда
        existing = conn.execute('''
            SELECT id FROM user_awards WHERE user_id = ? AND award_id = ?
        ''', (user_id, award_id)).fetchone()
        
        if existing:
            # Награда уже назначена, не создаем событие повторно
            conn.close()
            return True
        
        conn.execute('''
            INSERT OR REPLACE INTO user_awards (user_id, award_id, assigned_by)
            VALUES (?, ?, ?)
        ''', (user_id, award_id, assigned_by))
        
        # Создаем событие бубенчика для награды
        source = f'award:{award_id}'
        reason = f'Назначена награда (ID: {award_id})'
        points = get_rating_setting(f'rating_award_{award_id}', 0)
        
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
            'award_assign',
            details=f'Назначена награда {award_id} пользователю {user_id}',
            metadata={'target_user_id': user_id, 'award_id': award_id, 'assigned_by': assigned_by},
            user_id=assigned_by
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning award: {e}")
        conn.close()
        return False


def remove_award(user_id, award_id):
    """Удаляет награду у пользователя"""
    if not user_id or not award_id:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM user_awards
            WHERE user_id = ? AND award_id = ?
        ''', (user_id, award_id))
        
        # Деактивируем событие бубенчика для награды
        source = f'award:{award_id}'
        conn.execute('''
            UPDATE snowflake_events
            SET active = 0, revoked_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND source = ? AND active = 1
        ''', (user_id, source))
        
        conn.commit()
        log_activity(
            'award_remove',
            details=f'Удалена награда {award_id} у пользователя {user_id}',
            metadata={'target_user_id': user_id, 'award_id': award_id}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing award: {e}")
        conn.close()
        return False

# Декораторы для проверки прав доступа
# Проверка подписи sign
