"""Rating snowflake helpers."""

import re

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.settings import get_rating_setting, get_setting, set_setting

RATING_RECALC_PENDING_KEY = 'rating_recalc_pending'

_RATING_AGGREGATION_FROM = '''
    FROM users u
    LEFT JOIN snowflake_events se ON u.user_id = se.user_id
        AND (se.active = 1 OR CAST(se.active AS INTEGER) = 1)
        AND (se.manual_revoked IS NULL OR se.manual_revoked = 0
             OR CAST(se.manual_revoked AS INTEGER) = 0)
    GROUP BY u.user_id, u.username
'''
def _normalize_contact_value(value):
    if not value:
        return ''
    value = str(value).strip()
    if not value:
        return ''
    lowered = value.lower()
    if lowered in {'не использую', 'нет', '-', 'none', 'no', 'n/a'}:
        return ''
    return value


_SNOWFLAKE_CONTACT_SOURCES = (
    ('telegram', 'Telegram', 'Заполнен Telegram'),
    ('whatsapp', 'WhatsApp', 'Заполнен WhatsApp'),
    ('viber', 'Viber', 'Заполнен Viber'),
)
_SNOWFLAKE_SOURCE_LABELS = {source: label for source, label, _ in _SNOWFLAKE_CONTACT_SOURCES}



def _normalize_multiline_text(value, max_length=None):
    if value is None:
        return ''
    text = str(value)
    text = text.replace('\r\n', '\n')
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = text.replace('\u2028', '\n').replace('\u2029', '\n')
    text = text.strip()
    text = re.sub(r'\n{3,}', '\n\n', text)
    if max_length and len(text) > max_length:
        text = text[:max_length]
    return text



def _sync_contact_snowflakes(conn, user_row):
    """Синхронизирует записи о бубенчиках с актуальными контактами пользователя."""
    if not isinstance(user_row, dict):
        user_row = dict(user_row)
    user_id = user_row.get('user_id')
    if not user_id:
        return

    existing = conn.execute(
        '''
        SELECT id, source, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ?
        ''',
        (user_id,)
    ).fetchall()
    existing_map = {row['source']: row for row in existing}

    for source, label, reason in _SNOWFLAKE_CONTACT_SOURCES:
        contact_value = _normalize_contact_value(user_row.get(source))
        event = existing_map.get(source)
        # Получаем настройку очков для этого контакта
        points = get_rating_setting(f'rating_contact_{source}', 1)
        if contact_value:
            if not event:
                conn.execute(
                    '''
                    INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (user_id, source, reason, points, 1, 0)
                )
            elif not event['active'] and not event['manual_revoked']:
                conn.execute(
                    '''
                    UPDATE snowflake_events
                    SET active = 1,
                        points = ?,
                        revoked_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    ''',
                    (points, event['id'])
                )
        else:
            if event and event['active'] and not event['manual_revoked']:
                conn.execute(
                    '''
                    UPDATE snowflake_events
                    SET active = 0,
                        revoked_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    ''',
                    (event['id'],)
                )



def _get_snowflake_source_label(source):
    contact_map = {s: label for s, label, _ in _SNOWFLAKE_CONTACT_SOURCES}
    if source in contact_map:
        return contact_map[source]
    if source.startswith('event:'):
        parts = source.split(':')
        if len(parts) >= 3:
            try:
                event_part = parts[1]
                event_id = int(event_part)
            except (ValueError, TypeError):
                event_id = None
            suffix = parts[2]
            if suffix == 'registration_bonus':
                return f'Бонус за регистрацию (мероприятие #{event_id})' if event_id else 'Бонус за регистрацию'
            if suffix == 'gift_not_sent':
                return f'Неотправленный подарок (мероприятие #{event_id})' if event_id else 'Неотправленный подарок'
            if suffix == 'gift_sent':
                return f'Отправленный подарок (мероприятие #{event_id})' if event_id else 'Отправленный подарок'
            if suffix == 'order_bonus':
                return f'Очередность отправки подарка (мероприятие #{event_id})' if event_id else 'Очередность отправки подарка'
    if source.startswith('award:'):
        try:
            award_id = int(source.replace('award:', ''))
            conn = get_db_connection()
            award = conn.execute('SELECT title FROM awards WHERE id = ?', (award_id,)).fetchone()
            conn.close()
            if award:
                return f'Награда: {award["title"]}'
        except (ValueError, TypeError):
            pass
        return 'Награда'
    if source.startswith('title:'):
        try:
            title_id = int(source.replace('title:', ''))
            conn = get_db_connection()
            title = conn.execute('SELECT display_name FROM titles WHERE id = ?', (title_id,)).fetchone()
            conn.close()
            if title:
                return f'Звание: {title["display_name"]}'
        except (ValueError, TypeError):
            pass
        return 'Звание'
    return source


def recalculate_all_snowflake_events(conn, settings_dict):
    """Пересчитывает все существующие события snowflake_events на основе новых настроек"""
    # Получаем все активные события
    events = conn.execute('''
        SELECT id, user_id, source, points, active, manual_revoked
        FROM snowflake_events
        WHERE active = 1
    ''').fetchall()
    
    updated_count = 0
    created_count = 0
    created_count = 0
    
    # Обрабатываем существующие события
    for event in events:
        source = event['source']
        new_points = None
        
        # Определяем новые очки на основе source
        if source in ('telegram', 'whatsapp', 'viber'):
            # Контакты
            setting_key = f'rating_contact_{source}'
            if setting_key in settings_dict:
                new_points = int(settings_dict[setting_key])
        elif source.startswith('event:'):
            parts = source.split(':')
            if len(parts) >= 3:
                suffix = parts[2]
                if suffix == 'registration_bonus':
                    # Регистрация на мероприятие
                    if 'rating_event_registration' in settings_dict:
                        new_points = int(settings_dict['rating_event_registration'])
                elif suffix == 'gift_not_sent':
                    # Неотправленный подарок
                    if 'rating_event_gift_not_sent' in settings_dict:
                        new_points = int(settings_dict['rating_event_gift_not_sent'])
        elif source.startswith('award:'):
            # Награда
            try:
                award_id = int(source.replace('award:', ''))
                setting_key = f'rating_award_{award_id}'
                if setting_key in settings_dict:
                    new_points = int(settings_dict[setting_key])
                    log_debug(f"Found award event: source={source}, award_id={award_id}, setting_key={setting_key}, new_points={new_points}")
            except (ValueError, TypeError) as e:
                log_error(f"Error parsing award source {source}: {e}")
                pass
        elif source.startswith('title:'):
            # Звание
            try:
                title_id = int(source.replace('title:', ''))
                setting_key = f'rating_title_{title_id}'
                if setting_key in settings_dict:
                    new_points = int(settings_dict[setting_key])
            except (ValueError, TypeError):
                pass
        
        # Обновляем очки, если они изменились
        if new_points is not None and new_points != event['points']:
            conn.execute('''
                UPDATE snowflake_events
                SET points = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_points, event['id']))
            updated_count += 1
    
    # Создаем события для наград, которые назначены, но событий еще нет
    for key, value in settings_dict.items():
        if key.startswith('rating_award_'):
            try:
                award_id = int(key.replace('rating_award_', ''))
                points = int(value)
                
                # Получаем всех пользователей с этой наградой
                users_with_award = conn.execute('''
                    SELECT DISTINCT user_id FROM user_awards WHERE award_id = ?
                ''', (award_id,)).fetchall()
                
                log_debug(f"Found {len(users_with_award)} users with award {award_id}, points={points}")
                
                for user_row in users_with_award:
                    user_id = user_row['user_id']
                    source = f'award:{award_id}'
                    
                    # Проверяем, есть ли уже событие
                    existing = conn.execute('''
                        SELECT id, active FROM snowflake_events
                        WHERE user_id = ? AND source = ?
                    ''', (user_id, source)).fetchone()
                    
                    if not existing:
                        # Создаем новое событие
                        reason = f'Назначена награда (ID: {award_id})'
                        conn.execute('''
                            INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                            VALUES (?, ?, ?, ?, 1, 0)
                        ''', (user_id, source, reason, points))
                        created_count += 1
                        log_debug(f"Created new event for user {user_id}, award {award_id}, points {points}")
                    elif existing['active']:
                        # Обновляем существующее активное событие, если очки изменились
                        old_points_row = conn.execute('SELECT points FROM snowflake_events WHERE id = ?', (existing['id'],)).fetchone()
                        if old_points_row and old_points_row['points'] != points:
                            conn.execute('''
                                UPDATE snowflake_events
                                SET points = ?, updated_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                            ''', (points, existing['id']))
                            updated_count += 1
                            log_debug(f"Updated active event {existing['id']} for user {user_id}, award {award_id}: {old_points_row['points']} -> {points}")
                    elif not existing['active']:
                        # Активируем и обновляем существующее неактивное событие
                        conn.execute('''
                            UPDATE snowflake_events
                            SET points = ?, active = 1, manual_revoked = 0,
                                revoked_at = NULL, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (points, existing['id']))
                        updated_count += 1
                        log_debug(f"Reactivated event {existing['id']} for user {user_id}, award {award_id}, points {points}")
            except (ValueError, TypeError) as e:
                log_error(f"Error processing award setting {key}: {e}")
        
        elif key.startswith('rating_title_'):
            try:
                title_id = int(key.replace('rating_title_', ''))
                points = int(value)
                
                # Получаем всех пользователей с этим званием
                users_with_title = conn.execute('''
                    SELECT DISTINCT user_id FROM user_titles WHERE title_id = ?
                ''', (title_id,)).fetchall()
                
                for user_row in users_with_title:
                    user_id = user_row['user_id']
                    source = f'title:{title_id}'
                    
                    # Проверяем, есть ли уже событие
                    existing = conn.execute('''
                        SELECT id, active FROM snowflake_events
                        WHERE user_id = ? AND source = ?
                    ''', (user_id, source)).fetchone()
                    
                    if not existing:
                        # Создаем новое событие (даже если points = 0, чтобы было событие для будущих обновлений)
                        reason = f'Назначено звание (ID: {title_id})'
                        conn.execute('''
                            INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                            VALUES (?, ?, ?, ?, 1, 0)
                        ''', (user_id, source, reason, points))
                        created_count += 1
                    elif existing['active'] and existing['id']:
                        # Обновляем существующее активное событие
                        conn.execute('''
                            UPDATE snowflake_events
                            SET points = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (points, existing['id']))
                        updated_count += 1
                    elif not existing['active']:
                        # Активируем и обновляем существующее неактивное событие
                        conn.execute('''
                            UPDATE snowflake_events
                            SET points = ?, active = 1, manual_revoked = 0,
                                revoked_at = NULL, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        ''', (points, existing['id']))
                        updated_count += 1
            except (ValueError, TypeError) as e:
                log_error(f"Error processing title setting {key}: {e}")
    
    log_debug(f"Recalculated {updated_count} snowflake events, created {created_count} new events")
    return updated_count + created_count


def _load_rating_settings_dict(conn):
    rows = conn.execute(
        '''
        SELECT key, value FROM settings
        WHERE category = 'rating' OR key LIKE 'rating_%'
        '''
    ).fetchall()
    return {row['key']: row['value'] for row in rows}


def rating_cache_is_populated(conn) -> bool:
    row = conn.execute('SELECT COUNT(*) AS count FROM user_rating_cache').fetchone()
    return bool(row and row['count'] > 0)


def rebuild_rating_cache(conn) -> int:
    """Rebuild materialized rating cache. Returns number of rows."""
    conn.execute('DELETE FROM user_rating_cache')
    conn.execute(
        f'''
        INSERT INTO user_rating_cache (user_id, username, total_points, updated_at)
        SELECT
            u.user_id,
            u.username,
            COALESCE(SUM(CAST(se.points AS REAL)), 0.0),
            CURRENT_TIMESTAMP
        {_RATING_AGGREGATION_FROM}
        '''
    )
    row = conn.execute('SELECT COUNT(*) AS count FROM user_rating_cache').fetchone()
    return int(row['count']) if row else 0


def get_rating_page(conn, page: int, per_page: int):
    """Read rating page from cache. Returns (rows, total_count)."""
    total_count = conn.execute('SELECT COUNT(*) AS count FROM user_rating_cache').fetchone()['count']
    offset = (page - 1) * per_page
    rows = conn.execute(
        '''
        SELECT user_id, username, total_points
        FROM user_rating_cache
        ORDER BY total_points DESC, LOWER(username) ASC
        LIMIT ? OFFSET ?
        ''',
        (per_page, offset),
    ).fetchall()
    return rows, total_count


def get_live_rating_page(conn, page: int, per_page: int):
    """Fallback live aggregation when cache is empty."""
    total_count = conn.execute('SELECT COUNT(*) AS count FROM users').fetchone()['count']
    offset = (page - 1) * per_page
    rows = conn.execute(
        f'''
        SELECT
            u.user_id,
            u.username,
            COALESCE(SUM(CAST(se.points AS REAL)), 0.0) AS total_points
        {_RATING_AGGREGATION_FROM}
        ORDER BY total_points DESC, LOWER(u.username) ASC
        LIMIT ? OFFSET ?
        ''',
        (per_page, offset),
    ).fetchall()
    return rows, total_count


def queue_rating_recalc() -> None:
    set_setting(RATING_RECALC_PENDING_KEY, '1', category='rating')


def run_rating_maintenance() -> bool:
    """Run pending snowflake recalc and rebuild rating cache."""
    conn = get_db_connection()
    try:
        if get_setting(RATING_RECALC_PENDING_KEY, '0') == '1':
            settings_dict = _load_rating_settings_dict(conn)
            recalculate_all_snowflake_events(conn, settings_dict)
            conn.execute(
                '''
                UPDATE settings SET value = '0', updated_at = CURRENT_TIMESTAMP
                WHERE key = ?
                ''',
                (RATING_RECALC_PENDING_KEY,),
            )
            if conn.total_changes == 0:
                conn.execute(
                    '''
                    INSERT INTO settings (key, value, category)
                    VALUES (?, '0', 'rating')
                    ''',
                    (RATING_RECALC_PENDING_KEY,),
                )
        count = rebuild_rating_cache(conn)
        conn.commit()
        log_debug(f'Rating cache rebuilt: {count} rows')
        return True
    except Exception as exc:
        log_error(f'run_rating_maintenance failed: {exc}')
        conn.rollback()
        return False
    finally:
        conn.close()
