"""Event stages, registration counts, and snowflake side-effects on stage transitions."""

from datetime import datetime, timedelta, timezone

from gwadm.config import EVENT_TIME_OFFSET_HOURS
from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.settings import get_rating_setting

EVENT_STAGES = [
    {'type': 'pre_registration', 'name': 'Предварительная регистрация', 'required': False, 'has_start': True, 'has_end': False},
    {'type': 'main_registration', 'name': 'Основная регистрация', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'registration_closed', 'name': 'Закрытие регистрации', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'lottery', 'name': 'Жеребьёвка', 'required': False, 'has_start': False, 'has_end': False},
    {'type': 'celebration_date', 'name': 'Обмен подарками', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'after_party', 'name': 'Мероприятие завершено', 'required': True, 'has_start': False, 'has_end': True},
]


def get_event_now():
    return datetime.utcnow() + timedelta(hours=EVENT_TIME_OFFSET_HOURS)


def parse_event_datetime(value):
    """Безопасно парсит сохранённые в БД даты этапов в объект datetime."""
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    value_str = str(value).strip()
    if not value_str:
        return None

    formats = (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%d %H:%M',
    )

    for fmt in formats:
        try:
            return datetime.strptime(value_str, fmt)
        except ValueError:
            continue

    try:
        result = datetime.fromisoformat(value_str)
        if result.tzinfo is not None:
            result = result.astimezone(timezone.utc).replace(tzinfo=None)
            if EVENT_TIME_OFFSET_HOURS:
                result += timedelta(hours=EVENT_TIME_OFFSET_HOURS)
        return result
    except ValueError:
        return None


def get_current_event_stage(event_id):
    """Определяет текущий этап мероприятия на основе текущей даты."""
    conn = get_db_connection()
    stage_rows = conn.execute('''
        SELECT * FROM event_stages
        WHERE event_id = ?
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()

    stages = [dict(row) for row in stage_rows]
    for stage in stages:
        if (
            stage.get('stage_type') == 'after_party'
            and not stage.get('start_datetime')
            and stage.get('end_datetime')
        ):
            stage['start_datetime'] = stage['end_datetime']

    if not stages:
        return None

    now = get_event_now()

    registration_closed_stage = None
    for stage in stages:
        if stage['stage_type'] == 'registration_closed' and stage['start_datetime']:
            try:
                start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%dT%H:%M')
                except ValueError:
                    continue
            if now >= start_dt:
                registration_closed_stage = stage
                break

    if registration_closed_stage:
        create_participant_approvals_for_event(event_id)

    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    stages_info_dict = {stage['type']: stage for stage in EVENT_STAGES}

    current_stage = None

    for stage_info in EVENT_STAGES:
        stage_type = stage_info['type']
        if stage_type not in stages_dict:
            continue

        stage = dict(stages_dict[stage_type])

        if stage['start_datetime']:
            try:
                start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%dT%H:%M')
                except ValueError:
                    log_debug(f"get_current_event_stage: cannot parse start_datetime for stage {stage_type}: {stage['start_datetime']}")
                    continue

            if now < start_dt:
                log_debug(f"get_current_event_stage: stage {stage_type} not started yet (start: {start_dt}, now: {now})")
                continue

        if stage['end_datetime']:
            try:
                end_dt = datetime.strptime(stage['end_datetime'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    end_dt = datetime.strptime(stage['end_datetime'], '%Y-%m-%dT%H:%M')
                except ValueError:
                    end_dt = None
            if stage_type == 'after_party':
                end_dt = None

            if end_dt and now > end_dt:
                continue

        current_order = stage['stage_order']
        next_stage_started = False
        for next_stage in stages:
            if next_stage['stage_order'] > current_order and next_stage['start_datetime']:
                try:
                    next_start_dt = datetime.strptime(next_stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        next_start_dt = datetime.strptime(next_stage['start_datetime'], '%Y-%m-%dT%H:%M')
                    except ValueError:
                        continue
                if now >= next_start_dt:
                    next_stage_started = True
                    log_debug(f"get_current_event_stage: stage {stage_type} ended because next stage {next_stage['stage_type']} started at {next_start_dt}")
                    break

        if next_stage_started:
            continue

        current_stage = {
            'data': stage,
            'info': stage_info
        }
        log_debug(f"get_current_event_stage: found active stage {stage_type} for event {event_id}")
        break

    if not current_stage:
        log_debug(f"get_current_event_stage: no active stage found for event {event_id}")

    return current_stage


def get_event_registrations_count(event_id):
    """Получает количество зарегистрированных пользователей на мероприятие."""
    conn = get_db_connection()
    count = conn.execute('''
        SELECT COUNT(*) as count FROM event_registrations
        WHERE event_id = ?
    ''', (event_id,)).fetchone()
    conn.close()
    return count['count'] if count else 0


def get_event_stages(event_id):
    """Возвращает список этапов мероприятия в порядке их следования."""
    conn = get_db_connection()
    try:
        stage_rows = conn.execute('''
            SELECT stage_type, stage_order, start_datetime, end_datetime
            FROM event_stages
            WHERE event_id = ?
            ORDER BY stage_order
        ''', (event_id,)).fetchall()
    finally:
        conn.close()
    stages = []
    for row in stage_rows:
        stage = dict(row)
        if (
            stage.get('stage_type') == 'after_party'
            and not stage.get('start_datetime')
            and stage.get('end_datetime')
        ):
            stage['start_datetime'] = stage['end_datetime']
        stages.append(stage)
    return stages


def create_participant_approvals_for_event(event_id):
    """Создает записи для ревью участников при закрытии регистрации."""
    conn = get_db_connection()
    try:
        _revoke_gift_events(conn, event_id)

        registrations = conn.execute('''
            SELECT user_id FROM event_registrations WHERE event_id = ?
        ''', (event_id,)).fetchall()

        for reg in registrations:
            conn.execute('''
                INSERT OR IGNORE INTO event_participant_approvals
                (event_id, user_id, approved)
                VALUES (?, ?, 0)
            ''', (event_id, reg['user_id']))
            _ensure_registration_bonus_event(conn, event_id, reg['user_id'])
            _ensure_gift_not_sent_event(conn, event_id, reg['user_id'])
            _ensure_gift_sent_event(conn, event_id, reg['user_id'])

        _ensure_order_bonus_events(conn, event_id)

        conn.commit()
        log_debug(f"Created participant approvals for event {event_id}")
    except Exception as e:
        log_error(f"Error creating participant approvals: {e}")
        conn.rollback()
    finally:
        conn.close()


def _ensure_registration_bonus_event(conn, event_id, user_id):
    source = f'event:{event_id}:registration_bonus'
    reason = f'Регистрация закрыта: мероприятие #{event_id}'
    event_row = conn.execute('SELECT rating_registration FROM events WHERE id = ?', (event_id,)).fetchone()
    if event_row and event_row['rating_registration'] is not None:
        points = int(event_row['rating_registration'])
    else:
        points = get_rating_setting('rating_event_registration', 1)
    existing = conn.execute(
        '''
        SELECT id, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ? AND source = ?
        ''',
        (user_id, source)
    ).fetchone()
    if not existing:
        conn.execute(
            '''
            INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (user_id, source, reason, points, 1, 0)
        )
    elif not existing['active']:
        conn.execute(
            '''
            UPDATE snowflake_events
            SET active = 1,
                points = ?,
                manual_revoked = 0,
                revoked_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (points, existing['id'])
        )


def _ensure_gift_not_sent_event(conn, event_id, user_id):
    """Создает или активирует событие бубенчика за неотправленный подарок при закрытии регистрации."""
    source = f'event:{event_id}:gift_not_sent'
    reason = f'Неотправленный подарок при закрытии регистрации: мероприятие #{event_id}'
    event_row = conn.execute('SELECT rating_gift_not_sent FROM events WHERE id = ?', (event_id,)).fetchone()
    if event_row and event_row['rating_gift_not_sent'] is not None:
        points = int(event_row['rating_gift_not_sent'])
    else:
        points = get_rating_setting('rating_event_gift_not_sent', 0)

    if points == 0:
        return

    assignment = conn.execute('''
        SELECT id, santa_sent_at FROM event_assignments
        WHERE event_id = ? AND santa_user_id = ?
    ''', (event_id, user_id)).fetchone()

    if not assignment or assignment['santa_sent_at']:
        return

    existing = conn.execute(
        '''
        SELECT id, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ? AND source = ?
        ''',
        (user_id, source)
    ).fetchone()

    if not existing:
        conn.execute(
            '''
            INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (user_id, source, reason, points, 1, 0)
        )
    elif not existing['active']:
        conn.execute(
            '''
            UPDATE snowflake_events
            SET active = 1,
                points = ?,
                manual_revoked = 0,
                revoked_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (points, existing['id'])
        )


def _ensure_gift_sent_event(conn, event_id, user_id):
    """Создает или активирует событие бубенчика за отправленный подарок при закрытии регистрации."""
    source = f'event:{event_id}:gift_sent'
    reason = f'Отправленный подарок при закрытии регистрации: мероприятие #{event_id}'
    event_row = conn.execute('SELECT rating_gift_sent FROM events WHERE id = ?', (event_id,)).fetchone()
    if event_row and event_row['rating_gift_sent'] is not None:
        points = int(event_row['rating_gift_sent'])
    else:
        points = get_rating_setting('rating_event_gift_sent', 0)

    if points == 0:
        return

    assignment = conn.execute('''
        SELECT id, santa_sent_at FROM event_assignments
        WHERE event_id = ? AND santa_user_id = ?
    ''', (event_id, user_id)).fetchone()

    if not assignment or not assignment['santa_sent_at']:
        return

    existing = conn.execute(
        '''
        SELECT id, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ? AND source = ?
        ''',
        (user_id, source)
    ).fetchone()

    if not existing:
        conn.execute(
            '''
            INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (user_id, source, reason, points, 1, 0)
        )
    elif not existing['active']:
        conn.execute(
            '''
            UPDATE snowflake_events
            SET active = 1,
                points = ?,
                manual_revoked = 0,
                revoked_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (points, existing['id'])
        )


def _ensure_order_bonus_events(conn, event_id):
    """Создает или обновляет события бубенчиков за очередность отправки подарка при закрытии регистрации."""
    source = f'event:{event_id}:order_bonus'

    event_row = conn.execute('SELECT rating_order_coefficient FROM events WHERE id = ?', (event_id,)).fetchone()
    if not event_row or event_row['rating_order_coefficient'] is None or event_row['rating_order_coefficient'] == 0:
        return

    coefficient = float(event_row['rating_order_coefficient'])

    total_participants = conn.execute('''
        SELECT COUNT(DISTINCT santa_user_id) as total
        FROM event_assignments
        WHERE event_id = ?
    ''', (event_id,)).fetchone()

    if not total_participants or total_participants['total'] == 0:
        return

    total = total_participants['total']

    sent_assignments = conn.execute('''
        SELECT DISTINCT santa_user_id, santa_sent_at
        FROM event_assignments
        WHERE event_id = ? AND santa_sent_at IS NOT NULL
        ORDER BY santa_sent_at ASC
    ''', (event_id,)).fetchall()

    if not sent_assignments:
        return

    for idx, assignment in enumerate(sent_assignments, start=1):
        user_id = assignment['santa_user_id']
        order_num = idx

        points = float((total - order_num + 1) * coefficient)

        if points <= 0:
            continue

        reason = f'Очередность отправки подарка: {order_num}-й из {total} (мероприятие #{event_id})'

        existing = conn.execute('''
            SELECT id, active, manual_revoked
            FROM snowflake_events
            WHERE user_id = ? AND source = ?
        ''', (user_id, source)).fetchone()

        if not existing:
            conn.execute('''
                INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                VALUES (?, ?, ?, ?, 1, 0)
            ''', (user_id, source, reason, points))
        elif not existing['active']:
            conn.execute('''
                UPDATE snowflake_events
                SET active = 1,
                    points = ?,
                    reason = ?,
                    manual_revoked = 0,
                    revoked_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (points, reason, existing['id']))
        else:
            conn.execute('''
                UPDATE snowflake_events
                SET points = ?,
                    reason = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (points, reason, existing['id']))


def _revoke_gift_events(conn, event_id):
    """Снимает бубенчики за отправленный/неотправленный подарок и за очередность при продлении мероприятия."""
    source_not_sent = f'event:{event_id}:gift_not_sent'
    conn.execute('''
        UPDATE snowflake_events
        SET active = 0,
            revoked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE source = ? AND active = 1
    ''', (source_not_sent,))

    source_sent = f'event:{event_id}:gift_sent'
    conn.execute('''
        UPDATE snowflake_events
        SET active = 0,
            revoked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE source = ? AND active = 1
    ''', (source_sent,))

    source_order = f'event:{event_id}:order_bonus'
    conn.execute('''
        UPDATE snowflake_events
        SET active = 0,
            revoked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE source = ? AND active = 1
    ''', (source_order,))
