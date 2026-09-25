"""User presence: last_seen updates and online counters."""

from datetime import datetime, timedelta

from flask import session

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug
from gwadm.migrations.runner import column_exists

ONLINE_WINDOW_SECONDS = 3600
PRESENCE_TOUCH_INTERVAL_SECONDS = 300
SESSION_PRESENCE_KEY = '_presence_touched_at'


def _parse_timestamp(value) -> datetime | None:
    if not value:
        return None
    text = str(value).split('.')[0]
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _presence_column(conn) -> str:
    if column_exists(conn, 'users', 'last_seen'):
        return 'last_seen'
    return 'last_login'


def touch_user_presence(user_id, *, force: bool = False, conn=None) -> None:
    """Update users.last_seen (throttled unless force=True)."""
    if user_id is None:
        return

    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()

    presence_col = _presence_column(conn)
    if presence_col != 'last_seen':
        if own_conn:
            conn.close()
        return

    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        if own_conn:
            conn.close()
        return

    if not force:
        row = conn.execute(
            'SELECT last_seen FROM users WHERE user_id = ?',
            (user_id_int,),
        ).fetchone()
        if row and row['last_seen']:
            last_seen = _parse_timestamp(row['last_seen'])
            if last_seen and (datetime.now() - last_seen).total_seconds() < PRESENCE_TOUCH_INTERVAL_SECONDS:
                if own_conn:
                    conn.close()
                return

    conn.execute(
        'UPDATE users SET last_seen = ? WHERE user_id = ?',
        (_now_str(), user_id_int),
    )
    conn.commit()
    if own_conn:
        conn.close()


def count_online_users(conn=None) -> int:
    """Users active within ONLINE_WINDOW_SECONDS (last_seen or last_login)."""
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()

    cutoff = (datetime.now() - timedelta(seconds=ONLINE_WINDOW_SECONDS)).strftime('%Y-%m-%d %H:%M:%S')
    if column_exists(conn, 'users', 'last_seen'):
        query = '''
            SELECT COUNT(*) as count
            FROM users
            WHERE COALESCE(last_seen, last_login) >= ?
        '''
    else:
        query = '''
            SELECT COUNT(*) as count
            FROM users
            WHERE last_login >= ?
        '''
    row = conn.execute(query, (cutoff,)).fetchone()

    if own_conn:
        conn.close()
    return int(row['count'] or 0)


def get_user_presence_status(last_seen=None, last_login=None) -> str:
    """Return display status for participants list."""
    active_at = _parse_timestamp(last_seen) or _parse_timestamp(last_login)
    if not active_at:
        return 'Оффлайн'

    delta = (datetime.now() - active_at).total_seconds()
    if delta < ONLINE_WINDOW_SECONDS:
        return 'Онлайн'
    if active_at.date() == datetime.now().date():
        return 'Был сегодня'
    return 'Оффлайн'


def register_presence_tracking(app) -> None:
    """Touch last_seen on authenticated requests (throttled via session)."""
    import time

    from flask import request

    @app.before_request
    def _touch_user_presence():
        if request.endpoint and str(request.endpoint).startswith('static'):
            return None

        user_id = session.get('user_id')
        if not user_id:
            return None

        touched_at = session.get(SESSION_PRESENCE_KEY)
        if touched_at is not None:
            try:
                if time.time() - float(touched_at) < PRESENCE_TOUCH_INTERVAL_SECONDS:
                    return None
            except (TypeError, ValueError):
                pass

        try:
            touch_user_presence(user_id)
            session[SESSION_PRESENCE_KEY] = time.time()
        except Exception as exc:
            log_debug(f'presence touch failed for user {user_id}: {exc}')

        return None
