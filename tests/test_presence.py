"""User presence (online counter) tests."""

import os
from datetime import datetime, timedelta

import gwadm.db as gwadm_db
from gwadm.db import get_db_connection
from gwadm.services.presence import (
    ONLINE_WINDOW_SECONDS,
    count_online_users,
    get_user_presence_status,
    touch_user_presence,
)


def _reset_db_module(db_path: str) -> None:
    import gwadm.config as config

    os.environ['DATABASE_PATH'] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None


def _seed_user(conn, user_id=9001, username='presence_user'):
    conn.execute(
        'INSERT INTO users (user_id, username) VALUES (?, ?)',
        (user_id, username),
    )
    conn.commit()


def test_touch_user_presence_updates_last_seen(tmp_path):
    _reset_db_module(str(tmp_path / 'presence.db'))
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_user(conn)
    touch_user_presence(9001, force=True, conn=conn)
    row = conn.execute('SELECT last_seen FROM users WHERE user_id = 9001').fetchone()
    conn.close()
    assert row['last_seen'] is not None


def test_touch_user_presence_throttled_without_force(tmp_path):
    _reset_db_module(str(tmp_path / 'presence2.db'))
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_user(conn, user_id=9002, username='throttle_user')
    touch_user_presence(9002, force=True, conn=conn)
    first = conn.execute('SELECT last_seen FROM users WHERE user_id = 9002').fetchone()['last_seen']
    touch_user_presence(9002, conn=conn)
    second = conn.execute('SELECT last_seen FROM users WHERE user_id = 9002').fetchone()['last_seen']
    conn.close()
    assert first == second


def test_count_online_users_uses_last_seen(tmp_path):
    _reset_db_module(str(tmp_path / 'presence3.db'))
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_user(conn, user_id=9003, username='online_user')
    recent = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    old = (datetime.now() - timedelta(seconds=ONLINE_WINDOW_SECONDS + 60)).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('UPDATE users SET last_seen = ? WHERE user_id = 9003', (recent,))
    _seed_user(conn, user_id=9004, username='offline_user')
    conn.execute('UPDATE users SET last_seen = ? WHERE user_id = 9004', (old,))
    conn.commit()
    count = count_online_users(conn)
    conn.close()
    assert count >= 1


def test_get_user_presence_status():
    recent = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    old = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
    assert get_user_presence_status(recent) == 'Онлайн'
    assert get_user_presence_status(old) == 'Оффлайн'


def test_index_online_count_includes_active_session(client):
    import re

    client.get('/login/dev')
    response = client.get('/')
    assert response.status_code == 200

    conn = get_db_connection()
    expected = count_online_users(conn)
    conn.close()
    assert expected >= 1

    body = response.get_data(as_text=True)
    match = re.search(
        r'stat-number">\s*(\d+)\s*</div>\s*<div class="stat-label">Онлайн',
        body,
    )
    assert match is not None
    assert int(match.group(1)) == expected
