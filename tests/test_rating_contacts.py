"""Tests for contact snowflake sync (including Max)."""

import sqlite3
from unittest.mock import patch

from gwadm.services.rating import sync_user_contact_snowflakes


def _contact_conn(tmp_path):
    db_path = tmp_path / 'contacts.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        '''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            telegram TEXT,
            whatsapp TEXT,
            viber TEXT,
            max TEXT
        );
        CREATE TABLE snowflake_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT,
            points INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            manual_revoked INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            revoked_at TIMESTAMP
        );
        CREATE TABLE user_rating_cache (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            total_points REAL NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
    )
    return conn


def _rating_points(key, default=1):
    if key == 'rating_contact_max':
        return 3
    return default


def test_sync_user_contact_snowflakes_creates_max_event(tmp_path):
    conn = _contact_conn(tmp_path)
    conn.execute(
        "INSERT INTO users (user_id, username, max) VALUES (1, 'alice', '+79990001122')"
    )
    conn.commit()

    with patch('gwadm.services.rating.get_rating_setting', side_effect=_rating_points):
        sync_user_contact_snowflakes(conn, 1)
    conn.commit()

    row = conn.execute(
        '''
        SELECT source, active, points, reason
        FROM snowflake_events WHERE user_id = 1 AND source = 'max'
        '''
    ).fetchone()
    assert row is not None
    assert row['active'] == 1
    assert row['points'] == 3
    assert row['reason'] == 'Заполнен Max'
    conn.close()


def test_sync_user_contact_snowflakes_deactivates_cleared_max(tmp_path):
    conn = _contact_conn(tmp_path)
    conn.execute(
        "INSERT INTO users (user_id, username, max) VALUES (1, 'alice', '+79990001122')"
    )
    conn.execute(
        '''
        INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
        VALUES (1, 'max', 'Заполнен Max', 3, 1, 0)
        '''
    )
    conn.execute("UPDATE users SET max = NULL WHERE user_id = 1")
    conn.commit()

    sync_user_contact_snowflakes(conn, 1)
    conn.commit()

    row = conn.execute(
        "SELECT active FROM snowflake_events WHERE user_id = 1 AND source = 'max'"
    ).fetchone()
    assert row['active'] == 0
    conn.close()
