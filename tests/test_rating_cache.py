"""Tests for materialized rating cache."""

import sqlite3

from gwadm.services.rating import (
    get_live_rating_page,
    get_rating_page,
    rating_cache_is_populated,
    rebuild_rating_cache,
)


def _seed_rating_data(conn):
    conn.execute(
        '''
        INSERT INTO users (user_id, username, email)
        VALUES (1, 'alice', 'a@example.com'), (2, 'bob', 'b@example.com')
        '''
    )
    conn.execute(
        '''
        INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
        VALUES (1, 'telegram', 'tg', 5, 1, 0),
               (2, 'telegram', 'tg', 2, 1, 0)
        '''
    )
    conn.commit()


def test_rebuild_and_read_rating_cache(tmp_path):
    db_path = tmp_path / 'rating.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT
        );
        CREATE TABLE snowflake_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT,
            points INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            manual_revoked INTEGER DEFAULT 0
        );
        CREATE TABLE user_rating_cache (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            total_points REAL NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    _seed_rating_data(conn)

    assert rating_cache_is_populated(conn) is False
    count = rebuild_rating_cache(conn)
    conn.commit()
    assert count == 2
    assert rating_cache_is_populated(conn) is True

    rows, total = get_rating_page(conn, 1, 10)
    assert total == 2
    assert rows[0]['username'] == 'alice'
    assert float(rows[0]['total_points']) == 5.0
    conn.close()


def test_live_rating_fallback(tmp_path):
    db_path = tmp_path / 'rating_live.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT
        );
        CREATE TABLE snowflake_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT,
            points INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            manual_revoked INTEGER DEFAULT 0
        );
        CREATE TABLE user_rating_cache (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            total_points REAL NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    _seed_rating_data(conn)

    rows, total = get_live_rating_page(conn, 1, 10)
    assert total == 2
    assert rows[0]['username'] == 'alice'
    conn.close()
