"""Tests for SQLite audit script."""

import os
import sqlite3
from unittest.mock import patch

from gwadm.services.sqlite_audit import run_sqlite_audit


def test_sqlite_audit_on_temp_db(tmp_path, monkeypatch):
    db_path = tmp_path / 'audit.db'
    conn = sqlite3.connect(db_path)
    conn.executescript('''
        CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT);
        CREATE TABLE snowflake_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            source TEXT,
            points INTEGER,
            active INTEGER,
            manual_revoked INTEGER
        );
        CREATE TABLE user_rating_cache (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            total_points REAL,
            updated_at TIMESTAMP
        );
        CREATE TABLE broadcast_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT
        );
    ''')
    conn.commit()
    conn.close()

    monkeypatch.setenv('DATABASE_PATH', str(db_path))
    import importlib
    import gwadm.config as config
    import gwadm.db as gwadm_db

    importlib.reload(config)
    config.DATABASE_PATH = str(db_path)
    gwadm_db._db_path = None

    with patch('gwadm.services.sqlite_audit.get_db_connection') as mock_conn:
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        mock_conn.return_value = c
        lines, has_warn = run_sqlite_audit()

    assert any('database size' in line for line in lines)
    assert has_warn is True
    assert any('rating cache is empty' in line for line in lines)
