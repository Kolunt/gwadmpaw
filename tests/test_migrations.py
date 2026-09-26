"""Database migration tests."""

import os
import sqlite3

import gwadm.db as gwadm_db
from gwadm.migrations.runner import CURRENT_VERSION


def _reset_db_module(db_path: str) -> None:
    import gwadm.config as config

    os.environ["DATABASE_PATH"] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None


def test_fresh_database_applies_all_migrations(tmp_path):
    db_path = str(tmp_path / "fresh.db")
    _reset_db_module(db_path)
    gwadm_db.ensure_db()

    conn = sqlite3.connect(db_path)
    version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
    users = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
    ).fetchone()
    settings = conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0]
    last_seen_col = conn.execute("PRAGMA table_info(users)").fetchall()
    hero_whispers = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hero_whispers'"
    ).fetchone()
    conn.close()

    assert version == CURRENT_VERSION
    assert users is not None
    assert settings > 0
    assert any(row[1] == 'last_seen' for row in last_seen_col)
    assert hero_whispers is not None


def test_legacy_database_is_stamped_without_rerunning_alters(tmp_path):
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        '''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            username TEXT NOT NULL
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL,
            value TEXT
        )
        '''
    )
    conn.commit()
    conn.close()

    _reset_db_module(db_path)
    gwadm_db.ensure_db()

    conn = sqlite3.connect(db_path)
    version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
    schema_tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    conn.close()

    assert version == CURRENT_VERSION
    table_names = {row[0] for row in schema_tables}
    assert {"settings", "schema_version", "users"}.issubset(table_names)
    assert "roles" not in table_names
