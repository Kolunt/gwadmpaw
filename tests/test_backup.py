"""Tests for SQLite backup rotation."""

import importlib
import os
import sqlite3

import gwadm.config as config
import cron_tasks


def test_backup_creates_file_and_rotates(tmp_path, monkeypatch):
    db_path = tmp_path / 'database.db'
    backup_dir = tmp_path / 'backups'
    conn = sqlite3.connect(db_path)
    conn.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    conn.commit()
    conn.close()

    monkeypatch.setenv('DATABASE_PATH', str(db_path))
    monkeypatch.setenv('BACKUP_DIR', str(backup_dir))
    importlib.reload(config)
    importlib.reload(cron_tasks)

    for _ in range(cron_tasks.BACKUP_RETENTION_COUNT + 1):
        assert cron_tasks.backup_database() is True

    backups = sorted(backup_dir.glob('database_*.db'))
    assert len(backups) == cron_tasks.BACKUP_RETENTION_COUNT
    assert all(path.stat().st_size > 0 for path in backups)
