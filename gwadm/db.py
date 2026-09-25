"""SQLite database connection and schema initialization."""

import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from gwadm.logging_config import log_debug, log_error

_db_initialized = False
_db_path = None
_db_init_lock = threading.Lock()


def get_db_path():
    """Return path to the SQLite database file."""
    global _db_path
    if _db_path is None:
        from gwadm.config import DATABASE_PATH

        _db_path = DATABASE_PATH
    return _db_path


def _database_is_ready():
    """Проверяет, что БД уже инициализирована и доступна для чтения."""
    conn = None
    try:
        conn = sqlite3.connect(get_db_path(), timeout=30)
        conn.execute('SELECT 1 FROM settings LIMIT 1')
        return True
    except Exception:
        return False
    finally:
        if conn:
            conn.close()


def init_db():
    """Инициализирует базу данных, создавая таблицы если их нет."""
    global _db_initialized
    if _db_initialized:
        return

    conn = None
    try:
        db_path = get_db_path()
        log_debug(f"Initializing database at: {db_path}")

        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')

        from gwadm.migrations.runner import run_migrations

        run_migrations(conn)
        conn.commit()
        _db_initialized = True
        log_debug(f"Database initialized successfully at: {db_path}")
    except sqlite3.OperationalError as e:
        if 'locked' in str(e).lower() and _database_is_ready():
            _db_initialized = True
            log_debug("Database already initialized by another worker")
        else:
            log_error(f"Error initializing database: {e}")
            raise
    except Exception as e:
        log_error(f"Error initializing database: {e}")
        raise
    finally:
        if conn:
            conn.close()


def ensure_db():
    """Убеждается, что база данных инициализирована"""
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
        for attempt in range(5):
            try:
                init_db()
                return
            except sqlite3.OperationalError as e:
                if 'locked' not in str(e).lower():
                    raise
                if _database_is_ready():
                    _db_initialized = True
                    return
                if attempt < 4:
                    time.sleep(0.25 * (attempt + 1))
                    continue
                raise


def is_database_initialized() -> bool:
    """Return True after ensure_db() has completed in this process."""
    return _db_initialized


def get_db_connection():
    """Получает соединение с базой данных"""
    if not _db_initialized:
        raise RuntimeError(
            'Database is not initialized. Call ensure_db() at application or cron startup.'
        )
    db_path = get_db_path()
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


@contextmanager
def get_db():
    """Context manager for a database connection (caller commits explicitly)."""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()
