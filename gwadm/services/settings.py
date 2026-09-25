"""Application settings stored in SQLite."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_error


def get_setting(key, default=None):
    """Получает значение настройки из БД."""
    try:
        conn = get_db_connection()
        setting = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
        conn.close()
        return setting['value'] if setting and setting['value'] else default
    except Exception as e:
        log_error(f"Error getting setting {key}: {e}")
        return default


def set_setting(key, value, category=None):
    """Set or update a setting in the database."""
    try:
        conn = get_db_connection()
        existing = conn.execute('SELECT key FROM settings WHERE key = ?', (key,)).fetchone()
        if existing:
            conn.execute(
                'UPDATE settings SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?',
                (str(value), key),
            )
        else:
            columns = [
                col[1] for col in conn.execute('PRAGMA table_info(settings)').fetchall()
            ]
            if 'category' in columns and category:
                conn.execute(
                    'INSERT INTO settings (key, value, category) VALUES (?, ?, ?)',
                    (key, str(value), category),
                )
            else:
                conn.execute(
                    'INSERT INTO settings (key, value) VALUES (?, ?)',
                    (key, str(value)),
                )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error setting {key}: {e}")
        return False


def get_rating_setting(key, default=1):
    """Получает настройку рейтинга как целое число."""
    try:
        value = get_setting(key, str(default))
        return int(value) if value else default
    except (ValueError, TypeError):
        return default
