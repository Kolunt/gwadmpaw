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


def get_rating_setting(key, default=1):
    """Получает настройку рейтинга как целое число."""
    try:
        value = get_setting(key, str(default))
        return int(value) if value else default
    except (ValueError, TypeError):
        return default
