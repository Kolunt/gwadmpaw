"""Internationalization: DB-backed UI strings with static fallback."""

from flask import session

from gwadm.logging_config import log_error
from gwadm.ui_string_registry import UI_STRING_SEEDS

STATIC_FALLBACK_RU: dict[str, str] = {row[0]: row[2] for row in UI_STRING_SEEDS}
STATIC_FALLBACK_EN: dict[str, str] = {row[0]: row[3] for row in UI_STRING_SEEDS}


def get_locale():
    """Определяет текущую локаль. Для неавторизованных — русский."""
    try:
        from flask import current_app

        if 'user_id' in session:
            try:
                from gwadm.db import get_db_connection

                conn = get_db_connection()
                try:
                    user = conn.execute(
                        'SELECT language FROM users WHERE user_id = ?',
                        (session['user_id'],),
                    ).fetchone()
                finally:
                    conn.close()
                languages = current_app.config.get('LANGUAGES', {})
                if user and dict(user).get('language') and user['language'] in languages:
                    return user['language']
            except Exception as e:
                log_error(f"Error getting user language: {e}")
    except Exception:
        pass
    return 'ru'


def _(text: str) -> str:
    """Return translated string for msg_key."""
    try:
        loc = get_locale()
    except Exception:
        loc = 'ru'
    try:
        from gwadm.db import get_db_connection
        from gwadm.services.ui_strings import get_string

        return get_string(text, loc, get_db_connection, STATIC_FALLBACK_RU, STATIC_FALLBACK_EN)
    except RuntimeError:
        pass
    except Exception as e:
        log_error(f"ui translate error for {text!r}: {e}")
    if loc == 'en':
        return STATIC_FALLBACK_EN.get(text, text)
    return STATIC_FALLBACK_RU.get(text, text)


def format_date(date, format=None):
    """Форматирование даты (fallback)."""
    return str(date)


def format_datetime(datetime, format=None):
    """Форматирование даты и времени (fallback)."""
    return str(datetime)
