"""Application configuration loaded from environment variables."""

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

_DEFAULT_SECRET_KEY = 'dev-only-insecure-key'
_DEFAULT_GWARS_PASSWORD = 'deadmoroz'

SECRET_KEY = os.environ.get('SECRET_KEY', _DEFAULT_SECRET_KEY)
GWARS_PASSWORD = os.environ.get('GWARS_PASSWORD', _DEFAULT_GWARS_PASSWORD)
CRON_SECRET_TOKEN = os.environ.get('CRON_SECRET_TOKEN', '').strip()
FLASK_ENV = os.environ.get('FLASK_ENV', 'development').strip().lower()

ADMIN_USER_IDS = [283494, 240139]

try:
    EVENT_TIME_OFFSET_HOURS = int(os.getenv('EVENT_TIME_OFFSET_HOURS', '3'))
except ValueError:
    EVENT_TIME_OFFSET_HOURS = 0


def _resolve_database_path() -> str:
    explicit = os.environ.get('DATABASE_PATH', '').strip()
    if explicit:
        return explicit
    if os.path.exists('/home/gwadm'):
        return '/home/gwadm/gwadm/database.db'
    return str(ROOT_DIR / 'database.db')


DATABASE_PATH = _resolve_database_path()

AVATAR_CACHE_DIR = str(ROOT_DIR / 'static' / 'uploads' / 'avatars' / 'cache')


def is_production() -> bool:
    """True when running with production settings (SECRET_KEY or FLASK_ENV=production)."""
    env_secret = os.environ.get('SECRET_KEY', '').strip()
    if env_secret and env_secret != _DEFAULT_SECRET_KEY:
        return True
    return FLASK_ENV == 'production'


def is_debug() -> bool:
    """True for verbose debug logging (local dev or FLASK_DEBUG=1)."""
    if os.environ.get('FLASK_DEBUG', '').strip() in ('1', 'true', 'yes', 'on'):
        return True
    return not is_production()


def is_gwars_password_configured() -> bool:
    """True when GWARS_PASSWORD is explicitly set in environment."""
    return bool(os.environ.get('GWARS_PASSWORD', '').strip())


def is_dev_login_enabled() -> bool:
    """Whether /login/dev is allowed (localhost still required in route)."""
    flag = os.environ.get('ENABLE_DEV_LOGIN', '').strip().lower()
    if flag in ('0', 'false', 'no', 'off'):
        return False
    if flag in ('1', 'true', 'yes', 'on'):
        return True
    return not is_production()


def warn_insecure_defaults() -> None:
    """Log warnings for insecure defaults on production (does not block startup)."""
    if not is_production():
        return
    if not is_gwars_password_configured():
        import logging
        logging.getLogger(__name__).warning(
            'GWARS_PASSWORD is not set in environment; using built-in default. '
            'Set GWARS_PASSWORD in .env for production (see R-305).'
        )
    if SECRET_KEY == _DEFAULT_SECRET_KEY:
        import logging
        logging.getLogger(__name__).warning(
            'SECRET_KEY is not set; sessions are not secure across restarts.'
        )

LETTER_UPLOAD_RELATIVE = 'uploads/letter_attachments'
ASSIGNMENT_RECEIPT_RELATIVE = 'uploads/assignment_receipts'
ALLOWED_LETTER_IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp'}
ALLOWED_AWARD_IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp'}
MAX_UPLOAD_BYTES = 5 * 1024 * 1024
LETTER_UPLOAD_FOLDER = str(ROOT_DIR / 'static' / 'uploads' / 'letter_attachments')
ASSIGNMENT_RECEIPT_FOLDER = str(ROOT_DIR / 'static' / 'uploads' / 'assignment_receipts')

