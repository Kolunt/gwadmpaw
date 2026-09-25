"""Application factory."""

import os

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from gwadm import db as gwadm_db
from gwadm.config import ROOT_DIR, SECRET_KEY, is_production
from gwadm.db import ensure_db
from gwadm.extensions import init_extensions, register_blueprints
from gwadm.logging_config import log_error, setup_logging
from version import __version__


def _ensure_upload_dirs(app: Flask) -> None:
    """Create upload directories under static/."""
    static = app.static_folder
    if not static:
        return
    for subpath in (
        'uploads/letter_attachments',
        'uploads/assignment_receipts',
        'uploads/avatars/cache',
    ):
        os.makedirs(os.path.join(static, subpath), exist_ok=True)


def _init_database() -> None:
    try:
        ensure_db()
    except Exception as e:
        log_error(f"Failed to initialize database on startup: {e}")
        if gwadm_db._database_is_ready():
            gwadm_db._db_initialized = True


def create_app() -> Flask:
    """Create and configure the Flask application."""
    setup_logging()

    app = Flask(
        __name__,
        template_folder=str(ROOT_DIR / 'templates'),
        static_folder=str(ROOT_DIR / 'static'),
    )
    app.secret_key = SECRET_KEY
    app.config['VERSION'] = __version__
    app.config['LANGUAGES'] = {
        'ru': 'Русский',
        'en': 'English',
    }
    app.config['BABEL_DEFAULT_LOCALE'] = 'ru'
    app.config['BABEL_DEFAULT_TIMEZONE'] = 'Europe/Moscow'
    app.config['BABEL_TRANSLATION_DIRECTORIES'] = str(ROOT_DIR / 'translations')

    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    if is_production():
        app.config['SESSION_COOKIE_SECURE'] = True
        app.config['SESSION_COOKIE_HTTPONLY'] = True
        app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

    _ensure_upload_dirs(app)
    init_extensions(app)
    register_blueprints(app)
    _init_database()

    return app
