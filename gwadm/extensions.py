"""Flask extensions (Babel) and blueprint registration hook."""

from flask import Flask

from gwadm.i18n import get_locale
from gwadm.logging_config import log_error

babel = None
BABEL_AVAILABLE = False


def init_extensions(app: Flask) -> None:
    """Initialize Flask-Babel and other extensions on the app."""
    global babel, BABEL_AVAILABLE
    try:
        from flask_babel import Babel

        babel = Babel()
        babel.init_app(app, locale_selector=get_locale)
        BABEL_AVAILABLE = True
    except ImportError:
        babel = None
        BABEL_AVAILABLE = False
    except Exception as e:
        babel = None
        BABEL_AVAILABLE = False
        log_error(f"Error initializing Babel: {e}")


def register_blueprints(app: Flask) -> None:
    """Register blueprints (R-202+). Routes remain in app.py until then."""
    pass
