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
    """Register application blueprints."""
    from gwadm.blueprints.auth import bp as auth_bp
    from gwadm.blueprints.meta import bp as meta_bp
    from gwadm.blueprints.profile import bp as profile_bp
    from gwadm.blueprints.public import bp as public_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(meta_bp)
    app.register_blueprint(public_bp)
    app.register_blueprint(profile_bp)
    from gwadm.blueprints.events import bp as events_bp
    from gwadm.blueprints.assignments import bp as assignments_bp
    from gwadm.blueprints.admin import bp as admin_bp
    from gwadm.blueprints.admin.impersonation import impersonation_bp
    from gwadm.blueprints.integrations import bp as integrations_bp

    app.register_blueprint(events_bp)
    app.register_blueprint(assignments_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(impersonation_bp)
    app.register_blueprint(integrations_bp)
