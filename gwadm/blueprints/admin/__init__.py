"""Admin blueprint package."""

from flask import Blueprint

bp = Blueprint("admin", __name__, url_prefix="/admin")

from gwadm.blueprints.admin import (  # noqa: E402,F401
    assignments_admin,
    awards,
    broadcasts,
    dashboard,
    events,
    faq,
    logs,
    rating,
    roles,
    rules,
    settings,
    telegram_menu,
    test,
    titles,
    users,
)
