"""Admin: dashboard."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp
from gwadm.services.admin_dashboard import get_admin_dashboard_stats

@bp.route('/')
@require_role('admin')
def admin_panel():
    """Главная страница админ-панели"""
    stats = get_admin_dashboard_stats()
    return render_template('admin/dashboard.html', stats=stats)
