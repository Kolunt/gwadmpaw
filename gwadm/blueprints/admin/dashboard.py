"""Admin: dashboard."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/')
@require_role('admin')
def admin_panel():
    """Главная страница админ-панели"""
    return render_template('admin/index.html')
