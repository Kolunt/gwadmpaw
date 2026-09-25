"""Admin: test."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/test')
def admin_test():
    """Тестовый маршрут для проверки загрузки админ-панели"""
    user_id = session.get('user_id')
    roles = session.get('roles', [])
    has_admin = has_role(user_id, 'admin') if user_id else False
    return f"""
    <h1>Admin Test Route</h1>
    <p>User ID: {user_id or 'Not logged in'}</p>
    <p>Session roles: {roles}</p>
    <p>Has admin role (check): {has_admin}</p>
    <p>User roles from DB: {get_user_roles(user_id) if user_id else 'N/A'}</p>
    <p><a href="/admin">Try /admin</a></p>
    <p><a href="/dashboard">Dashboard</a></p>
    """
