"""Admin: logs."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/logs')
@require_role('admin')
def admin_logs():
    """Отображение действий пользователей."""
    limit = request.args.get('limit', type=int)
    user_filter = request.args.get('user_id', type=int)
    action_filter = request.args.get('action', '').strip()
    
    if not limit or limit <= 0:
        limit = 200
    limit = max(50, min(limit, 1000))
    
    conn = get_db_connection()
    params = []
    where_clauses = []
    
    if user_filter:
        where_clauses.append('user_id = ?')
        params.append(user_filter)
    
    if action_filter:
        where_clauses.append('action LIKE ?')
        params.append(f'%{action_filter}%')
    
    query = '''
        SELECT id, user_id, username, action, details, metadata, ip_address, created_at
        FROM activity_logs
    '''
    if where_clauses:
        query += ' WHERE ' + ' AND '.join(where_clauses)
    query += ' ORDER BY created_at DESC LIMIT ?'
    params.append(limit)
    
    rows = conn.execute(query, params).fetchall()
    conn.close()
    
    logs = []
    for row in rows:
        item = dict(row)
        metadata_value = item.get('metadata')
        if metadata_value:
            try:
                item['metadata'] = json.loads(metadata_value)
            except (json.JSONDecodeError, TypeError):
                item['metadata'] = metadata_value
        else:
            item['metadata'] = None
        logs.append(item)
    
    return render_template('admin/logs.html', logs=logs, limit=limit, user_filter=user_filter, action_filter=action_filter)
