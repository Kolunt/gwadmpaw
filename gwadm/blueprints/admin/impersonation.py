"""Admin impersonation."""

from flask import Blueprint
from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug
from gwadm.blueprints.admin import bp

impersonation_bp = Blueprint('impersonation', __name__)

@bp.route('/users/<int:user_id>/impersonate', methods=['POST'])
@require_role('admin')
def admin_user_impersonate(user_id):
    """Позволяет администратору управлять выбранным пользователем"""
    # Проверяем, не активна ли уже импровизированная сессия
    if session.get('impersonation_original'):
        flash('Вы уже управляете другим пользователем. Завершите текущую сессию управления сначала.', 'warning')
        next_url = request.form.get('next')
        if not next_url or not next_url.startswith('/'):
            next_url = url_for('profile.view_profile', user_id=user_id)
        return redirect(next_url)
    
    # Если администратор пытается управлять собой
    if session.get('user_id') == user_id:
        flash('Вы уже авторизованы под этим пользователем.', 'info')
        next_url = request.form.get('next')
        if not next_url or not next_url.startswith('/'):
            next_url = url_for('profile.view_profile', user_id=user_id)
        return redirect(next_url)
    
    conn = get_db_connection()
    user = conn.execute('SELECT user_id, username, level, synd FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
    if not user:
        conn.close()
        flash('Пользователь не найден', 'error')
        next_url = request.form.get('next')
        if not next_url or not next_url.startswith('/'):
            next_url = url_for('admin.admin_users')
        return redirect(next_url)
    
    # Сохраняем данные исходной сессии администратора
    original_info = {
        'user_id': session.get('user_id'),
        'username': session.get('username'),
        'roles': list(session.get('roles', [])) if session.get('roles') else [],
        'level': session.get('level'),
        'synd': session.get('synd')
    }
    session['impersonation_original'] = original_info
    session['impersonation_target'] = {
        'user_id': user['user_id'],
        'username': user['username']
    }
    session['impersonation_started_at'] = datetime.now().isoformat()
    
    return_url = request.form.get('return_url')
    if return_url and return_url.startswith('/'):
        session['impersonation_return_url'] = return_url
    else:
        session['impersonation_return_url'] = url_for('admin.admin_users')
    
    log_activity(
        'impersonation_start',
        details=f"Начат режим управления пользователем {user['username']} ({user['user_id']})",
        metadata={
            'target_user_id': user['user_id'],
            'target_username': user['username']
        }
    )
    
    # Обновляем сессию под выбранного пользователя
    session['user_id'] = user['user_id']
    session['username'] = user['username']
    session['level'] = user['level']
    session['synd'] = user['synd']
    session['roles'] = get_user_role_names(user['user_id'])
    
    conn.close()
    
    next_url = request.form.get('next')
    if not next_url or not next_url.startswith('/'):
        next_url = url_for('profile.view_profile', user_id=user['user_id'])
    
    flash(f'Вы управляете пользователем {user["username"]}', 'info')
    return redirect(next_url)


@impersonation_bp.route('/impersonation/stop', methods=['POST'])
@require_login
def stop_impersonation():
    """Завершает режим управления пользователем"""
    original_info = session.get('impersonation_original')
    target_info = session.get('impersonation_target') or {}
    if not original_info:
        flash('Режим управления не активен.', 'error')
        return redirect(url_for('profile.dashboard'))
    
    # Восстанавливаем исходные данные администратора
    session['user_id'] = original_info.get('user_id')
    session['username'] = original_info.get('username')
    session['roles'] = original_info.get('roles', [])
    session['level'] = original_info.get('level')
    session['synd'] = original_info.get('synd')
    
    impersonation_started = session.get('impersonation_started_at')
    return_url = session.get('impersonation_return_url')
    
    duration_seconds = None
    if impersonation_started:
        try:
            start_dt = datetime.fromisoformat(impersonation_started)
            duration_seconds = max(0, int((datetime.now() - start_dt).total_seconds()))
        except (ValueError, TypeError):
            duration_seconds = None
    
    log_activity(
        'impersonation_stop',
        details=f"Завершен режим управления пользователем {target_info.get('username', '') or target_info.get('user_id', 'неизвестно')}",
        metadata={
            'target_user_id': target_info.get('user_id'),
            'target_username': target_info.get('username'),
            'duration_seconds': duration_seconds
        }
    )
    
    # Очищаем данные импровизированной сессии
    session.pop('impersonation_original', None)
    session.pop('impersonation_target', None)
    session.pop('impersonation_started_at', None)
    session.pop('impersonation_return_url', None)
    
    flash('Вы вернулись к своей учетной записи.', 'success')
    
    if return_url and return_url.startswith('/'):
        return redirect(return_url)
    
    # Если исходная ссылка недоступна, возвращаем на страницу управления пользователями
    if 'admin' in session.get('roles', []):
        return redirect(url_for('admin.admin_users'))
    return redirect(url_for('profile.dashboard'))
