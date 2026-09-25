"""Admin: roles."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/roles')
@require_role('admin')
def admin_roles():
    """Редирект на вкладку ролей в управлении пользователями"""
    return redirect(url_for('admin.admin_users') + '#roles')

@bp.route('/roles/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_role_create():
    """Создание новой роли"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        
        if not name or not display_name:
            flash('Имя и отображаемое имя роли обязательны', 'error')
            return render_template('admin/role_form.html')
        
        # Проверяем, что имя роли уникально
        conn = get_db_connection()
        existing = conn.execute('SELECT id FROM roles WHERE name = ?', (name,)).fetchone()
        if existing:
            flash('Роль с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/role_form.html')
        
        try:
            cursor = conn.execute('''
                INSERT INTO roles (name, display_name, description, is_system)
                VALUES (?, ?, ?, 0)
            ''', (name, display_name, description))
            role_id = cursor.lastrowid
            
            # Сохраняем выбранные права
            selected_permissions = request.form.getlist('permissions')
            for permission_id in selected_permissions:
                try:
                    permission_id_int = int(permission_id)
                    assign_permission_to_role(role_id, permission_id_int)
                except ValueError:
                    pass
            
            conn.commit()
            flash('Роль успешно создана', 'success')
            conn.close()
            return redirect(url_for('admin.admin_roles'))
        except Exception as e:
            log_error(f"Error creating role: {e}")
            flash(f'Ошибка создания роли: {str(e)}', 'error')
            conn.close()
    
    # Получаем все права для отображения в форме
    permissions = get_all_permissions()
    return render_template('admin/role_form.html', permissions=permissions, role_permissions=[])


@bp.route('/roles/<int:role_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_role_edit(role_id):
    """Редактирование роли"""
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE id = ?', (role_id,)).fetchone()
    
    if not role:
        flash('Роль не найдена', 'error')
        conn.close()
        return redirect(url_for('admin.admin_roles'))
    
    # Системные роли нельзя редактировать
    if role['is_system']:
        flash('Системные роли нельзя редактировать', 'error')
        conn.close()
        return redirect(url_for('admin.admin_roles'))
    
    if request.method == 'POST':
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        
        if not display_name:
            flash('Отображаемое имя роли обязательно', 'error')
            permissions = get_all_permissions()
            conn.close()
            return render_template('admin/role_form.html', role=role, permissions=permissions)
        
        try:
            conn.execute('''
                UPDATE roles SET display_name = ?, description = ?
                WHERE id = ?
            ''', (display_name, description, role_id))
            
            # Обновляем права роли
            selected_permissions = request.form.getlist('permissions')
            selected_permission_ids = [int(pid) for pid in selected_permissions if pid.isdigit()]
            
            # Получаем текущие права роли
            current_permissions = get_role_permissions(role_id)
            current_permission_ids = [p['id'] for p in current_permissions]
            
            # Удаляем права, которые были сняты
            for perm_id in current_permission_ids:
                if perm_id not in selected_permission_ids:
                    remove_permission_from_role(role_id, perm_id)
            
            # Добавляем новые права
            for perm_id in selected_permission_ids:
                if perm_id not in current_permission_ids:
                    assign_permission_to_role(role_id, perm_id)
            
            conn.commit()
            flash('Роль успешно обновлена', 'success')
            conn.close()
            return redirect(url_for('admin.admin_roles'))
        except Exception as e:
            log_error(f"Error updating role: {e}")
            flash(f'Ошибка обновления роли: {str(e)}', 'error')
            conn.close()
    
    permissions = get_all_permissions()
    # Получаем права текущей роли
    role_perms = get_role_permissions(role_id)
    role_permissions_list = [p['id'] for p in role_perms]
    conn.close()
    return render_template('admin/role_form.html', role=role, permissions=permissions, role_permissions=role_permissions_list)

@bp.route('/roles/<int:role_id>/delete', methods=['POST'])
@require_role('admin')
def admin_role_delete(role_id):
    """Удаление роли"""
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE id = ?', (role_id,)).fetchone()
    
    if not role:
        flash('Роль не найдена', 'error')
        conn.close()
        return redirect(url_for('admin.admin_roles'))
    
    # Системные роли нельзя удалять
    if role['is_system']:
        flash('Системные роли нельзя удалять', 'error')
        conn.close()
        return redirect(url_for('admin.admin_roles'))
    
    try:
        conn.execute('DELETE FROM roles WHERE id = ?', (role_id,))
        conn.commit()
        flash('Роль успешно удалена', 'success')
    except Exception as e:
        log_error(f"Error deleting role: {e}")
        flash(f'Ошибка удаления роли: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin.admin_roles'))
