"""Admin: titles."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp
from gwadm.services.titles import get_all_titles, get_user_titles

@bp.route('/titles')
@require_role('admin')
def admin_titles():
    """Управление званиями"""
    conn = get_db_connection()
    titles = conn.execute('SELECT * FROM titles ORDER BY is_system DESC, display_name').fetchall()
    
    # Для каждого звания получаем количество пользователей
    titles_with_counts = []
    for title in titles:
        count = conn.execute('''
            SELECT COUNT(*) as count FROM user_titles WHERE title_id = ?
        ''', (title['id'],)).fetchone()
        titles_with_counts.append({
            **dict(title),
            'user_count': count['count']
        })
    
    conn.close()
    
    return render_template('admin/titles.html', titles=titles_with_counts)


@bp.route('/titles/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_title_create():
    """Создание нового звания"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#007bff').strip()
        icon = request.form.get('icon', '').strip()
        
        if not name or not display_name:
            flash('Имя и отображаемое имя звания обязательны', 'error')
            return render_template('admin/title_form.html')
        
        # Проверяем, что имя звания уникально
        conn = get_db_connection()
        existing = conn.execute('SELECT id FROM titles WHERE name = ?', (name,)).fetchone()
        if existing:
            flash('Звание с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/title_form.html')
        
        try:
            conn.execute('''
                INSERT INTO titles (name, display_name, description, color, icon, is_system)
                VALUES (?, ?, ?, ?, ?, 0)
            ''', (name, display_name, description, color, icon))
            conn.commit()
            flash('Звание успешно создано', 'success')
            conn.close()
            return redirect(url_for('admin.admin_titles'))
        except Exception as e:
            log_error(f"Error creating title: {e}")
            flash(f'Ошибка создания звания: {str(e)}', 'error')
            conn.close()
    
    return render_template('admin/title_form.html')

@bp.route('/titles/<int:title_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_title_edit(title_id):
    """Редактирование звания"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE id = ?', (title_id,)).fetchone()
    
    if not title:
        flash('Звание не найдено', 'error')
        conn.close()
        return redirect(url_for('admin.admin_titles'))
    
    # Системные звания нельзя редактировать
    if title['is_system']:
        flash('Системные звания нельзя редактировать', 'error')
        conn.close()
        return redirect(url_for('admin.admin_titles'))
    
    if request.method == 'POST':
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#007bff').strip()
        icon = request.form.get('icon', '').strip()
        
        if not display_name:
            flash('Отображаемое имя звания обязательно', 'error')
            conn.close()
            return render_template('admin/title_form.html', title=title)
        
        try:
            conn.execute('''
                UPDATE titles SET display_name = ?, description = ?, color = ?, icon = ?
                WHERE id = ?
            ''', (display_name, description, color, icon, title_id))
            conn.commit()
            flash('Звание успешно обновлено', 'success')
            conn.close()
            return redirect(url_for('admin.admin_titles'))
        except Exception as e:
            log_error(f"Error updating title: {e}")
            flash(f'Ошибка обновления звания: {str(e)}', 'error')
            conn.close()
    
    conn.close()
    return render_template('admin/title_form.html', title=title)


@bp.route('/titles/<int:title_id>/delete', methods=['POST'])
@require_role('admin')
def admin_title_delete(title_id):
    """Удаление звания"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE id = ?', (title_id,)).fetchone()
    
    if not title:
        flash('Звание не найдено', 'error')
        conn.close()
        return redirect(url_for('admin.admin_titles'))
    
    # Системные звания нельзя удалять
    if title['is_system']:
        flash('Системные звания нельзя удалять', 'error')
        conn.close()
        return redirect(url_for('admin.admin_titles'))
    
    try:
        conn.execute('DELETE FROM titles WHERE id = ?', (title_id,))
        conn.commit()
        flash('Звание успешно удалено', 'success')
    except Exception as e:
        log_error(f"Error deleting title: {e}")
        flash(f'Ошибка удаления звания: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin.admin_titles'))


@bp.route('/users/<int:user_id>/titles', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_titles(user_id):
    """Управление званиями пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('admin.admin_users'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        title_id = request.form.get('title_id')
        
        if action == 'assign' and title_id:
            try:
                title_id_int = int(title_id)
                if assign_title(user_id, title_id_int, assigned_by=session['user_id']):
                    flash('Звание успешно назначено', 'success')
                else:
                    flash('Ошибка назначения звания', 'error')
            except ValueError:
                flash('Неверный ID звания', 'error')
        elif action == 'remove' and title_id:
            try:
                title_id_int = int(title_id)
                if remove_title(user_id, title_id_int):
                    flash('Звание успешно удалено', 'success')
                else:
                    flash('Ошибка удаления звания', 'error')
            except ValueError:
                flash('Неверный ID звания', 'error')
    
    # Получаем все звания
    all_titles = get_all_titles()
    
    # Получаем звания пользователя
    user_titles = get_user_titles(user_id)
    user_title_ids = [t['id'] for t in user_titles]
    
    conn.close()
    
    return render_template('admin/user_titles.html', 
                         user=user, 
                         all_titles=all_titles, 
                         user_titles=user_titles,
                         user_title_ids=user_title_ids)
