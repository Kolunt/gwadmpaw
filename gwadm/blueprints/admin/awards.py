"""Admin: awards."""

import os

from flask import (
    Blueprint, current_app, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp
from gwadm.config import ALLOWED_AWARD_IMAGE_EXTENSIONS
from gwadm.services.uploads import save_validated_image, validate_image_upload

@bp.route('/awards')
@require_role('admin')
def admin_awards():
    """Список наград"""
    conn = get_db_connection()
    awards = conn.execute('''
        SELECT a.*, 
               COUNT(ua.id) as users_count,
               u.username as creator_name
        FROM awards a
        LEFT JOIN user_awards ua ON a.id = ua.award_id
        LEFT JOIN users u ON a.created_by = u.user_id
        GROUP BY a.id
        ORDER BY a.sort_order, a.created_at DESC
    ''').fetchall()
    conn.close()
    return render_template('admin/awards.html', awards=awards)


@bp.route('/awards/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_award_create():
    """Создание награды"""
    conn = get_db_connection()
    
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        icon = request.form.get('icon', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        image_file = request.files.get('image')
        selected_users = request.form.getlist('users')  # Получаем список выбранных пользователей
        
        if not title:
            flash('Заголовок награды обязателен', 'error')
            users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
            conn.close()
            return render_template('admin/award_form.html', users=users)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
        except ValueError:
            sort_order = 100
        
        image_path = None
        if image_file and image_file.filename:
            data, upload_error = validate_image_upload(
                image_file,
                ALLOWED_AWARD_IMAGE_EXTENSIONS,
                allow_svg=True,
            )
            if upload_error:
                flash(upload_error, 'error')
                users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
                conn.close()
                return render_template('admin/award_form.html', users=users)
            file_ext = os.path.splitext(image_file.filename)[1].lower()
            upload_dir = os.path.join(current_current_app.static_folder, 'uploads', 'awards')
            filename = save_validated_image(data, upload_dir, 'award', file_ext)
            image_path = f'/static/uploads/awards/{filename}'
        
        try:
            # Создаем награду
            cursor = conn.execute('''
                INSERT INTO awards (title, icon, image, sort_order, created_by)
                VALUES (?, ?, ?, ?, ?)
            ''', (title, icon, image_path, sort_order, session['user_id']))
            award_id = cursor.lastrowid
            
            # Присваиваем награду выбранным пользователям
            if selected_users:
                for user_id_str in selected_users:
                    try:
                        user_id = int(user_id_str)
                        assign_award(user_id, award_id, assigned_by=session['user_id'])
                    except ValueError:
                        continue
            
            conn.commit()
            flash('Награда успешно создана', 'success')
            conn.close()
            return redirect(url_for('admin.admin_awards'))
        except Exception as e:
            log_error(f"Error creating award: {e}")
            flash(f'Ошибка создания награды: {str(e)}', 'error')
            conn.close()
    
    # GET запрос - получаем список пользователей
    users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
    conn.close()
    return render_template('admin/award_form.html', users=users)

@bp.route('/awards/<int:award_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_award_edit(award_id):
    """Редактирование награды"""
    conn = get_db_connection()
    award = conn.execute('SELECT * FROM awards WHERE id = ?', (award_id,)).fetchone()
    
    if not award:
        flash('Награда не найдена', 'error')
        conn.close()
        return redirect(url_for('admin.admin_awards'))
    
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        icon = request.form.get('icon', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        image_file = request.files.get('image')
        delete_image = request.form.get('delete_image', '0')
        selected_users = request.form.getlist('users')  # Получаем список выбранных пользователей
        
        if not title:
            flash('Заголовок награды обязателен', 'error')
            users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
            # Получаем текущих пользователей с наградой
            current_users = conn.execute('''
                SELECT user_id FROM user_awards WHERE award_id = ?
            ''', (award_id,)).fetchall()
            current_user_ids = [u['user_id'] for u in current_users]
            conn.close()
            return render_template('admin/award_form.html', award=award, users=users, current_user_ids=current_user_ids)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
        except ValueError:
            sort_order = 100
        
        # Обработка загрузки/удаления изображения
        image_path = award['image']
        
        if delete_image == '1':
            # Удаляем старое изображение
            if image_path:
                old_filepath = os.path.join(current_app.static_folder, image_path.replace('/static/', ''))
                if os.path.exists(old_filepath):
                    try:
                        os.remove(old_filepath)
                    except Exception as e:
                        log_debug(f"Error deleting old image: {e}")
            image_path = None
        
        if image_file and image_file.filename:
            if image_path:
                old_filepath = os.path.join(current_current_app.static_folder, image_path.replace('/static/', ''))
                if os.path.exists(old_filepath):
                    try:
                        os.remove(old_filepath)
                    except Exception as e:
                        log_debug(f"Error deleting old image: {e}")

            data, upload_error = validate_image_upload(
                image_file,
                ALLOWED_AWARD_IMAGE_EXTENSIONS,
                allow_svg=True,
            )
            if upload_error:
                flash(upload_error, 'error')
                users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
                current_users = conn.execute(
                    'SELECT user_id FROM user_awards WHERE award_id = ?', (award_id,),
                ).fetchall()
                current_user_ids = [u['user_id'] for u in current_users]
                conn.close()
                return render_template(
                    'admin/award_form.html',
                    award=award,
                    users=users,
                    current_user_ids=current_user_ids,
                )
            file_ext = os.path.splitext(image_file.filename)[1].lower()
            upload_dir = os.path.join(current_current_app.static_folder, 'uploads', 'awards')
            filename = save_validated_image(data, upload_dir, 'award', file_ext)
            image_path = f'/static/uploads/awards/{filename}'
        
        try:
            # Обновляем награду
            conn.execute('''
                UPDATE awards SET title = ?, icon = ?, image = ?, sort_order = ?
                WHERE id = ?
            ''', (title, icon, image_path, sort_order, award_id))
            
            # Обновляем присвоение наград пользователям
            # Получаем текущих пользователей с наградой
            current_users = conn.execute('''
                SELECT user_id FROM user_awards WHERE award_id = ?
            ''', (award_id,)).fetchall()
            current_user_ids = {u['user_id'] for u in current_users}
            
            # Преобразуем selected_users в множество int
            selected_user_ids = set()
            for uid in selected_users:
                try:
                    selected_user_ids.add(int(uid))
                except (ValueError, TypeError):
                    continue
            
            assigned_by = session.get('user_id')
            
            # Добавляем новых пользователей
            for user_id in selected_user_ids:
                if user_id not in current_user_ids:
                    try:
                        conn.execute('''
                            INSERT OR REPLACE INTO user_awards (user_id, award_id, assigned_by)
                            VALUES (?, ?, ?)
                        ''', (user_id, award_id, assigned_by))
                    except Exception as e:
                        log_error(f"Error assigning award to user {user_id}: {e}")
            
            # Удаляем пользователей, которых больше нет в списке
            for user_id in current_user_ids:
                if user_id not in selected_user_ids:
                    try:
                        conn.execute('''
                            DELETE FROM user_awards
                            WHERE user_id = ? AND award_id = ?
                        ''', (user_id, award_id))
                    except Exception as e:
                        log_error(f"Error removing award from user {user_id}: {e}")
            
            conn.commit()
            flash('Награда успешно обновлена', 'success')
            conn.close()
            return redirect(url_for('admin.admin_awards'))
        except Exception as e:
            log_error(f"Error updating award: {e}")
            flash(f'Ошибка обновления награды: {str(e)}', 'error')
            conn.close()
    
    # GET запрос - получаем список пользователей и текущих пользователей с наградой
    users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
    current_users = conn.execute('''
        SELECT user_id FROM user_awards WHERE award_id = ?
    ''', (award_id,)).fetchall()
    current_user_ids = [u['user_id'] for u in current_users]
    conn.close()
    return render_template('admin/award_form.html', award=award, users=users, current_user_ids=current_user_ids)


@bp.route('/awards/<int:award_id>/delete', methods=['POST'])
@require_role('admin')
def admin_award_delete(award_id):
    """Удаление награды"""
    conn = get_db_connection()
    award = conn.execute('SELECT * FROM awards WHERE id = ?', (award_id,)).fetchone()
    
    if not award:
        flash('Награда не найдена', 'error')
        conn.close()
        return redirect(url_for('admin.admin_awards'))
    
    try:
        # Удаляем изображение если есть
        if award['image']:
            image_path = os.path.join(current_app.static_folder, award['image'].replace('/static/', ''))
            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                except Exception as e:
                    log_debug(f"Error deleting award image: {e}")
        
        conn.execute('DELETE FROM awards WHERE id = ?', (award_id,))
        conn.commit()
        flash('Награда успешно удалена', 'success')
    except Exception as e:
        log_error(f"Error deleting award: {e}")
        flash(f'Ошибка удаления награды: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin.admin_awards'))
