"""Admin: users."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/users')
@require_role('admin')
def admin_users():
    """Управление пользователями"""
    conn = get_db_connection()
    users = conn.execute('''
        SELECT u.*,
               GROUP_CONCAT(r.display_name, ', ') as roles,
               (SELECT COUNT(*) FROM user_admin_comments WHERE user_id = u.user_id) as comments_count
        FROM users u
        LEFT JOIN user_roles ur ON u.user_id = ur.user_id
        LEFT JOIN roles r ON ur.role_id = r.id
        GROUP BY u.user_id
        ORDER BY u.created_at DESC
    ''').fetchall()
    
    roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    roles_with_counts = []
    for role in roles:
        count = conn.execute('''
            SELECT COUNT(*) as count FROM user_roles WHERE role_id = ?
        ''', (role['id'],)).fetchone()
        roles_with_counts.append({
            **dict(role),
            'user_count': count['count']
        })
    
    conn.close()
    
    return render_template('admin/users.html', users=users, roles=roles_with_counts)


@bp.route('/users/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_create():
    """Создание нового пользователя"""
    available_languages = app.config.get('LANGUAGES', {'ru': 'Русский', 'en': 'English'})
    if request.method == 'POST':
        user_id = request.form.get('user_id', '').strip()
        username = request.form.get('username', '').strip()
        level = request.form.get('level', '0')
        synd = request.form.get('synd', '0')
        has_passport = request.form.get('has_passport', '0')
        has_mobile = request.form.get('has_mobile', '0')
        old_passport = request.form.get('old_passport', '0')
        usersex = request.form.get('usersex', '0')
        bio = request.form.get('bio', '').strip()
        contact_info = request.form.get('contact_info', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        telegram = request.form.get('telegram', '').strip()
        whatsapp = request.form.get('whatsapp', '').strip()
        viber = request.form.get('viber', '').strip()
        last_name = request.form.get('last_name', '').strip()
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        postal_code = request.form.get('postal_code', '').strip()
        country = request.form.get('country', '').strip()
        city = request.form.get('city', '').strip()
        street = request.form.get('street', '').strip()
        house = request.form.get('house', '').strip()
        building = request.form.get('building', '').strip()
        apartment = request.form.get('apartment', '').strip()
        language = request.form.get('language', 'ru').strip()
        avatar_seed_form = request.form.get('avatar_seed', '').strip()
        avatar_style = request.form.get('avatar_style', '').strip()
        
        if not user_id or not username:
            flash('ID и имя пользователя обязательны', 'error')
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
        
        try:
            user_id_int = int(user_id)
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = int(has_passport)
            has_mobile_int = int(has_mobile)
            old_passport_int = int(old_passport)
        except ValueError:
            flash('Неверный формат числовых полей', 'error')
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
        
        conn = get_db_connection()
        
        # Проверяем, существует ли пользователь
        existing = conn.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id_int,)).fetchone()
        if existing:
            flash('Пользователь с таким ID уже существует', 'error')
            conn.close()
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
        
        try:
            if language not in available_languages:
                language = 'ru'
            avatar_seed = avatar_seed_form or generate_unique_avatar_seed(user_id_int)
            if not avatar_style or avatar_style not in AVATAR_STYLES:
                avatar_style = 'avataaars'
            
            conn.execute('''
                INSERT INTO users 
                (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, 
                 avatar_seed, avatar_style, language,
                 bio, contact_info, email, phone, telegram, whatsapp, viber,
                 last_name, first_name, middle_name,
                 postal_code, country, city, street, house, building, apartment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id_int, username, level_int, synd_int, has_passport_int, has_mobile_int, old_passport_int,
                  usersex, avatar_seed, avatar_style, language,
                  bio, contact_info, email, phone, telegram, whatsapp, viber,
                  last_name, first_name, middle_name,
                  postal_code, country, city, street, house, building, apartment))
            conn.commit()
            log_activity(
                'admin_user_create',
                details=f'Создан пользователь {username} (ID {user_id_int})',
                metadata={'target_user_id': user_id_int, 'username': username}
            )
            flash('Пользователь успешно создан', 'success')
            conn.close()
            return redirect(url_for('admin.admin_users'))
        except Exception as e:
            log_error(f"Error creating user: {e}")
            flash(f'Ошибка создания пользователя: {str(e)}', 'error')
            conn.close()
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
    
    return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)

@bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_edit(user_id):
    """Редактирование пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    available_languages = app.config.get('LANGUAGES', {'ru': 'Русский', 'en': 'English'})
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('admin.admin_users'))
    
    if request.method == 'POST':
        # Обработка назначения/удаления ролей
        role_action = request.form.get('role_action')
        if role_action:
            role_name = request.form.get('role_name')
            if role_action == 'assign' and role_name:
                if assign_role(user_id, role_name, assigned_by=session['user_id']):
                    flash(f'Роль "{role_name}" успешно назначена', 'success')
                else:
                    flash(f'Ошибка назначения роли', 'error')
            elif role_action == 'remove' and role_name:
                if remove_role(user_id, role_name):
                    flash(f'Роль "{role_name}" успешно удалена', 'success')
                else:
                    flash(f'Ошибка удаления роли', 'error')
        
        # Обработка назначения/удаления званий
        title_action = request.form.get('title_action')
        if title_action:
            title_id = request.form.get('title_id')
            if title_action == 'assign' and title_id:
                try:
                    title_id_int = int(title_id)
                    if assign_title(user_id, title_id_int, assigned_by=session['user_id']):
                        flash('Звание успешно назначено', 'success')
                    else:
                        flash('Ошибка назначения звания', 'error')
                except ValueError:
                    flash('Неверный формат ID звания', 'error')
            elif title_action == 'remove' and title_id:
                try:
                    title_id_int = int(title_id)
                    if remove_title(user_id, title_id_int):
                        flash('Звание успешно удалено', 'success')
                    else:
                        flash('Ошибка удаления звания', 'error')
                except ValueError:
                    flash('Неверный формат ID звания', 'error')
        
        # Обработка блокировки/разблокировки пользователя
        block_action = request.form.get('block_action')
        if block_action:
            if block_action == 'block':
                blocked_reason = request.form.get('blocked_reason', '').strip()
                if not blocked_reason:
                    flash('Причина блокировки обязательна', 'error')
                else:
                    try:
                        blocked_by = session['user_id']
                        blocked_at = datetime.utcnow()
                        conn.execute('''
                            UPDATE users SET
                                is_blocked = 1,
                                blocked_by = ?,
                                blocked_reason = ?,
                                blocked_at = ?
                            WHERE user_id = ?
                        ''', (blocked_by, blocked_reason, blocked_at, user_id))
                        conn.commit()
                        log_activity(
                            'admin_user_blocked',
                            details=f'Пользователь {user_id} заблокирован',
                            metadata={
                                'target_user_id': user_id,
                                'blocked_reason': blocked_reason,
                                'blocked_by': blocked_by
                            }
                        )
                        flash('Пользователь успешно заблокирован', 'success')
                    except Exception as e:
                        log_error(f"Error blocking user: {e}")
                        flash(f'Ошибка блокировки пользователя: {str(e)}', 'error')
            elif block_action == 'unblock':
                try:
                    conn.execute('''
                        UPDATE users SET
                            is_blocked = 0,
                            blocked_by = NULL,
                            blocked_reason = NULL,
                            blocked_at = NULL
                        WHERE user_id = ?
                    ''', (user_id,))
                    conn.commit()
                    log_activity(
                        'admin_user_unblocked',
                        details=f'Пользователь {user_id} разблокирован',
                        metadata={'target_user_id': user_id}
                    )
                    flash('Пользователь успешно разблокирован', 'success')
                except Exception as e:
                    log_error(f"Error unblocking user: {e}")
                    flash(f'Ошибка разблокировки пользователя: {str(e)}', 'error')
        
        # Обновление основных данных пользователя
        username = request.form.get('username', '').strip()
        level = request.form.get('level', '0')
        synd = request.form.get('synd', '0')
        has_passport = request.form.get('has_passport', '0')
        has_mobile = request.form.get('has_mobile', '0')
        usersex = request.form.get('usersex', '0')
        bio = request.form.get('bio', '').strip()
        contact_info = request.form.get('contact_info', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        telegram = request.form.get('telegram', '').strip()
        whatsapp = request.form.get('whatsapp', '').strip()
        viber = request.form.get('viber', '').strip()
        last_name = request.form.get('last_name', '').strip()
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        postal_code = request.form.get('postal_code', '').strip()
        country = request.form.get('country', '').strip()
        city = request.form.get('city', '').strip()
        street = request.form.get('street', '').strip()
        house = request.form.get('house', '').strip()
        building = request.form.get('building', '').strip()
        apartment = request.form.get('apartment', '').strip()
        language = request.form.get('language', (user['language'] or 'ru')).strip()
        avatar_seed = request.form.get('avatar_seed', '').strip()
        avatar_style = request.form.get('avatar_style', '').strip()
        old_passport = request.form.get('old_passport', str(user['old_passport'] or 0))
        
        if not username:
            flash('Имя пользователя обязательно', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
            # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
            blocker_info = None
            user_keys = user.keys()
            if 'blocked_by' in user_keys and user['blocked_by']:
                blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
                if blocker:
                    blocker_info = dict(blocker)
            conn.close()
            user_roles = get_user_roles(user_id)
            user_role_names = [r['name'] for r in user_roles]
            all_titles = get_all_titles()
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            return render_template('admin/user_form.html', 
                                 user=dict(user),
                                 all_roles=all_roles,
                                 user_roles=user_roles,
                                 user_role_names=user_role_names,
                                 all_titles=all_titles,
                                 user_titles=user_titles,
                                 user_title_ids=user_title_ids,
                                 avatar_styles=AVATAR_STYLES,
                                 available_languages=available_languages,
                                 blocker_info=blocker_info)
        
        try:
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = int(has_passport)
            has_mobile_int = int(has_mobile)
            old_passport_int = int(old_passport)
        except ValueError:
            flash('Неверный формат числовых полей', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
            # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
            blocker_info = None
            user_keys = user.keys()
            if 'blocked_by' in user_keys and user['blocked_by']:
                blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
                if blocker:
                    blocker_info = dict(blocker)
            conn.close()
            user_roles = get_user_roles(user_id)
            user_role_names = [r['name'] for r in user_roles]
            all_titles = get_all_titles()
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            return render_template('admin/user_form.html', 
                                 user=dict(user),
                                 all_roles=all_roles,
                                 user_roles=user_roles,
                                 user_role_names=user_role_names,
                                 all_titles=all_titles,
                                 user_titles=user_titles,
                                 user_title_ids=user_title_ids,
                                 avatar_styles=AVATAR_STYLES,
                                 available_languages=available_languages,
                                 blocker_info=blocker_info)
        
        try:
            if language not in available_languages:
                language = 'ru'
            if not avatar_seed:
                avatar_seed = user['avatar_seed']
            if not avatar_style or avatar_style not in AVATAR_STYLES:
                avatar_style = user['avatar_style'] or 'avataaars'
            conn.execute('''
                UPDATE users SET
                    username = ?, level = ?, synd = ?, has_passport = ?, has_mobile = ?, old_passport = ?,
                    usersex = ?, bio = ?, contact_info = ?,
                    email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?,
                    last_name = ?, first_name = ?, middle_name = ?,
                    postal_code = ?, country = ?, city = ?, street = ?, house = ?, building = ?, apartment = ?,
                    avatar_seed = ?, avatar_style = ?, language = ?
                WHERE user_id = ?
            ''', (username, level_int, synd_int, has_passport_int, has_mobile_int, old_passport_int,
                  usersex, bio, contact_info, email, phone, 
                  telegram, whatsapp, viber,
                  last_name, first_name, middle_name,
                  postal_code, country, city, street, house, building, apartment,
                  avatar_seed, avatar_style, language,
                  user_id))
            conn.commit()
            log_activity(
                'admin_user_update',
                details=f'Обновлены данные пользователя {user_id}',
                metadata={'target_user_id': user_id, 'username': username}
            )
            if not role_action and not title_action:
                flash('Пользователь успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('admin.admin_user_edit', user_id=user_id))
        except Exception as e:
            log_error(f"Error updating user: {e}")
            flash(f'Ошибка обновления пользователя: {str(e)}', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
            # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
            blocker_info = None
            user_keys = user.keys()
            if 'blocked_by' in user_keys and user['blocked_by']:
                blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
                if blocker:
                    blocker_info = dict(blocker)
            conn.close()
            user_roles = get_user_roles(user_id)
            user_role_names = [r['name'] for r in user_roles]
            all_titles = get_all_titles()
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            return render_template('admin/user_form.html', 
                                 user=dict(user),
                                 all_roles=all_roles,
                                 user_roles=user_roles,
                                 user_role_names=user_role_names,
                                 all_titles=all_titles,
                                 user_titles=user_titles,
                                 user_title_ids=user_title_ids,
                                 avatar_styles=AVATAR_STYLES,
                                 available_languages=available_languages,
                                 blocker_info=blocker_info)
    
    # GET запрос - получаем данные для отображения
    all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    user_roles = get_user_roles(user_id)
    user_role_names = [r['name'] for r in user_roles]
    all_titles = get_all_titles()
    user_titles = get_user_titles(user_id)
    user_title_ids = [t['id'] for t in user_titles]
    
    # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
    blocker_info = None
    user_keys = user.keys()
    if 'blocked_by' in user_keys and user['blocked_by']:
        blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
        if blocker:
            blocker_info = dict(blocker)
    
    conn.close()
    return render_template('admin/user_form.html', 
                         user=dict(user),
                         all_roles=all_roles,
                         user_roles=user_roles,
                         user_role_names=user_role_names,
                         all_titles=all_titles,
                         user_titles=user_titles,
                         user_title_ids=user_title_ids,
                         avatar_styles=AVATAR_STYLES,
                         available_languages=available_languages,
                         blocker_info=blocker_info)


@bp.route('/users/<int:user_id>/delete', methods=['POST'])
@require_role('admin')
def admin_user_delete(user_id):
    """Удаление пользователя администратором"""
    if session.get('user_id') == user_id:
        flash('Нельзя удалить собственную учетную запись', 'error')
        return redirect(url_for('admin.admin_users'))

    conn = get_db_connection()
    try:
        user = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user_id,)).fetchone()
        if not user:
            conn.close()
            flash('Пользователь не найден', 'error')
            return redirect(url_for('admin.admin_users'))

        username = user['username']

        conn.execute('BEGIN')
        conn.execute('UPDATE user_roles SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE user_titles SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE user_awards SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE awards SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE events SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE activity_logs SET user_id = NULL WHERE user_id = ?', (user_id,))
        conn.execute('UPDATE event_participant_approvals SET approved_by = NULL WHERE approved_by = ?', (user_id,))
        conn.execute('UPDATE event_assignments SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE faq_categories SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE faq_categories SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('UPDATE contacts SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE contacts SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('UPDATE faq_items SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE faq_items SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('UPDATE settings SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        conn.close()
        log_error(f"Ошибка удаления пользователя {user_id}: {e}")
        flash('Ошибка при удалении пользователя', 'error')
        return redirect(url_for('admin.admin_users'))

    conn.close()

    log_activity(
        'admin_user_delete',
        details=f'Удален пользователь {username} (ID {user_id})',
        metadata={'target_user_id': user_id, 'target_username': username}
    )
    flash('Пользователь успешно удален', 'success')
    return redirect(url_for('admin.admin_users'))



@bp.route('/users/<int:user_id>/roles', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_roles(user_id):
    """Управление ролями пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        return redirect(url_for('admin.admin_users'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        role_name = request.form.get('role_name')
        
        if action == 'assign':
            if assign_role(user_id, role_name, assigned_by=session['user_id']):
                flash(f'Роль "{role_name}" успешно назначена', 'success')
            else:
                flash(f'Ошибка назначения роли', 'error')
        elif action == 'remove':
            if remove_role(user_id, role_name):
                flash(f'Роль "{role_name}" успешно удалена', 'success')
            else:
                flash(f'Ошибка удаления роли', 'error')
    
    # Получаем все роли
    all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    
    # Получаем роли пользователя
    user_roles = get_user_roles(user_id)
    user_role_names = [r['name'] for r in user_roles]
    
    conn.close()
    
    return render_template('admin/user_roles.html', 
                         user=user, 
                         all_roles=all_roles, 
                         user_roles=user_roles,
                         user_role_names=user_role_names)
