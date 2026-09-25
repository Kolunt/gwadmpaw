"""Admin: rating."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug
from gwadm.services.activity import log_activity
from gwadm.services.rating import queue_rating_recalc

from gwadm.blueprints.admin import bp

@bp.route('/rating-settings/fix-points', methods=['POST'])
@require_role('admin')
def admin_rating_settings_fix_points():
    """Исправляет значения points в snowflake_events, преобразуя строки в числа"""
    conn = get_db_connection()
    try:
        # Получаем все события с points как строками
        events = conn.execute('''
            SELECT id, points FROM snowflake_events
            WHERE typeof(points) = 'text' OR points IS NULL
        ''').fetchall()
        
        fixed_count = 0
        for event in events:
            try:
                # Преобразуем points в int
                old_points = event['points']
                if old_points is None:
                    new_points = 0
                else:
                    new_points = int(old_points)
                
                # Обновляем запись
                conn.execute('''
                    UPDATE snowflake_events
                    SET points = ?
                    WHERE id = ?
                ''', (new_points, event['id']))
                fixed_count += 1
            except (ValueError, TypeError) as e:
                log_error(f"Error fixing points for event {event['id']}: {e}")
                continue
        
        conn.commit()
        flash(f'Исправлено записей: {fixed_count}', 'success')
    except Exception as e:
        log_error(f"Error fixing points: {e}")
        conn.rollback()
        flash('Ошибка при исправлении данных: ' + str(e), 'error')
    finally:
        conn.close()
    
    return redirect(url_for('admin.admin_rating_settings'))



@bp.route('/rating-settings', methods=['GET', 'POST'])
@require_role('admin')
def admin_rating_settings():
    """Страница настроек рейтинга (цыфорки)"""
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action', 'save')  # 'save' или 'update'
        
        # Обновляем настройки рейтинга
        settings_dict = {}
        for key in request.form:
            if key.startswith('setting_'):
                setting_key = key.replace('setting_', '')
                if setting_key.startswith('rating_'):
                    setting_value = request.form.get(key, '0')
                    settings_dict[setting_key] = setting_value
        
        # Получаем все награды и звания для создания настроек
        awards = conn.execute('SELECT id, title FROM awards').fetchall()
        titles = conn.execute('SELECT id, display_name FROM titles').fetchall()
        
        # Сохраняем настройки
        for key, value in settings_dict.items():
            try:
                # Проверяем, существует ли настройка
                existing_setting = conn.execute('SELECT key FROM settings WHERE key = ?', (key,)).fetchone()
                if existing_setting:
                    # Обновляем существующую настройку
                    columns_info = conn.execute("PRAGMA table_info(settings)").fetchall()
                    columns = [col[1] for col in columns_info]
                    
                    if 'updated_at' in columns and 'updated_by' in columns:
                        conn.execute('''
                            UPDATE settings 
                            SET value = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                            WHERE key = ?
                        ''', (value, session.get('user_id'), key))
                    elif 'updated_at' in columns:
                        conn.execute('''
                            UPDATE settings 
                            SET value = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE key = ?
                        ''', (value, key))
                    else:
                        conn.execute('''
                            UPDATE settings 
                            SET value = ?
                            WHERE key = ?
                        ''', (value, key))
                else:
                    # Создаем новую настройку
                    description = ''
                    if key == 'rating_contact_telegram':
                        description = 'Очки за заполненный Telegram'
                    elif key == 'rating_contact_whatsapp':
                        description = 'Очки за заполненный WhatsApp'
                    elif key == 'rating_contact_viber':
                        description = 'Очки за заполненный Viber'
                    elif key == 'rating_event_registration':
                        description = 'Очки за регистрацию на мероприятие (начисляются автоматически всем зарегистрированным участникам при закрытии регистрации администратором)'
                    elif key == 'rating_event_gift_not_sent':
                        description = 'Очки за неотправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации не отправили подарок)'
                    elif key == 'rating_event_gift_sent':
                        description = 'Очки за отправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации отправили подарок)'
                    elif key.startswith('rating_award_'):
                        # Награда
                        try:
                            award_id = int(key.replace('rating_award_', ''))
                            award = next((a for a in awards if a['id'] == award_id), None)
                            if award:
                                description = f'Очки за награду: {award["title"]}'
                        except (ValueError, TypeError):
                            description = 'Очки за награду'
                    elif key.startswith('rating_title_'):
                        # Звание
                        try:
                            title_id = int(key.replace('rating_title_', ''))
                            title = next((t for t in titles if t['id'] == title_id), None)
                            if title:
                                description = f'Очки за звание: {title["display_name"]}'
                        except (ValueError, TypeError):
                            description = 'Очки за звание'
                    
                    columns_info = conn.execute("PRAGMA table_info(settings)").fetchall()
                    columns = [col[1] for col in columns_info]
                    
                    if 'created_at' in columns and 'created_by' in columns and 'updated_at' in columns and 'updated_by' in columns:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category, created_at, created_by, updated_at, updated_by)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, ?)
                        ''', (key, value, description, 'rating', session.get('user_id'), session.get('user_id')))
                    elif 'updated_at' in columns and 'updated_by' in columns:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category, updated_at, updated_by)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                        ''', (key, value, description, 'rating', session.get('user_id')))
                    elif 'updated_at' in columns:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category, updated_at)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (key, value, description, 'rating'))
                    else:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category)
                            VALUES (?, ?, ?, ?)
                        ''', (key, value, description, 'rating'))
            except Exception as e:
                log_error(f"Error updating rating setting {key}: {e}")
        
        conn.commit()
        
        if action == 'update':
            queue_rating_recalc()
            flash(
                'Настройки сохранены. Пересчёт очков поставлен в очередь '
                '(выполнится при следующем запуске gwadm-rating-cache).',
                'success',
            )
        else:
            flash('Настройки рейтинга успешно сохранены', 'success')
        
        conn.close()
        return redirect(url_for('admin.admin_rating_settings'))
    
    # Получаем все настройки рейтинга
    rating_settings = conn.execute('''
        SELECT * FROM settings 
        WHERE category = 'rating' OR key LIKE 'rating_%'
        ORDER BY key
    ''').fetchall()
    
    # Создаем словарь для быстрого доступа к настройкам
    settings_dict = {}
    for setting in rating_settings:
        settings_dict[setting['key']] = dict(setting)
    
    # Получаем все награды и звания для настройки очков
    awards = conn.execute('''
        SELECT id, title, icon FROM awards ORDER BY sort_order, title
    ''').fetchall()
    
    titles = conn.execute('''
        SELECT id, name, display_name, icon FROM titles ORDER BY is_system DESC, display_name
    ''').fetchall()
    
    # Инициализируем настройки для наград и званий, если их еще нет
    for award in awards:
        key = f'rating_award_{award["id"]}'
        if key not in settings_dict:
            # Создаем настройку с дефолтным значением 0
            try:
                columns_info = conn.execute("PRAGMA table_info(settings)").fetchall()
                columns = [col[1] for col in columns_info]
                description = f'Очки за награду: {award["title"]}'
                
                if 'created_at' in columns and 'created_by' in columns and 'updated_at' in columns and 'updated_by' in columns:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category, created_at, created_by, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, ?)
                    ''', (key, '0', description, 'rating', session.get('user_id'), session.get('user_id')))
                elif 'updated_at' in columns and 'updated_by' in columns:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    ''', (key, '0', description, 'rating', session.get('user_id')))
                elif 'updated_at' in columns:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category, updated_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (key, '0', description, 'rating'))
                else:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category)
                        VALUES (?, ?, ?, ?)
                    ''', (key, '0', description, 'rating'))
                settings_dict[key] = {'key': key, 'value': '0', 'description': description}
            except Exception as e:
                log_error(f"Error initializing rating setting for award {award['id']}: {e}")
    
    for title in titles:
        key = f'rating_title_{title["id"]}'
        if key not in settings_dict:
            # Создаем настройку с дефолтным значением 0
            try:
                columns_info = conn.execute("PRAGMA table_info(settings)").fetchall()
                columns = [col[1] for col in columns_info]
                description = f'Очки за звание: {title["display_name"]}'
                
                if 'created_at' in columns and 'created_by' in columns and 'updated_at' in columns and 'updated_by' in columns:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category, created_at, created_by, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, ?)
                    ''', (key, '0', description, 'rating', session.get('user_id'), session.get('user_id')))
                elif 'updated_at' in columns and 'updated_by' in columns:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    ''', (key, '0', description, 'rating', session.get('user_id')))
                elif 'updated_at' in columns:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category, updated_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (key, '0', description, 'rating'))
                else:
                    conn.execute('''
                        INSERT INTO settings (key, value, description, category)
                        VALUES (?, ?, ?, ?)
                    ''', (key, '0', description, 'rating'))
                settings_dict[key] = {'key': key, 'value': '0', 'description': description}
            except Exception as e:
                log_error(f"Error initializing rating setting for title {title['id']}: {e}")
    
    conn.commit()
    conn.close()
    
    return render_template('admin/rating_settings.html', 
                         settings_dict=settings_dict,
                         awards=awards,
                         titles=titles)


@bp.route('/rating/<int:user_id>')
@require_role('admin')
def admin_rating_detail(user_id):
    conn = get_db_connection()
    try:
        user_row = conn.execute('''
            SELECT user_id, username, telegram, whatsapp, viber
            FROM users
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        if not user_row:
            flash('Пользователь не найден', 'error')
            return redirect(url_for('public.user_rating'))

        user_dict = dict(user_row)
        _sync_contact_snowflakes(conn, user_dict)
        conn.commit()

        events = [
            dict(row) for row in conn.execute('''
                SELECT id, source, reason, points, active, manual_revoked, created_at, updated_at, revoked_at
                FROM snowflake_events
                WHERE user_id = ?
                ORDER BY created_at DESC, id DESC
            ''', (user_id,)).fetchall()
        ]
    finally:
        conn.close()

    for event in events:
        event['source_label'] = _get_snowflake_source_label(event['source'])

    active_count = 0.0
    for event in events:
        active = event['active']
        if active == 1 or active == '1' or (isinstance(active, bool) and active):
            try:
                # Преобразуем points в float, чтобы поддерживать десятичные значения
                points = float(event['points']) if event['points'] is not None else 0.0
            except (ValueError, TypeError):
                points = 0.0
            active_count += points
    return render_template(
        'admin/rating_detail.html',
        user=user_dict,
        events=events,
        active_count=active_count,
    )



@bp.route('/rating/events/<int:event_id>/annul', methods=['POST'])
@require_role('admin')
def admin_rating_event_annul(event_id):
    conn = get_db_connection()
    try:
        event = conn.execute('SELECT id, user_id, active, manual_revoked FROM snowflake_events WHERE id = ?', (event_id,)).fetchone()
        if not event:
            flash('Запись не найдена', 'error')
            return redirect(url_for('public.user_rating'))

        user_id = event['user_id']
        if event['manual_revoked'] and not event['active']:
            flash('Бубенчик уже аннулирован.', 'info')
        else:
            conn.execute('''
                UPDATE snowflake_events
                SET active = 0,
                    manual_revoked = 1,
                    revoked_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (event_id,))
            conn.commit()
            log_activity('snowflake_annul', details=f'Аннулирован бубенчик #{event_id}', metadata={'event_id': event_id, 'target_user_id': user_id})
            flash('Бубенчик аннулирован.', 'success')
    finally:
        conn.close()

    return redirect(url_for('admin.admin_rating_detail', user_id=user_id))



@bp.route('/rating/events/<int:event_id>/restore', methods=['POST'])
@require_role('admin')
def admin_rating_event_restore(event_id):
    conn = get_db_connection()
    try:
        event = conn.execute('SELECT id, user_id, manual_revoked FROM snowflake_events WHERE id = ?', (event_id,)).fetchone()
        if not event:
            flash('Запись не найдена', 'error')
            return redirect(url_for('public.user_rating'))

        user_id = event['user_id']
        conn.execute('''
            UPDATE snowflake_events
            SET active = 1,
                manual_revoked = 0,
                revoked_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (event_id,))
        conn.commit()
        log_activity('snowflake_restore', details=f'Восстановлен бубенчик #{event_id}', metadata={'event_id': event_id, 'target_user_id': user_id})
        flash('Бубенчик восстановлен.', 'success')
    finally:
        conn.close()

    return redirect(url_for('admin.admin_rating_detail', user_id=user_id))
