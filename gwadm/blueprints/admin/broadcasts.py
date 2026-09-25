"""Admin: broadcasts."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/broadcasts')
@require_role('admin')
def admin_broadcasts():
    """Страница рассылок"""
    conn = get_db_connection()
    # Получаем всех пользователей с email и telegram
    users = conn.execute('''
        SELECT user_id, username, email, telegram, is_blocked
        FROM users
        ORDER BY username COLLATE NOCASE
    ''').fetchall()
    
    # Получаем историю рассылок
    broadcasts_history_raw = conn.execute('''
        SELECT id, created_by, created_by_username, recipient_type, delivery_method,
               subject, message, total_recipients, success_count, error_count,
               errors, created_at
        FROM broadcasts_history
        ORDER BY created_at DESC
        LIMIT 50
    ''').fetchall()
    
    # Получаем шаблоны рассылок
    templates = conn.execute('''
        SELECT id, name, description, delivery_method, subject, message, 
               created_by_username, created_at, updated_at
        FROM broadcast_templates
        ORDER BY updated_at DESC
    ''').fetchall()
    
    conn.close()
    
    # Парсим JSON ошибок для каждого элемента истории
    broadcasts_history = []
    for item in broadcasts_history_raw:
        item_dict = dict(item)
        # Парсим JSON ошибок, если они есть
        if item_dict.get('errors'):
            try:
                item_dict['errors_parsed'] = json.loads(item_dict['errors'])
            except (json.JSONDecodeError, TypeError):
                item_dict['errors_parsed'] = [item_dict['errors']] if item_dict['errors'] else []
        else:
            item_dict['errors_parsed'] = []
        broadcasts_history.append(item_dict)
    
    # Проверяем доступность интеграций
    smtp_enabled = get_setting('smtp_enabled', '0') == '1'
    smtp_verified = get_setting('smtp_verified', '0') == '1'
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    telegram_verified = get_setting('telegram_verified', '0') == '1'
    
    smtp_available = smtp_enabled and smtp_verified
    telegram_available = telegram_enabled and telegram_verified
    
    return render_template('admin/broadcasts.html', 
                         users=users,
                         smtp_available=smtp_available,
                         telegram_available=telegram_available,
                         broadcasts_history=broadcasts_history,
                         templates=templates)


@bp.route('/broadcasts/send', methods=['POST'])
@require_role('admin')
def admin_broadcasts_send():
    """Обработка отправки рассылки"""
    recipient_type = request.form.get('recipient_type', 'all')  # 'all' или 'selected'
    selected_users = request.form.getlist('selected_users')  # Список user_id
    delivery_method = request.form.get('delivery_method', 'email')  # 'email' или 'telegram'
    subject = request.form.get('subject', '').strip()
    message = request.form.get('message', '').strip()
    
    if not message:
        flash('Текст сообщения обязателен', 'error')
        return redirect(url_for('admin.admin_broadcasts'))
    
    if delivery_method == 'email' and not subject:
        flash('Тема письма обязательна для email рассылки', 'error')
        return redirect(url_for('admin.admin_broadcasts'))
    
    # Получаем список получателей с расширенными данными для плейсхолдеров
    conn = get_db_connection()
    if recipient_type == 'all':
        if delivery_method == 'email':
            recipients = conn.execute('''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE email IS NOT NULL AND email != '' AND is_blocked = 0
            ''').fetchall()
        else:  # telegram
            recipients = conn.execute('''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
            ''').fetchall()
    else:
        # Выбранные пользователи
        if not selected_users:
            conn.close()
            flash('Выберите хотя бы одного получателя', 'error')
            return redirect(url_for('admin.admin_broadcasts'))
        
        placeholders = ','.join(['?'] * len(selected_users))
        if delivery_method == 'email':
            recipients = conn.execute(f'''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE user_id IN ({placeholders}) 
                  AND email IS NOT NULL AND email != '' AND is_blocked = 0
            ''', selected_users).fetchall()
        else:  # telegram
            recipients = conn.execute(f'''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE user_id IN ({placeholders}) 
                  AND telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
            ''', selected_users).fetchall()
    
    conn.close()
    
    if not recipients:
        flash('Не найдено получателей с указанным способом доставки', 'error')
        return redirect(url_for('admin.admin_broadcasts'))
    
    # Функция замены плейсхолдеров
    def replace_placeholders(text, recipient):
        """Заменяет плейсхолдеры в тексте на данные получателя"""
        # Формируем имя (приоритет: first_name + last_name, затем username)
        name = ''
        if recipient.get('first_name') or recipient.get('last_name'):
            name_parts = []
            if recipient.get('first_name'):
                name_parts.append(recipient['first_name'])
            if recipient.get('last_name'):
                name_parts.append(recipient['last_name'])
            name = ' '.join(name_parts).strip()
        if not name:
            name = recipient.get('username', '')
        
        replacements = {
            '[name]': name,
            '[username]': recipient.get('username', ''),
            '[email]': recipient.get('email', ''),
            '[telegram]': recipient.get('telegram', ''),
            '[phone]': recipient.get('phone', ''),
            '[id]': str(recipient.get('user_id', '')),
            '[level]': str(recipient.get('level', '')) if recipient.get('level') else '',
            '[syndicate]': str(recipient.get('synd', '')) if recipient.get('synd') else '',
            '[first_name]': recipient.get('first_name', ''),
            '[last_name]': recipient.get('last_name', ''),
            '[city]': recipient.get('city', ''),
            '[country]': recipient.get('country', ''),
        }
        
        result = text
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, value)
        
        return result
    
    # Отправляем сообщения
    success_count = 0
    error_count = 0
    errors = []
    
    for recipient in recipients:
        try:
            # Заменяем плейсхолдеры в сообщении и теме
            personalized_message = replace_placeholders(message, recipient)
            personalized_subject = replace_placeholders(subject, recipient) if subject else ''
            
            if delivery_method == 'email':
                email = recipient['email']
                success, result_message = send_email_via_smtp(
                    to_email=email,
                    subject=personalized_subject,
                    body=personalized_message
                )
            else:  # telegram
                telegram = recipient['telegram']
                # Если telegram начинается с @, используем как username, иначе как chat_id
                success, result_message = send_telegram_message(
                    message=personalized_message,
                    chat_id=telegram
                )
            
            if success:
                success_count += 1
                log_activity(
                    'broadcast_sent',
                    details=f'Рассылка отправлена пользователю {recipient["username"]} (ID: {recipient["user_id"]}) через {delivery_method}',
                    metadata={
                        'recipient_id': recipient['user_id'],
                        'recipient_username': recipient['username'],
                        'delivery_method': delivery_method,
                        'subject': subject if delivery_method == 'email' else None
                    }
                )
            else:
                error_count += 1
                errors.append(f"{recipient['username']}: {result_message}")
        except Exception as e:
            error_count += 1
            errors.append(f"{recipient['username']}: {str(e)}")
            log_error(f"Error sending broadcast to {recipient['username']}: {e}")
    
    # Сохраняем историю рассылки
    conn = get_db_connection()
    try:
        errors_json = json.dumps(errors, ensure_ascii=False) if errors else None
        conn.execute('''
            INSERT INTO broadcasts_history 
            (created_by, created_by_username, recipient_type, delivery_method, subject, 
             message, total_recipients, success_count, error_count, errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            session.get('user_id'),
            session.get('username'),
            recipient_type,
            delivery_method,
            subject if delivery_method == 'email' else None,
            message,
            len(recipients),
            success_count,
            error_count,
            errors_json
        ))
        conn.commit()
    except Exception as e:
        log_error(f"Error saving broadcast history: {e}")
    finally:
        conn.close()
    
    # Формируем сообщение о результате
    if success_count > 0 and error_count == 0:
        flash(f'Рассылка успешно отправлена {success_count} получателям', 'success')
    elif success_count > 0:
        flash(f'Рассылка отправлена {success_count} получателям. Ошибок: {error_count}. Детали: {"; ".join(errors[:5])}', 'warning')
    else:
        flash(f'Не удалось отправить рассылку. Ошибки: {"; ".join(errors[:5])}', 'error')
    
    log_activity(
        'broadcast_completed',
        details=f'Рассылка завершена: успешно {success_count}, ошибок {error_count}',
        metadata={
            'recipient_type': recipient_type,
            'delivery_method': delivery_method,
            'success_count': success_count,
            'error_count': error_count,
            'total_recipients': len(recipients)
        }
    )
    
    return redirect(url_for('admin.admin_broadcasts'))


@bp.route('/broadcasts/templates', methods=['GET', 'POST'])
@require_role('admin')
def admin_broadcasts_templates():
    """Управление шаблонами рассылок"""
    if request.method == 'GET':
        # Редирект на главную страницу рассылок с табом шаблонов
        return redirect(url_for('admin.admin_broadcasts') + '#templates')
    
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            delivery_method = request.form.get('delivery_method', 'email')
            subject = request.form.get('subject', '').strip()
            message = request.form.get('message', '').strip()
            
            if not name or not message:
                flash('Название и текст сообщения обязательны', 'error')
                conn.close()
                return redirect(url_for('admin.admin_broadcasts_templates'))
            
            if delivery_method == 'email' and not subject:
                flash('Тема обязательна для email шаблона', 'error')
                conn.close()
                return redirect(url_for('admin.admin_broadcasts_templates'))
            
            try:
                conn.execute('''
                    INSERT INTO broadcast_templates 
                    (name, description, delivery_method, subject, message, created_by, created_by_username)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    name,
                    description,
                    delivery_method,
                    subject if delivery_method == 'email' else None,
                    message,
                    session.get('user_id'),
                    session.get('username')
                ))
                conn.commit()
                flash('Шаблон успешно создан', 'success')
                log_activity(
                    'broadcast_template_created',
                    details=f'Создан шаблон рассылки "{name}"',
                    metadata={'template_name': name, 'delivery_method': delivery_method}
                )
            except Exception as e:
                log_error(f"Error creating broadcast template: {e}")
                flash('Ошибка при создании шаблона', 'error')
        
        elif action == 'update':
            template_id = request.form.get('template_id')
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            delivery_method = request.form.get('delivery_method', 'email')
            subject = request.form.get('subject', '').strip()
            message = request.form.get('message', '').strip()
            
            if not template_id or not name or not message:
                flash('Название и текст сообщения обязательны', 'error')
                conn.close()
                return redirect(url_for('admin.admin_broadcasts_templates'))
            
            if delivery_method == 'email' and not subject:
                flash('Тема обязательна для email шаблона', 'error')
                conn.close()
                return redirect(url_for('admin.admin_broadcasts_templates'))
            
            try:
                conn.execute('''
                    UPDATE broadcast_templates
                    SET name = ?, description = ?, delivery_method = ?, 
                        subject = ?, message = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    name,
                    description,
                    delivery_method,
                    subject if delivery_method == 'email' else None,
                    message,
                    template_id
                ))
                conn.commit()
                flash('Шаблон успешно обновлен', 'success')
                log_activity(
                    'broadcast_template_updated',
                    details=f'Обновлен шаблон рассылки "{name}"',
                    metadata={'template_id': template_id, 'template_name': name}
                )
            except Exception as e:
                log_error(f"Error updating broadcast template: {e}")
                flash('Ошибка при обновлении шаблона', 'error')
        
        elif action == 'delete':
            template_id = request.form.get('template_id')
            if template_id:
                try:
                    template = conn.execute('SELECT name FROM broadcast_templates WHERE id = ?', (template_id,)).fetchone()
                    conn.execute('DELETE FROM broadcast_templates WHERE id = ?', (template_id,))
                    conn.commit()
                    flash('Шаблон успешно удален', 'success')
                    if template:
                        log_activity(
                            'broadcast_template_deleted',
                            details=f'Удален шаблон рассылки "{template["name"]}"',
                            metadata={'template_id': template_id}
                        )
                except Exception as e:
                    log_error(f"Error deleting broadcast template: {e}")
                    flash('Ошибка при удалении шаблона', 'error')
    
    conn.close()
    
    # После POST запроса редиректим обратно на страницу рассылок с табом шаблонов
    return redirect(url_for('admin.admin_broadcasts') + '#templates')


@bp.route('/broadcasts/templates/<int:template_id>')
@require_role('admin')
def admin_broadcasts_template_get(template_id):
    """Получение шаблона по ID (для AJAX)"""
    conn = get_db_connection()
    template = conn.execute('''
        SELECT id, name, description, delivery_method, subject, message
        FROM broadcast_templates
        WHERE id = ?
    ''', (template_id,)).fetchone()
    conn.close()
    
    if template:
        return jsonify(dict(template))
    return jsonify({'error': 'Template not found'}), 404
