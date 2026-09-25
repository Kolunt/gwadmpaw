"""Admin: broadcasts."""

import json

from flask import flash, jsonify, redirect, render_template, request, session, url_for

from gwadm.blueprints.admin import bp
from gwadm.db import get_db_connection
from gwadm.decorators import require_role
from gwadm.logging_config import log_error
from gwadm.services.activity import log_activity
from gwadm.services.broadcast_queue import enqueue_broadcast, get_active_queue_jobs
from gwadm.services.settings import get_setting


@bp.route('/broadcasts')
@require_role('admin')
def admin_broadcasts():
    """Страница рассылок"""
    conn = get_db_connection()
    users = conn.execute('''
        SELECT user_id, username, email, telegram, is_blocked
        FROM users
        ORDER BY username COLLATE NOCASE
    ''').fetchall()

    broadcasts_history_raw = conn.execute('''
        SELECT id, created_by, created_by_username, recipient_type, delivery_method,
               subject, message, total_recipients, success_count, error_count,
               errors, created_at
        FROM broadcasts_history
        ORDER BY created_at DESC
        LIMIT 50
    ''').fetchall()

    templates = conn.execute('''
        SELECT id, name, description, delivery_method, subject, message,
               created_by_username, created_at, updated_at
        FROM broadcast_templates
        ORDER BY updated_at DESC
    ''').fetchall()

    broadcast_queue = get_active_queue_jobs(conn)
    conn.close()

    broadcasts_history = []
    for item in broadcasts_history_raw:
        item_dict = dict(item)
        if item_dict.get('errors'):
            try:
                item_dict['errors_parsed'] = json.loads(item_dict['errors'])
            except (json.JSONDecodeError, TypeError):
                item_dict['errors_parsed'] = [item_dict['errors']] if item_dict['errors'] else []
        else:
            item_dict['errors_parsed'] = []
        broadcasts_history.append(item_dict)

    smtp_enabled = get_setting('smtp_enabled', '0') == '1'
    smtp_verified = get_setting('smtp_verified', '0') == '1'
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    telegram_verified = get_setting('telegram_verified', '0') == '1'

    return render_template(
        'admin/broadcasts.html',
        users=users,
        smtp_available=smtp_enabled and smtp_verified,
        telegram_available=telegram_enabled and telegram_verified,
        broadcasts_history=broadcasts_history,
        broadcast_queue=broadcast_queue,
        templates=templates,
    )


@bp.route('/broadcasts/send', methods=['POST'])
@require_role('admin')
def admin_broadcasts_send():
    """Поставить рассылку в очередь."""
    recipient_type = request.form.get('recipient_type', 'all')
    selected_users = request.form.getlist('selected_users')
    delivery_method = request.form.get('delivery_method', 'email')
    subject = request.form.get('subject', '').strip()
    message = request.form.get('message', '').strip()

    if not message:
        flash('Текст сообщения обязателен', 'error')
        return redirect(url_for('admin.admin_broadcasts'))

    if delivery_method == 'email' and not subject:
        flash('Тема письма обязательна для email рассылки', 'error')
        return redirect(url_for('admin.admin_broadcasts'))

    conn = get_db_connection()
    if recipient_type == 'all':
        if delivery_method == 'email':
            count_row = conn.execute('''
                SELECT COUNT(*) AS count FROM users
                WHERE email IS NOT NULL AND email != '' AND is_blocked = 0
            ''').fetchone()
        else:
            count_row = conn.execute('''
                SELECT COUNT(*) AS count FROM users
                WHERE telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
            ''').fetchone()
        recipient_user_ids = None
    else:
        if not selected_users:
            conn.close()
            flash('Выберите хотя бы одного получателя', 'error')
            return redirect(url_for('admin.admin_broadcasts'))
        recipient_user_ids = [int(uid) for uid in selected_users]
        placeholders = ','.join(['?'] * len(selected_users))
        if delivery_method == 'email':
            count_row = conn.execute(
                f'''
                SELECT COUNT(*) AS count FROM users
                WHERE user_id IN ({placeholders})
                  AND email IS NOT NULL AND email != '' AND is_blocked = 0
                ''',
                selected_users,
            ).fetchone()
        else:
            count_row = conn.execute(
                f'''
                SELECT COUNT(*) AS count FROM users
                WHERE user_id IN ({placeholders})
                  AND telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
                ''',
                selected_users,
            ).fetchone()
    conn.close()

    total_recipients = count_row['count'] if count_row else 0
    if total_recipients == 0:
        flash('Не найдено получателей с указанным способом доставки', 'error')
        return redirect(url_for('admin.admin_broadcasts'))

    queue_id = enqueue_broadcast(
        created_by=session.get('user_id'),
        created_by_username=session.get('username'),
        recipient_type=recipient_type,
        delivery_method=delivery_method,
        subject=subject,
        message=message,
        recipient_user_ids=recipient_user_ids,
        total_recipients=total_recipients,
    )

    flash(
        f'Рассылка поставлена в очередь (#{queue_id}, {total_recipients} получателей). '
        'Доставка начнётся в течение нескольких минут.',
        'success',
    )
    log_activity(
        'broadcast_queued',
        details=f'Рассылка #{queue_id} поставлена в очередь ({total_recipients} получателей)',
        metadata={
            'queue_id': queue_id,
            'recipient_type': recipient_type,
            'delivery_method': delivery_method,
            'total_recipients': total_recipients,
        },
    )
    return redirect(url_for('admin.admin_broadcasts'))


@bp.route('/broadcasts/templates', methods=['GET', 'POST'])
@require_role('admin')
def admin_broadcasts_templates():
    """Управление шаблонами рассылок"""
    if request.method == 'GET':
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
                    session.get('username'),
                ))
                conn.commit()
                flash('Шаблон успешно создан', 'success')
                log_activity(
                    'broadcast_template_created',
                    details=f'Создан шаблон рассылки "{name}"',
                    metadata={'template_name': name, 'delivery_method': delivery_method},
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
                    template_id,
                ))
                conn.commit()
                flash('Шаблон успешно обновлен', 'success')
                log_activity(
                    'broadcast_template_updated',
                    details=f'Обновлен шаблон рассылки "{name}"',
                    metadata={'template_id': template_id, 'template_name': name},
                )
            except Exception as e:
                log_error(f"Error updating broadcast template: {e}")
                flash('Ошибка при обновлении шаблона', 'error')

        elif action == 'delete':
            template_id = request.form.get('template_id')
            if template_id:
                try:
                    template = conn.execute(
                        'SELECT name FROM broadcast_templates WHERE id = ?',
                        (template_id,),
                    ).fetchone()
                    conn.execute('DELETE FROM broadcast_templates WHERE id = ?', (template_id,))
                    conn.commit()
                    flash('Шаблон успешно удален', 'success')
                    if template:
                        log_activity(
                            'broadcast_template_deleted',
                            details=f'Удален шаблон рассылки "{template["name"]}"',
                            metadata={'template_id': template_id},
                        )
                except Exception as e:
                    log_error(f"Error deleting broadcast template: {e}")
                    flash('Ошибка при удалении шаблона', 'error')

    conn.close()
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
