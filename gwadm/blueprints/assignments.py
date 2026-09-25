"""User letter and assignment routes."""

import os
from datetime import datetime

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.config import (
    ALLOWED_LETTER_IMAGE_EXTENSIONS,
    ASSIGNMENT_RECEIPT_RELATIVE,
    LETTER_UPLOAD_FOLDER,
    LETTER_UPLOAD_RELATIVE,
)
from gwadm.services.assignments import (
    _format_full_address,
    get_admin_letter_assignments,
    get_user_assignments,
    mark_assignment_received,
    mark_assignment_sent,
)
from gwadm.services.events import is_event_finished
from gwadm.services.events_stages import parse_event_datetime
from gwadm.services.rating import _normalize_multiline_text
from gwadm.services.roles import has_role
from gwadm.services.uploads import save_validated_image, validate_image_upload

bp = Blueprint('assignments', __name__)

@bp.route('/letter', methods=['GET', 'POST'])
@require_login
def letter():
    """Страница с письмом получателя для Деда Мороза"""
    user_id = session.get('user_id')
    if not user_id:
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('auth.login'))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Не удалось определить пользователя', 'error')
        return redirect(url_for('auth.login'))

    assignment_id = request.args.get('assignment_id', type=int)
    if assignment_id is None and request.method == 'POST':
        assignment_id = request.form.get('assignment_id', type=int)

    is_admin = has_role(user_id_int, 'admin')
    admin_override = is_admin and (request.args.get('admin') == '1' or request.form.get('admin') == '1')

    if admin_override and request.method == 'POST':
        flash('Администраторы просматривают переписки только в режиме чтения.', 'error')
        return redirect(url_for('assignments.letter', assignment_id=assignment_id, admin=1) if assignment_id else url_for('admin.admin_letters'))

    accessible_assignments = []
    if admin_override:
        # Для админа показываем активные чаты
        accessible_assignments = get_admin_letter_assignments()
        
        # Также добавляем архивированные чаты, если запрашивается конкретный assignment_id
        if assignment_id:
            conn = get_db_connection()
            archived_chat = conn.execute('''
                SELECT original_assignment_id, event_id, santa_user_id, recipient_user_id
                FROM assignment_chat_history
                WHERE original_assignment_id = ?
            ''', (assignment_id,)).fetchone()
            conn.close()
            
            if archived_chat:
                # Получаем информацию об архивированном чате
                conn = get_db_connection()
                archived_info = conn.execute('''
                    SELECT
                        ea.*,
                        e.name AS event_name,
                        santa.username AS santa_username,
                        santa.first_name AS santa_first_name,
                        santa.last_name AS santa_last_name,
                        santa.middle_name AS santa_middle_name,
                        COALESCE(sd.country, santa.country) AS santa_country,
                        COALESCE(sd.city, santa.city) AS santa_city,
                        recipient.username AS recipient_username,
                        COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
                        COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
                        COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
                        COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
                        COALESCE(rd.country, recipient.country) AS recipient_country,
                        COALESCE(rd.city, recipient.city) AS recipient_city,
                        COALESCE(rd.street, recipient.street) AS recipient_street,
                        COALESCE(rd.house, recipient.house) AS recipient_house,
                        COALESCE(rd.building, recipient.building) AS recipient_building,
                        COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
                        rd.bio AS recipient_bio
                    FROM event_assignments ea
                    JOIN events e ON ea.event_id = e.id
                    JOIN users santa ON ea.santa_user_id = santa.user_id
                    JOIN users recipient ON ea.recipient_user_id = recipient.user_id
                    LEFT JOIN event_registration_details rd
                        ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
                    LEFT JOIN event_registration_details sd
                        ON sd.event_id = ea.event_id AND sd.user_id = ea.santa_user_id
                    WHERE ea.id = ?
                ''', (assignment_id,)).fetchone()
                conn.close()
                
                if archived_info:
                    archived_dict = dict(archived_info)
                    archived_dict['chat_role'] = 'admin'
                    archived_dict['is_archived'] = True
                    accessible_assignments.append(archived_dict)
    else:
        user_assignments = get_user_assignments(user_id_int)
        for assignment in user_assignments:
            role = None
            if assignment.get('santa_user_id') == user_id_int:
                role = 'santa'
            elif assignment.get('recipient_user_id') == user_id_int:
                role = 'grandchild'
            if not role:
                continue
            assignment_copy = dict(assignment)
            assignment_copy['chat_role'] = role
            accessible_assignments.append(assignment_copy)

    if not accessible_assignments:
        if admin_override:
            flash('Пока нет переписок для отображения.', 'info')
            return redirect(url_for('admin.admin_letters'))
        flash('У вас пока нет переписок для отображения.', 'info')
        return redirect(url_for('assignments.assignments'))

    selected_assignment = None
    if assignment_id:
        for assignment in accessible_assignments:
            if assignment.get('id') == assignment_id:
                selected_assignment = assignment
                break
        if not selected_assignment:
            flash('Выбранное задание не найдено. Показано первое доступное письмо.', 'warning')

    if not selected_assignment:
        selected_assignment = accessible_assignments[0]

    user_role = selected_assignment.get('chat_role', 'santa')

    event_finished = is_event_finished(selected_assignment.get('event_id'))

    if request.method == 'POST' and not admin_override:
        if event_finished:
            flash('Мероприятие завершено. Переписка доступна только для чтения.', 'error')
            return redirect(url_for('assignments.letter', assignment_id=selected_assignment.get('id')))

    if request.method == 'POST':
        message = _normalize_multiline_text(request.form.get('message'), max_length=2000)
        attachment_file = request.files.get('attachment')
        has_attachment = attachment_file and attachment_file.filename

        if not message and not has_attachment:
            flash('Введите сообщение или прикрепите изображение.', 'error')
            return redirect(url_for('assignments.letter', assignment_id=selected_assignment.get('id')))

        attachment_relative_path = None
        saved_filepath = None

        if has_attachment:
            data, upload_error = validate_image_upload(
                attachment_file, ALLOWED_LETTER_IMAGE_EXTENSIONS,
            )
            if upload_error:
                flash(upload_error, 'error')
                return redirect(url_for('assignments.letter', assignment_id=selected_assignment.get('id')))

            _, ext = os.path.splitext(attachment_file.filename)
            ext = ext.lower()
            if ext == '.jpeg':
                ext = '.jpg'
            try:
                unique_name = save_validated_image(
                    data,
                    LETTER_UPLOAD_FOLDER,
                    str(selected_assignment.get('id')),
                    ext,
                )
            except Exception as exc:
                log_error(f"Failed to save letter attachment: {exc}")
                flash('Не удалось загрузить изображение.', 'error')
                return redirect(url_for('assignments.letter', assignment_id=selected_assignment.get('id')))

            attachment_relative_path = f"{LETTER_UPLOAD_RELATIVE}/{unique_name}"

        conn = get_db_connection()
        try:
            conn.execute('''
                INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
                VALUES (?, ?, ?, ?)
            ''', (selected_assignment.get('id'), user_role, message, attachment_relative_path))
            conn.commit()
            flash('Сообщение отправлено.', 'success')
        except Exception as exc:
            conn.rollback()
            if saved_filepath and os.path.exists(saved_filepath):
                try:
                    os.remove(saved_filepath)
                except OSError:
                    pass
            log_error(f"Error saving letter message for assignment {selected_assignment.get('id')}: {exc}")
            flash('Не удалось сохранить сообщение.', 'error')
        finally:
            conn.close()

        return redirect(url_for('assignments.letter', assignment_id=selected_assignment.get('id')))

    recipient_first_name = (selected_assignment.get('recipient_first_name')
                            or selected_assignment.get('recipient_username')
                            or '').strip()
    recipient_middle_name = (selected_assignment.get('recipient_middle_name') or '').strip()
    recipient_last_name = (selected_assignment.get('recipient_last_name') or '').strip()

    recipient_full_name_parts = [
        part for part in [recipient_last_name, recipient_first_name, recipient_middle_name] if part
    ]
    default_signature = 'Твой внучок'
    recipient_full_name = (
        ' '.join(recipient_full_name_parts)
        if recipient_full_name_parts else (recipient_first_name or recipient_last_name or default_signature)
    )

    recipient_address = _format_full_address(selected_assignment)

    recipient_bio = selected_assignment.get('recipient_bio')
    if recipient_bio:
        recipient_bio = recipient_bio.strip()
    if not recipient_bio:
        recipient_bio = 'Я пока не успел рассказать о себе, но обязательно сделаю это совсем скоро!'

    santa_country = selected_assignment.get('santa_country')
    santa_city = selected_assignment.get('santa_city')
    origin_parts = []
    if santa_country:
        origin_parts.append(santa_country)
    if santa_city:
        origin_parts.append(santa_city)
    if not origin_parts:
        origin_parts.append('Россия')
    santa_origin = ', '.join(origin_parts)

    letter_context = {
        'event_name': selected_assignment.get('event_name', 'Мероприятие'),
        'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
        'grandchild': {
            'first_name': recipient_first_name or recipient_full_name or default_signature,
            'full_name': recipient_full_name,
            'address': recipient_address,
            'bio': recipient_bio,
        },
        'santa': {
            'origin': santa_origin,
        }
    }

    available_letters = []
    for assignment in accessible_assignments:
        role = assignment.get('chat_role', 'santa')
        label = ''
        if role == 'santa':
            counterpart = assignment.get('recipient_first_name') or assignment.get('recipient_username') or assignment.get('recipient_last_name') or 'Получатель'
            label = f"Получатель: {counterpart}"
        elif role == 'grandchild':
            counterpart = assignment.get('santa_username') or 'Дед Мороз'
            label = f"Дед Мороз: {counterpart}"
        elif role == 'admin':
            santa_label = assignment.get('santa_full_name') or assignment.get('santa_username') or 'Дед Мороз'
            recipient_label = assignment.get('recipient_full_name') or assignment.get('recipient_username') or 'Внучок'
            label = f"Санта: {santa_label} → Внучок: {recipient_label}"
        available_letters.append({
            'assignment_id': assignment.get('id'),
            'event_name': assignment.get('event_name', 'Мероприятие'),
            'label': label,
            'role': role,
            'santa_label': assignment.get('santa_full_name') or assignment.get('santa_username'),
            'recipient_label': assignment.get('recipient_full_name') or assignment.get('recipient_username')
        })

    conn = get_db_connection()
    raw_messages = conn.execute('''
        SELECT id, sender, message, created_at, attachment_path
        FROM letter_messages
        WHERE assignment_id = ?
        ORDER BY created_at ASC, id ASC
    ''', (selected_assignment.get('id'),)).fetchall()
    chat_messages = []
    message_updates = []
    earliest_dt = None
    for row in raw_messages:
        message_text = row['message']
        if message_text:
            normalized = _normalize_multiline_text(message_text)
            if normalized != message_text:
                message_text = normalized
                message_updates.append((normalized, row['id']))
        created_raw = row['created_at']
        created_display = ''
        created_dt = None
        if created_raw:
            try:
                created_dt = datetime.fromisoformat(str(created_raw))
            except ValueError:
                try:
                    created_dt = datetime.strptime(str(created_raw), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    created_dt = None
            if created_dt:
                created_display = created_dt.strftime('%d.%m.%Y %H:%M')
                if earliest_dt is None or created_dt < earliest_dt:
                    earliest_dt = created_dt
        attachment_rel = row['attachment_path']
        attachment_url = url_for('static', filename=attachment_rel) if attachment_rel else None

        chat_messages.append({
            'sender': row['sender'],
            'message': message_text,
            'created_display': created_display,
            'created_iso': str(created_raw) if created_raw is not None else '',
            'attachment_url': attachment_url
        })

    if message_updates:
        conn.executemany('UPDATE letter_messages SET message = ? WHERE id = ?', message_updates)
        conn.commit()
    conn.close()

    if earliest_dt is None:
        candidate_dates = []
        for key in ('assigned_at', 'santa_sent_at', 'recipient_received_at'):
            value = selected_assignment.get(key)
            if value:
                parsed = parse_event_datetime(value)
                if parsed:
                    candidate_dates.append(parsed)
        if candidate_dates:
            earliest_dt = min(candidate_dates)
    if earliest_dt is None:
        earliest_dt = datetime.now()
    letter_context['date'] = earliest_dt.strftime('%d.%m.%Y %H:%M')

    return render_template(
        'letter.html',
        letter=letter_context,
        assignment=selected_assignment,
        available_letters=available_letters,
        user_role=user_role,
        chat_messages=chat_messages,
        admin_view=admin_override,
        admin_letters_url=url_for('admin.admin_letters') if admin_override else None,
        event_finished=event_finished
    )



@bp.route('/assignments')
@require_login
def assignments():
    """Страница заданий для пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('auth.login'))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Не удалось определить пользователя', 'error')
        return redirect(url_for('auth.login'))
    
    user_assignments = get_user_assignments(user_id_int)
    
    # Группируем задания по мероприятиям
    assignments_by_event = {}
    for assignment in user_assignments:
        event_id = assignment.get('event_id')
        if not event_id:
            continue
        
        if event_id not in assignments_by_event:
            assignments_by_event[event_id] = {
                'event_name': assignment.get('event_name', 'Мероприятие'),
                'as_santa': None,  # Где пользователь Дед Мороз
                'as_recipient': None,  # Где пользователь Внучка
                'event_finished': is_event_finished(event_id)
            }
        
        # Проверяем, является ли пользователь Дедом Морозом в этом задании
        if assignment.get('santa_user_id') == user_id_int:
            assignments_by_event[event_id]['as_santa'] = assignment
        # Проверяем, является ли пользователь Внучкой в этом задании
        elif assignment.get('recipient_user_id') == user_id_int:
            assignments_by_event[event_id]['as_recipient'] = assignment
    
    return render_template('assignments.html', assignments_by_event=assignments_by_event)

@bp.route('/assignments/<int:assignment_id>/send', methods=['POST'])
@require_login
def assignment_mark_sent(assignment_id):
    """Обработчик отметки отправки подарка"""
    user_id = session.get('user_id')
    send_info = request.form.get('send_info', '').strip()
    
    success, message = mark_assignment_sent(assignment_id, user_id, send_info)
    flash(message, 'success' if success else 'error')
    
    return redirect(url_for('assignments.assignments'))


@bp.route('/assignments/<int:assignment_id>/receive', methods=['POST'])
@require_login
def assignment_mark_received(assignment_id):
    """Обработчик подтверждения получения подарка"""
    user_id = session.get('user_id')
    thank_you_message = (request.form.get('thank_you_message') or '').strip()
    receipt_file = request.files.get('receipt_image')

    success, message = mark_assignment_received(
        assignment_id,
        user_id,
        thank_you_message,
        receipt_file
    )
    flash(message, 'success' if success else 'error')
    
    return redirect(url_for('assignments.assignments'))
