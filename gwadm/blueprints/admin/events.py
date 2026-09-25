"""Admin: events."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/events')
@require_role('admin')
def admin_events():
    """Список мероприятий"""
    conn = get_db_connection()
    events = conn.execute('''
        SELECT e.*, u.username as creator_name,
               COUNT(es.id) as stages_count
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        LEFT JOIN event_stages es ON e.id = es.event_id
        GROUP BY e.id
        ORDER BY e.created_at DESC
    ''').fetchall()
    conn.close()
    
    events_with_info = []
    for event in events:
        event_dict = dict(event)
        current_stage = get_current_event_stage(event_dict['id'])
        event_dict['current_stage'] = current_stage
        event_dict['needs_review'] = False
        event_dict['review_pending_count'] = 0
        event_dict['review_approved_count'] = 0
        
        if current_stage and current_stage.get('info', {}).get('type') == 'registration_closed':
            event_id = event_dict['id']
            create_participant_approvals_for_event(event_id)
            conn_counts = get_db_connection()
            counts = conn_counts.execute('''
                SELECT 
                    SUM(CASE WHEN approved = 1 THEN 1 ELSE 0 END) as approved_count,
                    SUM(CASE WHEN approved IS NULL OR approved = 0 THEN 1 ELSE 0 END) as pending_count
                FROM event_participant_approvals
                WHERE event_id = ?
            ''', (event_id,)).fetchone()
            conn_counts.close()
            
            approved_count = counts['approved_count'] if counts and counts['approved_count'] else 0
            pending_count = counts['pending_count'] if counts and counts['pending_count'] else 0
            
            event_dict['needs_review'] = True
            event_dict['review_pending_count'] = pending_count
            event_dict['review_approved_count'] = approved_count
        
        events_with_info.append(event_dict)
    
    return render_template('admin/events.html', events=events_with_info)


@bp.route('/events/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_event_create():
    """Создание мероприятия"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        award_id = request.form.get('award_id', '').strip()
        award_id = int(award_id) if award_id else None
        
        # Получаем настройки рейтинга
        rating_registration = request.form.get('rating_registration', '').strip()
        rating_registration = int(rating_registration) if rating_registration else None
        rating_gift_not_sent = request.form.get('rating_gift_not_sent', '').strip()
        rating_gift_not_sent = int(rating_gift_not_sent) if rating_gift_not_sent else None
        rating_gift_sent = request.form.get('rating_gift_sent', '').strip()
        rating_gift_sent = int(rating_gift_sent) if rating_gift_sent else None
        rating_order_coefficient = request.form.get('rating_order_coefficient', '').strip()
        rating_order_coefficient = float(rating_order_coefficient) if rating_order_coefficient else None
        
        if not name:
            flash('Название мероприятия обязательно', 'error')
            conn = get_db_connection()
            awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
            conn.close()
            return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES, awards=awards)
        
        conn = get_db_connection()
        try:
            # Создаем мероприятие
            cursor = conn.execute('''
                INSERT INTO events (name, description, created_by, award_id, rating_registration, rating_gift_not_sent, rating_gift_sent, rating_order_coefficient)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, description, session.get('user_id'), award_id, rating_registration, rating_gift_not_sent, rating_gift_sent, rating_order_coefficient))
            event_id = cursor.lastrowid
            
            # Создаем этапы
            stage_order = 1
            for stage in EVENT_STAGES:
                start_datetime = None
                end_datetime = None
                
                if stage['has_start']:
                    start_str = request.form.get(f"stage_{stage['type']}_start", '').strip()
                    if start_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in start_str:
                                if len(start_str) == 16:  # YYYY-MM-DDTHH:MM
                                    start_datetime = datetime.strptime(start_str, '%Y-%m-%dT%H:%M')
                                elif len(start_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    start_datetime = datetime.strptime(start_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                start_datetime = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты начала этапа {stage['type']}: {e}, строка: {start_str}")
                            pass
                
                if stage['has_end']:
                    end_str = request.form.get(f"stage_{stage['type']}_end", '').strip()
                    if end_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in end_str:
                                if len(end_str) == 16:  # YYYY-MM-DDTHH:MM
                                    end_datetime = datetime.strptime(end_str, '%Y-%m-%dT%H:%M')
                                elif len(end_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    end_datetime = datetime.strptime(end_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                end_datetime = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты окончания этапа {stage['type']}: {e}, строка: {end_str}")
                            pass
                
                # Проверяем обязательность
                is_required = 1 if stage['required'] else 0
                is_optional = 1 if not stage['required'] else 0
                
                # Для обязательных этапов проверяем наличие даты начала
                if stage['required'] and stage['has_start'] and not start_datetime:
                    flash(f'Дата начала этапа "{stage["name"]}" обязательна', 'error')
                    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES, awards=awards)
                
                # Форматируем datetime для сохранения в БД
                start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
                end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None
                
                log_debug(f"Создание этапа {stage['type']}: start={start_datetime_str}, end={end_datetime_str}")
                
                conn.execute('''
                    INSERT INTO event_stages 
                    (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (event_id, stage['type'], stage_order, start_datetime_str, end_datetime_str, is_required, is_optional))
                stage_order += 1
            
            conn.commit()
            flash('Мероприятие успешно создано', 'success')
            conn.close()
            return redirect(url_for('admin.admin_events'))
        except Exception as e:
            log_error(f"Error creating event: {e}")
            flash(f'Ошибка создания мероприятия: {str(e)}', 'error')
            conn.rollback()
            conn.close()
    
    # GET запрос - получаем список наград
    conn = get_db_connection()
    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
    conn.close()
    return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES, awards=awards)

@bp.route('/events/<int:event_id>')
@require_role('admin')
def admin_event_view(event_id):
    """Просмотр мероприятия"""
    conn = get_db_connection()
    event = conn.execute('SELECT * FROM events WHERE id = ?', (event_id,)).fetchone()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        conn.close()
        return redirect(url_for('admin.admin_events'))
    
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    
    conn.close()
    
    # Сопоставляем этапы с их типами
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    stages_with_info = []
    for stage_info in EVENT_STAGES:
        stage_data = stages_dict.get(stage_info['type'], None)
        stages_with_info.append({
            'info': stage_info,
            'data': stage_data
        })
    
    # Определяем текущий этап для отображения кнопки ревью
    current_stage = get_current_event_stage(event_id)
    
    # Преобразуем event в словарь для корректной работы в шаблоне
    event_dict = dict(event) if event else {}
    
    # Получаем глобальные настройки рейтинга для сравнения
    global_rating_registration = get_rating_setting('rating_event_registration', 1)
    global_rating_gift_not_sent = get_rating_setting('rating_event_gift_not_sent', 0)
    global_rating_gift_sent = get_rating_setting('rating_event_gift_sent', 0)
    
    return render_template('admin/event_view.html', 
                         event=event_dict, 
                         stages_with_info=stages_with_info, 
                         current_stage=current_stage,
                         global_rating_registration=global_rating_registration,
                         global_rating_gift_not_sent=global_rating_gift_not_sent,
                         global_rating_gift_sent=global_rating_gift_sent)



@bp.route('/events/<int:event_id>/participants')
@require_role('admin')
def admin_event_participants(event_id):
    """Детальный список участников мероприятия для администраторов"""
    conn = get_db_connection()
    event = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.id = ?
    ''', (event_id,)).fetchone()

    if not event:
        conn.close()
        flash('Мероприятие не найдено', 'error')
        return redirect(url_for('admin.admin_events'))

    stages = conn.execute('''
        SELECT stage_type, start_datetime
        FROM event_stages
        WHERE event_id = ?
    ''', (event_id,)).fetchall()

    participants = conn.execute('''
        SELECT 
            er.user_id,
            er.registered_at,
            COALESCE(d.last_name, u.last_name) AS last_name,
            COALESCE(d.first_name, u.first_name) AS first_name,
            COALESCE(d.middle_name, u.middle_name) AS middle_name,
            COALESCE(d.postal_code, u.postal_code) AS postal_code,
            COALESCE(d.country, u.country) AS country,
            COALESCE(d.city, u.city) AS city,
            COALESCE(d.street, u.street) AS street,
            COALESCE(d.house, u.house) AS house,
            COALESCE(d.building, u.building) AS building,
            COALESCE(d.apartment, u.apartment) AS apartment,
            COALESCE(d.phone, u.phone) AS phone,
            COALESCE(d.telegram, u.telegram) AS telegram,
            COALESCE(d.whatsapp, u.whatsapp) AS whatsapp,
            COALESCE(d.viber, u.viber) AS viber,
            u.username,
            u.avatar_seed,
            u.avatar_style,
            u.email,
            epa.approved AS approval_flag,
            epa.notes AS approval_notes,
            epa.approved_at AS approval_timestamp,
            epa.approved_by AS approval_by
        FROM event_registrations er
        LEFT JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
        LEFT JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
        WHERE er.event_id = ?
        ORDER BY u.username COLLATE NOCASE
    ''', (event_id,)).fetchall()
    conn.close()

    stage_times = {row['stage_type']: row['start_datetime'] for row in stages}

    def parse_dt(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            try:
                return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None

    pre_start = parse_dt(stage_times.get('pre_registration'))
    main_start = parse_dt(stage_times.get('main_registration'))
    registration_closed_start = parse_dt(stage_times.get('registration_closed'))

    participants_data = []
    for row in participants:
        registered_at_dt = parse_dt(row['registered_at'])

        stage_label = 'main'
        if pre_start and main_start and registered_at_dt:
            if registered_at_dt >= pre_start and registered_at_dt < main_start:
                stage_label = 'pre'
            else:
                stage_label = 'main'
        elif pre_start and registered_at_dt and not main_start:
            if registered_at_dt < pre_start:
                stage_label = 'pre'
        elif main_start and registered_at_dt:
            stage_label = 'pre' if registered_at_dt < main_start else 'main'

        if registration_closed_start and registered_at_dt and registered_at_dt >= registration_closed_start:
            stage_label = 'main'

        approval_flag = row['approval_flag']
        approval_timestamp = row['approval_timestamp']
        approval_status = 'pending'
        if approval_flag == 1:
            approval_status = 'approved'
        elif approval_flag == 0 and approval_timestamp:
            approval_status = 'rejected'

        approval_notes = row['approval_notes']

        participants_data.append({
            'user_id': row['user_id'],
            'username': row['username'] or f'ID {row["user_id"]}',
            'registered_at': row['registered_at'],
            'last_name': row['last_name'],
            'first_name': row['first_name'],
            'middle_name': row['middle_name'],
            'postal_code': row['postal_code'],
            'country': row['country'],
            'city': row['city'],
            'street': row['street'],
            'house': row['house'],
            'building': row['building'],
            'apartment': row['apartment'],
            'phone': row['phone'],
            'telegram': row['telegram'],
            'whatsapp': row['whatsapp'],
            'viber': row['viber'],
            'email': row['email'],
            'stage': stage_label,
            'can_upgrade_to_main': stage_label == 'pre',
            'can_downgrade_to_pre': stage_label == 'main',
            'approval_status': approval_status,
            'approval_notes': approval_notes,
            'can_confirm_participant': stage_label == 'main' and approval_status != 'approved',
            'can_reject_participant': stage_label == 'main' and approval_status != 'rejected'
        })

    pre_participants = [p for p in participants_data if p['stage'] == 'pre']
    main_participants = [p for p in participants_data if p['stage'] == 'main']
    positive_participants = [p for p in participants_data if p['approval_status'] == 'approved']
    negative_participants = [p for p in participants_data if p['approval_status'] == 'rejected']
    na_participants = [p for p in participants_data if p['stage'] == 'main' and p['approval_status'] == 'pending']

    return render_template(
        'admin/event_participants.html',
        event=event,
        participants_all=participants_data,
        participants_pre=pre_participants,
        participants_main=main_participants,
        participants_positive=positive_participants,
        participants_negative=negative_participants,
        participants_na=na_participants,
        participants_count=len(participants_data),
        participants_pre_count=len(pre_participants),
        participants_main_count=len(main_participants),
        participants_positive_count=len(positive_participants),
        participants_negative_count=len(negative_participants),
        participants_na_count=len(na_participants)
    )

@bp.route('/events/<int:event_id>/distribution/positive')
@require_role('admin')
def admin_event_distribution_positive_view(event_id):
    """Отображает участников со статусом 'Позитив' для распределения"""
    try:
        conn = get_db_connection()
        event = conn.execute('''
            SELECT e.*, u.username as creator_name
            FROM events e
            LEFT JOIN users u ON e.created_by = u.user_id
            WHERE e.id = ?
        ''', (event_id,)).fetchone()

        if not event:
            conn.close()
            flash('Мероприятие не найдено', 'error')
            return redirect(url_for('admin.admin_events'))

        participants = conn.execute('''
            SELECT 
                er.user_id,
                u.username,
                u.last_name,
                u.first_name,
                u.middle_name,
                COALESCE(d.postal_code, u.postal_code) AS postal_code,
                COALESCE(d.country, u.country) AS country,
                COALESCE(d.city, u.city) AS city,
                COALESCE(d.street, u.street) AS street,
                COALESCE(d.house, u.house) AS house,
                COALESCE(d.building, u.building) AS building,
                COALESCE(d.apartment, u.apartment) AS apartment,
                COALESCE(d.phone, u.phone) AS phone,
                COALESCE(d.telegram, u.telegram) AS telegram,
                COALESCE(d.whatsapp, u.whatsapp) AS whatsapp,
                COALESCE(d.viber, u.viber) AS viber,
                epa.notes as approval_notes,
                er.registered_at
            FROM event_registrations er
            LEFT JOIN users u ON er.user_id = u.user_id
            LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
            INNER JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
            WHERE er.event_id = ?
              AND epa.approved = 1
            ORDER BY u.username COLLATE NOCASE
        ''', (event_id,)).fetchall()
        conn.close()

        participants_data = []
        participants_lookup = {}
        for row in participants:
            participant_dict = {
            'user_id': row['user_id'],
            'username': row['username'] or f'ID {row["user_id"]}',
            'last_name': row['last_name'],
            'first_name': row['first_name'],
            'middle_name': row['middle_name'],
            'address': {
                'postal_code': row['postal_code'],
                'country': row['country'],
                'city': row['city'],
                'street': row['street'],
                'house': row['house'],
                'building': row['building'],
                'apartment': row['apartment'],
            },
            'country': row['country'],
            'city': row['city'],
            'contacts': {
                'phone': row['phone'],
                'telegram': row['telegram'],
                'whatsapp': row['whatsapp'],
                'viber': row['viber'],
            },
            'notes': row['approval_notes'],
                'registered_at': row['registered_at'],
            }
            participants_data.append(participant_dict)
            participants_lookup[row['user_id']] = participant_dict

        conn_assignments = get_db_connection()
        # Загружаем пары и проверяем наличие сообщений от Деда Мороза для определения статуса отправки
        # Используем подзапрос для проверки наличия сообщений
        saved_rows = conn_assignments.execute('''
        SELECT 
            ea.santa_user_id, 
            ea.recipient_user_id, 
            ea.santa_sent_at, 
            ea.santa_send_info, 
            ea.recipient_received_at, 
            ea.locked, 
            ea.assignment_locked,
            ea.id as assignment_id,
            CASE 
                WHEN (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '') THEN 1
                WHEN EXISTS (
                    SELECT 1 FROM letter_messages lm 
                    WHERE lm.assignment_id = ea.id AND lm.sender = 'santa'
                ) THEN 1
                ELSE 0 
            END as has_sent_indicator
        FROM event_assignments ea
        WHERE ea.event_id = ?
          AND (ea.is_archived = 0 OR ea.is_archived IS NULL)
        ORDER BY ea.assigned_at ASC, ea.id ASC
        ''', (event_id,)).fetchall()
        conn_assignments.close()

        saved_pairs = []
        locked_santas = set()
        for record in saved_rows:
            santa = participants_lookup.get(record['santa_user_id'])
            recipient = participants_lookup.get(record['recipient_user_id'])
            if not santa or not recipient:
                continue
            locked_flag = bool(record['locked'])
            assignment_locked_flag = bool(record['assignment_locked'])
            if assignment_locked_flag:
                locked_santas.add(record['santa_user_id'])
            # Если santa_sent_at пустой, но есть сообщение от Деда Мороза, считаем что подарок отправлен
            santa_sent_at = record['santa_sent_at']
            has_sent_indicator = bool(record['has_sent_indicator']) if 'has_sent_indicator' in record.keys() else False
            
            saved_pairs.append({
                'santa_id': santa['user_id'],
                'santa_name': santa['username'],
                'santa_country': santa.get('country'),
                'santa_city': santa.get('city'),
                'recipient_id': recipient['user_id'],
                'recipient_name': recipient['username'],
                'recipient_country': recipient.get('country'),
                'recipient_city': recipient.get('city'),
                'santa_sent_at': santa_sent_at if (santa_sent_at and santa_sent_at != '') else None,
                'santa_send_info': record['santa_send_info'] if 'santa_send_info' in record.keys() else None,
                'recipient_received_at': record['recipient_received_at'],
                'has_sent_indicator': has_sent_indicator,
                'locked': locked_flag,
                'assignment_locked': assignment_locked_flag
            })

        # Определяем, какие участники имеют пары (сформированы)
        participants_with_pairs = set()
        for pair in saved_pairs:
            participants_with_pairs.add(pair['santa_id'])
            participants_with_pairs.add(pair['recipient_id'])
        
        # Считаем незакрепленные пары (без замков) для вкладки "unformed"
        unformed_pairs_count = len([p for p in saved_pairs if not p.get('locked', False)])
        
        distribution_url = url_for('admin.admin_event_distribution_positive_generate', event_id=event_id)
        distribution_save_url = url_for('admin.admin_event_distribution_positive_save', event_id=event_id)

        return render_template(
            'admin/event_distribution.html',
            event=event,
            distribution_type='positive',
            participants=participants_data,
            participants_count=len(participants_data),
            participants_with_pairs=participants_with_pairs,
            unformed_pairs_count=unformed_pairs_count,
            distribution_generate_url=distribution_url,
            distribution_save_url=distribution_save_url,
            distribution_create_assignments_url=url_for('admin.admin_event_distribution_positive_create_assignments', event_id=event_id),
            distribution_unassign_url=url_for('admin.admin_event_distribution_positive_unassign', event_id=event_id),
            saved_pairs=saved_pairs,
            saved_locked_santas=list(locked_santas)
        )
    except Exception as e:
        log_error(f"Error in admin_event_distribution_positive_view for event {event_id}: {e}")
        log_error(f"Traceback: {traceback.format_exc()}")
        flash(f'Ошибка при загрузке страницы распределения: {str(e)}', 'error')
        return redirect(url_for('admin.admin_event_view', event_id=event_id))


@bp.route('/events/<int:event_id>/distribution/positive/assignments', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_create_assignments(event_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходима авторизация'}), 403

    conn = get_db_connection()
    rows = conn.execute('''
        SELECT santa_user_id, recipient_user_id, santa_sent_at, recipient_received_at, locked
        FROM event_assignments
        WHERE event_id = ?
        ORDER BY assigned_at ASC, id ASC
    ''', (event_id,)).fetchall()
    conn.close()

    log_debug(f"create_assignments: event {event_id}, saved_pairs={len(rows)}")
    if not rows:
        return jsonify({'success': False, 'error': 'Нет сохранённого распределения для создания заданий'}), 400

    if any(not row['locked'] for row in rows):
        return jsonify({'success': False, 'error': 'Закрепите замком каждую пару перед созданием заданий.'}), 400

    assignments = [(row['santa_user_id'], row['recipient_user_id']) for row in rows]
    success, result = save_event_assignments(
        event_id,
        assignments,
        user_id,
        locked_pairs={(row['santa_user_id'], row['recipient_user_id']) for row in rows},
        assignment_locked=True
    )
    if success:
        log_debug(f"create_assignments: assignments locked for event {event_id}, count={result}")
        return jsonify({'success': True, 'message': f'Создано {result} заданий для участников.'})
    return jsonify({'success': False, 'error': result}), 500


@bp.route('/events/<int:event_id>/distribution/positive/unassign', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_unassign(event_id):
    data = request.get_json(silent=True) or {}
    santa_id = data.get('santa_id')
    try:
        santa_id = int(santa_id)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Некорректный идентификатор Деда Мороза'}), 400

    conn = get_db_connection()
    try:
        # Получаем информацию о назначении перед удалением
        assignment = conn.execute('''
            SELECT id, santa_user_id, recipient_user_id
            FROM event_assignments
            WHERE event_id = ? AND santa_user_id = ? AND is_archived = 0
        ''', (event_id, santa_id)).fetchone()
        
        if not assignment:
            conn.close()
            return jsonify({'success': False, 'error': 'Задание для выбранного участника не найдено'}), 404
        
        assignment_id = assignment['id']
        recipient_id = assignment['recipient_user_id']
        
        # Проверяем, есть ли сообщения в чате
        message_count = conn.execute('''
            SELECT COUNT(*) as cnt FROM letter_messages WHERE assignment_id = ?
        ''', (assignment_id,)).fetchone()
        
        has_messages = message_count and message_count['cnt'] > 0
        
        # Если есть сообщения, сохраняем чат в архив
        if has_messages:
            user_id = session.get('user_id')
            conn.execute('''
                INSERT INTO assignment_chat_history (
                    original_assignment_id, event_id, santa_user_id, recipient_user_id,
                    archived_at, archived_by
                )
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            ''', (assignment_id, event_id, santa_id, recipient_id, user_id))
            log_debug(f"Archived chat for assignment_id {assignment_id} (pair: {santa_id} -> {recipient_id}) with {message_count['cnt']} messages")
        
        # При расформировании снимаем замок и оставляем пару активной (не архивируем)
        # Это позволяет видеть расформированные пары на вкладке "unformed" и перетаскивать их
        # Также устанавливаем is_archived = 0, чтобы пара была видна при загрузке страницы
        conn.execute('''
            UPDATE event_assignments
            SET locked = 0, assignment_locked = 0, is_archived = 0
            WHERE id = ?
        ''', (assignment_id,))
        
        conn.commit()
        
        action_msg = 'Пара расформирована. Чат сохранён в архиве.' if has_messages else 'Пара расформирована. Пара снова доступна для редактирования.'
        
        log_activity(
            'assignment_removed',
            details=f'Задание отменено для мероприятия #{event_id} (Дед Мороз #{santa_id})',
            metadata={
                'event_id': event_id,
                'santa_user_id': santa_id,
                'recipient_user_id': recipient_id,
                'assignment_id': assignment_id,
                'chat_archived': has_messages,
                'messages_count': message_count['cnt'] if has_messages else 0
            }
        )
        return jsonify({'success': True, 'message': action_msg, 'chat_archived': has_messages})
    except Exception as e:
        conn.rollback()
        log_error(f"Error removing assignment for event {event_id}, santa {santa_id}: {e}")
        return jsonify({'success': False, 'error': 'Не удалось отменить задание'}), 500
    finally:
        conn.close()

@bp.route('/events/<int:event_id>/distribution/positive/random', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_generate(event_id):
    request_data = request.get_json(silent=True) or {}
    group_by_country = bool(request_data.get('group_by_country'))
    locked_pairs_raw = request_data.get('locked_pairs') or []
    assignment_locked_santas_raw = request_data.get('assignment_locked_santas') or []
    participant_ids_filter = request_data.get('participant_ids')  # Список ID участников для фильтрации
    unformed_only = bool(request_data.get('unformed_only', False))  # Флаг для формирования только из несформированных
    
    conn = get_db_connection()
    
    # Если нужно формировать только из несформированных участников, получаем список участников с парами
    excluded_user_ids = set()
    if unformed_only or participant_ids_filter:
        # Получаем участников, у которых уже есть пары
        existing_assignments = conn.execute('''
            SELECT DISTINCT santa_user_id, recipient_user_id
            FROM event_assignments
            WHERE event_id = ? AND is_archived = 0
        ''', (event_id,)).fetchall()
        
        for assignment in existing_assignments:
            excluded_user_ids.add(assignment['santa_user_id'])
            excluded_user_ids.add(assignment['recipient_user_id'])
    
    # Формируем запрос с учетом фильтров
    query = '''
        SELECT 
            er.user_id,
            u.username,
            COALESCE(d.country, u.country) AS country,
            COALESCE(d.city, u.city) AS city
        FROM event_registrations er
        LEFT JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
        LEFT JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
        WHERE er.event_id = ?
          AND epa.approved = 1
    '''
    params = [event_id]
    
    # Добавляем фильтры
    if participant_ids_filter:
        # Фильтруем по переданным ID
        placeholders = ','.join(['?'] * len(participant_ids_filter))
        query += f' AND er.user_id IN ({placeholders})'
        params.extend(participant_ids_filter)
    elif unformed_only:
        # Исключаем участников с парами
        if excluded_user_ids:
            placeholders = ','.join(['?'] * len(excluded_user_ids))
            query += f' AND er.user_id NOT IN ({placeholders})'
            params.extend(excluded_user_ids)
    
    query += ' ORDER BY u.username COLLATE NOCASE'
    
    participants = conn.execute(query, tuple(params)).fetchall()
    conn.close()

    if not participants or len(participants) < 2:
        return jsonify({'success': False, 'error': 'Недостаточно участников для распределения'}), 400

    participants_map = {
        row['user_id']: {
            'name': row['username'] or f'ID {row["user_id"]}',
            'country': row['country'],
            'city': row['city'],
        }
        for row in participants
    }

    user_ids = [row['user_id'] for row in participants]

    locked_assignments = {}
    locked_recipient_ids = set()
    try:
        for entry in locked_pairs_raw:
            santa_id_raw = entry.get('santa_id')
            recipient_id_raw = entry.get('recipient_id')
            santa_id = int(santa_id_raw)
            recipient_id = int(recipient_id_raw)
            if santa_id == recipient_id:
                return jsonify({'success': False, 'error': 'Закреплённая пара не может совпадать с самим собой'}), 400
            if santa_id not in participants_map or recipient_id not in participants_map:
                return jsonify({'success': False, 'error': 'Закреплённая пара содержит неизвестного участника'}), 400
            if group_by_country:
                santa_country = participants_map[santa_id].get('country')
                recipient_country = participants_map[recipient_id].get('country')
                if santa_country and recipient_country and santa_country != recipient_country:
                    return jsonify({'success': False, 'error': 'Закреплённая пара нарушает правило «По странам»'}), 400
            if santa_id in locked_assignments:
                return jsonify({'success': False, 'error': 'Каждый Дед Мороз может быть закреплён только один раз'}), 400
            if recipient_id in locked_recipient_ids:
                return jsonify({'success': False, 'error': 'Получатель уже закреплён в другой паре'}), 400
            locked_assignments[santa_id] = recipient_id
            locked_recipient_ids.add(recipient_id)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Некорректные данные закреплённых пар'}), 400

    assignment_locked_santas = set()
    try:
        assignment_locked_santas = {int(santa_id) for santa_id in assignment_locked_santas_raw}
    except (TypeError, ValueError):
        assignment_locked_santas = set()

    def is_valid_pair(santa_id, recipient_id, require_same_country: bool):
        if santa_id == recipient_id:
            return False
        if require_same_country:
            santa_country = participants_map.get(santa_id, {}).get('country')
            recipient_country = participants_map.get(recipient_id, {}).get('country')
            if santa_country and recipient_country and santa_country != recipient_country:
                return False
        return True

    used_santas = set(locked_assignments.keys())
    used_recipients = set(locked_assignments.values())

    remaining_candidate_santas = [sid for sid in user_ids if sid not in used_santas]
    random.shuffle(remaining_candidate_santas)

    recipients_by_country = defaultdict(list)
    for rid in user_ids:
        if rid in used_recipients:
            continue
        country = participants_map.get(rid, {}).get('country')
        recipients_by_country[country].append(rid)

    for country_list in recipients_by_country.values():
        random.shuffle(country_list)

    same_country_pairs = []
    if group_by_country:
        for santa_id in remaining_candidate_santas:
            if santa_id in used_santas:
                continue
            country = participants_map.get(santa_id, {}).get('country')
            candidates = recipients_by_country.get(country)
            if not candidates:
                continue
            recipient_id = None
            for idx, candidate in enumerate(candidates):
                if candidate != santa_id:
                    recipient_id = candidates.pop(idx)
                    break
            if recipient_id is None:
                continue
            same_country_pairs.append((santa_id, recipient_id))
            used_santas.add(santa_id)
            used_recipients.add(recipient_id)
            if not candidates:
                recipients_by_country.pop(country, None)

    remaining_santas = [sid for sid in user_ids if sid not in used_santas]
    available_recipients = [rid for rid in user_ids if rid not in used_recipients]

    if len(remaining_santas) != len(available_recipients):
        return jsonify({'success': False, 'error': 'Количество доступных Дедов Морозов и получателей не совпадает. Проверьте закреплённые пары.'}), 400

    def try_assignments(rem_santas, rem_recipients, require_same_country: bool, attempts: int = 3000):
        if len(rem_santas) != len(rem_recipients):
            return None
        rem_santas = rem_santas[:]
        rem_recipients = rem_recipients[:]
        for _ in range(attempts):
            random.shuffle(rem_santas)
            random.shuffle(rem_recipients)
            valid = True
            for santa_id, recipient_id in zip(rem_santas, rem_recipients):
                if not is_valid_pair(santa_id, recipient_id, require_same_country):
                    valid = False
                    break
            if valid:
                return list(zip(rem_santas, rem_recipients))
        return None

    extra_pairs = []
    if remaining_santas:
        if group_by_country:
            extra_pairs = try_assignments(remaining_santas, available_recipients, True)
            if not extra_pairs:
                extra_pairs = try_assignments(remaining_santas, available_recipients, False)
        else:
            extra_pairs = try_assignments(remaining_santas, available_recipients, False)

        if extra_pairs is None:
            error_message = 'Не удалось сформировать уникальные пары, попробуйте снова'
            if locked_assignments:
                error_message += ' Убедитесь, что закреплённые пары не блокируют распределение.'
            return jsonify({'success': False, 'error': error_message}), 500
    else:
        extra_pairs = []

    assignment_pairs = list(locked_assignments.items()) + same_country_pairs + extra_pairs

    # Проверяем, что все участники включены в распределение
    all_santa_ids = set(pair[0] for pair in assignment_pairs)
    all_recipient_ids = set(pair[1] for pair in assignment_pairs)
    all_participant_ids = set(user_ids)
    
    missing_santas = all_participant_ids - all_santa_ids
    missing_recipients = all_participant_ids - all_recipient_ids
    
    log_debug(f"admin_event_distribution_positive_generate: event_id={event_id}, total_participants={len(user_ids)}, "
              f"generated_pairs={len(assignment_pairs)}, locked={len(locked_assignments)}, "
              f"same_country={len(same_country_pairs)}, extra={len(extra_pairs)}")
    
    if missing_santas or missing_recipients:
        log_error(f"admin_event_distribution_positive_generate: Missing participants! "
                 f"Missing santas: {sorted(missing_santas)}, Missing recipients: {sorted(missing_recipients)}")
        return jsonify({
            'success': False, 
            'error': f'Не удалось создать распределение для всех участников. Отсутствует Дедов Морозов: {len(missing_santas)}, отсутствует получателей: {len(missing_recipients)}'
        }), 500

    if len(assignment_pairs) != len(user_ids):
        log_error(f"admin_event_distribution_positive_generate: Pair count mismatch! "
                 f"Expected: {len(user_ids)}, Got: {len(assignment_pairs)}")
        return jsonify({
            'success': False, 
            'error': f'Количество созданных пар ({len(assignment_pairs)}) не соответствует количеству участников ({len(user_ids)})'
        }), 500

    assignment_pairs.sort(key=lambda pair: participants_map[pair[0]]['name'] or '')

    # Загружаем существующие данные об отправке для сохранения при генерации
    # Учитываем как явные отметки (santa_sent_at), так и сообщения от Деда Мороза
    conn_existing = get_db_connection()
    existing_assignments = conn_existing.execute('''
        SELECT 
            ea.santa_user_id, 
            ea.recipient_user_id, 
            ea.santa_sent_at, 
            ea.santa_send_info, 
            ea.recipient_received_at,
            CASE 
                WHEN (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '') THEN 1
                WHEN EXISTS (
                    SELECT 1 FROM letter_messages lm 
                    WHERE lm.assignment_id = ea.id AND lm.sender = 'santa'
                ) THEN 1
                ELSE 0 
            END as has_sent_indicator
        FROM event_assignments ea
        WHERE ea.event_id = ?
          AND ea.is_archived = 0
    ''', (event_id,)).fetchall()
    conn_existing.close()
    
    existing_data_map = {}
    for row in existing_assignments:
        key = (row['santa_user_id'], row['recipient_user_id'])
        existing_data_map[key] = {
            'santa_sent_at': row['santa_sent_at'],
            'santa_send_info': row['santa_send_info'] if 'santa_send_info' in row.keys() else None,
            'recipient_received_at': row['recipient_received_at'],
            'has_sent_indicator': bool(row['has_sent_indicator']) if 'has_sent_indicator' in row.keys() else False
        }

    pairs = []
    for santa_id, recipient_id in assignment_pairs:
        santa_meta = participants_map.get(santa_id, {})
        recipient_meta = participants_map.get(recipient_id, {})
        
        # Проверяем, есть ли существующие данные об отправке для этой пары
        existing_key = (santa_id, recipient_id)
        existing_data = existing_data_map.get(existing_key, {})
        
        pairs.append({
            'santa_id': santa_id,
            'santa_name': santa_meta.get('name'),
            'santa_country': santa_meta.get('country'),
            'santa_city': santa_meta.get('city'),
            'recipient_id': recipient_id,
            'recipient_name': recipient_meta.get('name'),
            'recipient_country': recipient_meta.get('country'),
            'recipient_city': recipient_meta.get('city'),
            'santa_sent_at': existing_data.get('santa_sent_at') if (existing_data.get('santa_sent_at') and existing_data.get('santa_sent_at') != '') else None,
            'santa_send_info': existing_data.get('santa_send_info'),
            'recipient_received_at': existing_data.get('recipient_received_at'),
            'has_sent_indicator': existing_data.get('has_sent_indicator', False),
            'locked': santa_id in locked_assignments,
            'assignment_locked': santa_id in assignment_locked_santas
        })

    country_mode_applied = False
    if group_by_country:
        country_mode_applied = all(
            (participants_map.get(santa_id, {}).get('country') is None or
             participants_map.get(recipient_id, {}).get('country') is None or
             participants_map.get(santa_id, {}).get('country') == participants_map.get(recipient_id, {}).get('country'))
            for santa_id, recipient_id in assignment_pairs
        )

    log_debug(f"admin_event_distribution_positive_generate: Successfully generated {len(pairs)} pairs")
    return jsonify({'success': True, 'pairs': pairs, 'country_mode_applied': country_mode_applied})


@bp.route('/events/<int:event_id>/participants/add', methods=['POST'])
@require_role('admin')
def admin_event_participant_add(event_id):
    """Позволяет администратору добавить участника вручную"""
    identifier = request.form.get('user_identifier', '').strip()
    note = request.form.get('notes', '').strip()
    stage_choice = request.form.get('stage', 'main')

    if not identifier:
        flash('Укажите ID или имя пользователя', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        event = conn.execute('SELECT id, name FROM events WHERE id = ?', (event_id,)).fetchone()
        if not event:
            conn.close()
            flash('Мероприятие не найдено', 'error')
            return redirect(url_for('admin.admin_events'))

        user = None
        if identifier.isdigit():
            user = conn.execute('SELECT * FROM users WHERE user_id = ?', (int(identifier),)).fetchone()
        if not user:
            user = conn.execute('SELECT * FROM users WHERE LOWER(username) = ?', (identifier.lower(),)).fetchone()

        if not user:
            conn.close()
            flash('Пользователь не найден', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        existing = conn.execute('''
            SELECT 1 FROM event_registrations WHERE event_id = ? AND user_id = ?
        ''', (event_id, user['user_id'])).fetchone()
        if existing:
            conn.close()
            flash('Этот пользователь уже участвует в мероприятии', 'info')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        pre_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'pre_registration'
        ''', (event_id,)).fetchone()
        main_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'main_registration'
        ''', (event_id,)).fetchone()

        stage_choice = stage_choice if stage_choice in ('pre', 'main') else 'main'

        target_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if stage_choice == 'pre':
            if pre_stage and pre_stage['start_datetime']:
                target_datetime = pre_stage['start_datetime']
        else:
            if main_stage and main_stage['start_datetime']:
                target_datetime = main_stage['start_datetime']

        conn.execute('''
            INSERT INTO event_registrations (event_id, user_id)
            VALUES (?, ?)
        ''', (event_id, user['user_id']))

        profile = {key: (user[key] or '').strip() if isinstance(user[key], str) else user[key]
                   for key in user.keys()}

        conn.execute('''
            INSERT INTO event_registration_details (
                event_id, user_id, last_name, first_name, middle_name,
                postal_code, country, city, street, house, building, apartment,
                phone, telegram, whatsapp, viber
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                last_name = excluded.last_name,
                first_name = excluded.first_name,
                middle_name = excluded.middle_name,
                postal_code = excluded.postal_code,
                country = excluded.country,
                city = excluded.city,
                street = excluded.street,
                house = excluded.house,
                building = excluded.building,
                apartment = excluded.apartment,
                phone = excluded.phone,
                telegram = excluded.telegram,
                whatsapp = excluded.whatsapp,
                viber = excluded.viber,
                updated_at = CURRENT_TIMESTAMP
        ''', (
            event_id,
            user['user_id'],
            profile.get('last_name'),
            profile.get('first_name'),
            profile.get('middle_name'),
            profile.get('postal_code'),
            profile.get('country'),
            profile.get('city'),
            profile.get('street'),
            profile.get('house'),
            profile.get('building'),
            profile.get('apartment'),
            profile.get('phone'),
            profile.get('telegram'),
            profile.get('whatsapp'),
            profile.get('viber')
        ))

        conn.execute('''
            UPDATE event_registrations
            SET registered_at = ?
            WHERE event_id = ? AND user_id = ?
        ''', (target_datetime, event_id, user['user_id']))

        approval_note = note or 'Добавлен администратором вручную'
        conn.execute('''
            INSERT INTO event_participant_approvals (event_id, user_id, approved, approved_at, approved_by, notes)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                approved = 1,
                approved_at = CURRENT_TIMESTAMP,
                approved_by = excluded.approved_by,
                notes = excluded.notes
        ''', (event_id, user['user_id'], session.get('user_id'), approval_note))

        conn.commit()
        conn.close()

        log_activity(
            'admin_event_add_participant',
            details=f'Пользователь {user["username"]} (ID {user["user_id"]}) добавлен в мероприятие {event["name"]}',
            metadata={'event_id': event_id, 'target_user_id': user['user_id'], 'notes': approval_note}
        )
        flash('Участник успешно добавлен', 'success')
    except sqlite3.IntegrityError:
        conn.rollback()
        conn.close()
        flash('Не удалось добавить участника: данные противоречат существующим записям', 'error')
    except Exception as exc:
        conn.rollback()
        conn.close()
        log_error(f"Ошибка ручного добавления участника: {exc}")
        flash('Не удалось добавить участника', 'error')

    return redirect(url_for('admin.admin_event_participants', event_id=event_id))



@bp.route('/events/<int:event_id>/participants/upgrade', methods=['POST'])
@require_role('admin')
def admin_event_participant_upgrade(event_id):
    """Переводит участника из предварительной регистрации в основную"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        main_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'main_registration'
        ''', (event_id,)).fetchone()

        if not main_stage or not main_stage['start_datetime']:
            conn.close()
            flash('Этап основной регистрации не настроен', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        conn.execute('''
            UPDATE event_registrations
            SET registered_at = ?
            WHERE event_id = ? AND user_id = ?
        ''', (main_stage['start_datetime'], event_id, user_id))
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_upgrade_participant',
            details=f'Пользователь #{user_id} переведён в основную регистрацию мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id}
        )
        flash('Участник переведён в основную регистрацию', 'success')
    except Exception as exc:
        conn.rollback()
        conn.close()
        log_error(f"Ошибка перевода участника в основную регистрацию: {exc}")
        flash('Не удалось обновить участника', 'error')

    return redirect(url_for('admin.admin_event_participants', event_id=event_id))

@bp.route('/events/<int:event_id>/participants/downgrade', methods=['POST'])
@require_role('admin')
def admin_event_participant_downgrade(event_id):
    """Переводит участника из основной регистрации в предварительную"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        pre_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'pre_registration'
        ''', (event_id,)).fetchone()

        target_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if pre_stage and pre_stage['start_datetime']:
            target_datetime = pre_stage['start_datetime']

        conn.execute('''
            UPDATE event_registrations
            SET registered_at = ?
            WHERE event_id = ? AND user_id = ?
        ''', (target_datetime, event_id, user_id))
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_downgrade_participant',
            details=f'Пользователь #{user_id} переведён в предварительную регистрацию мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id}
        )
        flash('Участник переведён в предварительную регистрацию', 'success')
    except Exception as exc:
        conn.rollback()
        conn.close()
        log_error(f"Ошибка перевода участника в предварительную регистрацию: {exc}")
        flash('Не удалось обновить участника', 'error')

    return redirect(url_for('admin.admin_event_participants', event_id=event_id))

@bp.route('/events/<int:event_id>/participants/remove', methods=['POST'])
@require_role('admin')
def admin_event_participant_remove(event_id):
    """Удаление участника из мероприятия"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        conn.execute('BEGIN')

        conn.execute('DELETE FROM event_registrations WHERE event_id = ? AND user_id = ?', (event_id, user_id))
        conn.execute('DELETE FROM event_registration_details WHERE event_id = ? AND user_id = ?', (event_id, user_id))
        conn.execute('DELETE FROM event_participant_approvals WHERE event_id = ? AND user_id = ?', (event_id, user_id))
        conn.execute('DELETE FROM event_assignments WHERE event_id = ? AND (santa_user_id = ? OR recipient_user_id = ?)', (event_id, user_id, user_id))

        conn.commit()
        conn.close()

        log_activity(
            'admin_event_remove_participant',
            details=f'Пользователь #{user_id} удалён из мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id}
        )
        flash('Участник удалён из мероприятия', 'success')
    except Exception as exc:
        try:
            conn.rollback()
        finally:
            conn.close()
        log_error(f"Ошибка удаления участника из мероприятия: {exc}")
        flash('Не удалось удалить участника', 'error')

    return redirect(url_for('admin.admin_event_participants', event_id=event_id))



@bp.route('/events/<int:event_id>/participants/confirm', methods=['POST'])
@require_role('admin')
def admin_event_participant_confirm(event_id):
    """Подтверждение участия"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Некорректный участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id_int)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        stages = conn.execute('''
            SELECT stage_type, start_datetime
            FROM event_stages
            WHERE event_id = ?
        ''', (event_id,)).fetchall()

        def parse_dt(value):
            if not value:
                return None
            try:
                return datetime.fromisoformat(str(value))
            except ValueError:
                try:
                    return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return None

        pre_start = None
        main_start = None
        registration_closed_start = None
        for stage in stages:
            if stage['stage_type'] == 'pre_registration':
                pre_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'main_registration':
                main_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'registration_closed':
                registration_closed_start = parse_dt(stage['start_datetime'])

        registered_at_dt = parse_dt(registration['registered_at'])

        stage_label = 'main'
        if pre_start and main_start and registered_at_dt:
            if registered_at_dt >= pre_start and registered_at_dt < main_start:
                stage_label = 'pre'
        elif pre_start and registered_at_dt and not main_start:
            if registered_at_dt < pre_start:
                stage_label = 'pre'
        elif main_start and registered_at_dt:
            stage_label = 'pre' if registered_at_dt < main_start else 'main'

        if registration_closed_start and registered_at_dt and registered_at_dt >= registration_closed_start:
            stage_label = 'main'

        if stage_label != 'main' and main_start:
            conn.execute('''
                UPDATE event_registrations
                SET registered_at = ?
                WHERE event_id = ? AND user_id = ?
            ''', (main_start.strftime('%Y-%m-%d %H:%M:%S'), event_id, user_id_int))
            registered_at_dt = main_start
            stage_label = 'main'

        conn.execute('''
            INSERT INTO event_participant_approvals (event_id, user_id, approved, approved_at, approved_by, notes)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?, NULL)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                approved = 1,
                approved_at = CURRENT_TIMESTAMP,
                approved_by = excluded.approved_by,
                notes = NULL
        ''', (event_id, user_id_int, session.get('user_id')))
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_confirm_participant',
            details=f'Пользователь #{user_id_int} подтвержден для мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id_int}
        )
        flash('Участник подтвержден', 'success')
    except Exception as exc:
        try:
            conn.rollback()
        finally:
            conn.close()
        log_error(f"Ошибка подтверждения участника: {exc}")
        flash('Не удалось подтвердить участника', 'error')

    return redirect(url_for('admin.admin_event_participants', event_id=event_id))

@bp.route('/events/<int:event_id>/participants/reject', methods=['POST'])
@require_role('admin')
def admin_event_participant_reject(event_id):
    """Отказ в участии"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Некорректный участник', 'error')
        return redirect(url_for('admin.admin_event_participants', event_id=event_id))

    reason = request.form.get('reason', '').strip()

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id_int)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        stages = conn.execute('''
            SELECT stage_type, start_datetime
            FROM event_stages
            WHERE event_id = ?
        ''', (event_id,)).fetchall()

        def parse_dt(value):
            if not value:
                return None
            try:
                return datetime.fromisoformat(str(value))
            except ValueError:
                try:
                    return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return None

        pre_start = None
        main_start = None
        registration_closed_start = None
        for stage in stages:
            if stage['stage_type'] == 'pre_registration':
                pre_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'main_registration':
                main_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'registration_closed':
                registration_closed_start = parse_dt(stage['start_datetime'])

        registered_at_dt = parse_dt(registration['registered_at'])

        stage_label = 'main'
        if pre_start and main_start and registered_at_dt:
            if registered_at_dt < main_start:
                stage_label = 'pre'
        elif pre_start and registered_at_dt and not main_start:
            if registered_at_dt < pre_start:
                stage_label = 'pre'
        elif main_start and registered_at_dt:
            stage_label = 'pre' if registered_at_dt < main_start else 'main'

        if registration_closed_start and registered_at_dt and registered_at_dt >= registration_closed_start:
            stage_label = 'main'

        if stage_label != 'main':
            conn.close()
            flash('Отказ возможен только для основной регистрации', 'error')
            return redirect(url_for('admin.admin_event_participants', event_id=event_id))

        conn.execute('''
            INSERT INTO event_participant_approvals (event_id, user_id, approved, approved_at, approved_by, notes)
            VALUES (?, ?, 0, CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                approved = 0,
                approved_at = CURRENT_TIMESTAMP,
                approved_by = excluded.approved_by,
                notes = excluded.notes
        ''', (event_id, user_id_int, session.get('user_id'), reason or None))
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_reject_participant',
            details=f'Пользователь #{user_id_int} отклонен для мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id_int, 'reason': reason}
        )
        flash('Участнику отказано в участии', 'success')
    except Exception as exc:
        try:
            conn.rollback()
        finally:
            conn.close()
        log_error(f"Ошибка отклонения участника: {exc}")
        flash('Не удалось отказать участнику', 'error')

    return redirect(url_for('admin.admin_event_participants', event_id=event_id))


@bp.route('/events/<int:event_id>/distribution/positive/save', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_save(event_id):
    data = request.get_json(silent=True) or {}
    pairs = data.get('pairs')
    
    log_debug(f"admin_event_distribution_positive_save: event_id={event_id}, pairs type={type(pairs)}, pairs length={len(pairs) if pairs else 0}")
    
    if not pairs or not isinstance(pairs, list):
        log_error(f"admin_event_distribution_positive_save: Invalid pairs data. Type: {type(pairs)}, Value: {pairs}")
        return jsonify({'success': False, 'error': 'Некорректные данные распределения'}), 400

    enforce_country = bool(data.get('enforce_country'))

    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходима авторизация'}), 403

    conn = get_db_connection()
    # Используем ту же логику, что и в генерации: только зарегистрированные И утвержденные участники
    approved_rows = conn.execute('''
        SELECT 
            er.user_id, 
            COALESCE(d.country, u.country) AS country
        FROM event_registrations er
        LEFT JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
        JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
        WHERE er.event_id = ?
          AND epa.approved = 1
    ''', (event_id,)).fetchall()
    approved_ids = {row['user_id'] for row in approved_rows}
    country_lookup = {row['user_id']: row['country'] for row in approved_rows}
    conn.close()

    log_debug(f"admin_event_distribution_positive_save: approved_ids count={len(approved_ids)}, approved_ids={sorted(approved_ids)}")

    if len(approved_ids) < 2:
        log_error(f"admin_event_distribution_positive_save: Not enough approved participants. Count: {len(approved_ids)}")
        return jsonify({'success': False, 'error': 'Недостаточно утверждённых участников для сохранения распределения'}), 400

    assignments = []
    santas_seen = set()
    recipients_seen = set()

    try:
        for idx, entry in enumerate(pairs):
            if not isinstance(entry, dict):
                log_error(f"admin_event_distribution_positive_save: Entry {idx} is not a dict: {entry}")
                return jsonify({'success': False, 'error': f'Некорректный формат пары #{idx + 1}'}), 400
            
            santa_id_raw = entry.get('santa_id')
            recipient_id_raw = entry.get('recipient_id')
            
            if santa_id_raw is None or recipient_id_raw is None:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} missing IDs: santa_id={santa_id_raw}, recipient_id={recipient_id_raw}")
                return jsonify({'success': False, 'error': f'Пара #{idx + 1} содержит некорректные идентификаторы'}), 400
            
            try:
                santa_id = int(santa_id_raw)
                recipient_id = int(recipient_id_raw)
            except (TypeError, ValueError) as e:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} invalid IDs: santa_id={santa_id_raw}, recipient_id={recipient_id_raw}, error={e}")
                return jsonify({'success': False, 'error': f'Пара #{idx + 1} содержит некорректные идентификаторы'}), 400
            
            if santa_id == recipient_id:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} santa equals recipient: {santa_id}")
                return jsonify({'success': False, 'error': f'Участник {santa_id} не может быть назначен самому себе'}), 400
            
            if santa_id not in approved_ids:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} santa_id {santa_id} not in approved_ids. Approved: {sorted(approved_ids)}")
                return jsonify({'success': False, 'error': f'Пользователь {santa_id} не входит в список утверждённых участников'}), 400
            
            if recipient_id not in approved_ids:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} recipient_id {recipient_id} not in approved_ids. Approved: {sorted(approved_ids)}")
                return jsonify({'success': False, 'error': f'Получатель {recipient_id} не входит в список утверждённых участников'}), 400
            
            santa_country = country_lookup.get(santa_id)
            recipient_country = country_lookup.get(recipient_id)
            if enforce_country and santa_country and recipient_country and santa_country != recipient_country:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} country mismatch: santa={santa_country}, recipient={recipient_country}")
                return jsonify({'success': False, 'error': f'При распределении по странам Дед Мороз ({santa_country}) и Внучок ({recipient_country}) должны быть из одной страны'}), 400
            
            if santa_id in santas_seen:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} duplicate santa_id: {santa_id}")
                return jsonify({'success': False, 'error': f'Дед Мороз {santa_id} встречается более одного раза'}), 400
            
            if recipient_id in recipients_seen:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} duplicate recipient_id: {recipient_id}")
                return jsonify({'success': False, 'error': f'Получатель {recipient_id} встречается более одного раза'}), 400
            
            santas_seen.add(santa_id)
            recipients_seen.add(recipient_id)
            assignments.append((santa_id, recipient_id))
    except Exception as e:
        log_error(f"admin_event_distribution_positive_save: Unexpected error processing pairs: {e}")
        import traceback
        log_error(traceback.format_exc())
        return jsonify({'success': False, 'error': f'Ошибка обработки данных: {str(e)}'}), 400

    locked_pairs_raw = data.get('locked_pairs') or []
    locked_pairs_set = set()
    try:
        for entry in locked_pairs_raw:
            santa_id = int(entry.get('santa_id'))
            recipient_id = int(entry.get('recipient_id'))
            locked_pairs_set.add((santa_id, recipient_id))
    except (TypeError, ValueError, AttributeError):
        return jsonify({'success': False, 'error': 'Некорректные данные закреплённых пар'}), 400

    log_debug(f"admin_event_distribution_positive_save: assignments count={len(assignments)}, approved_ids count={len(approved_ids)}")
    log_debug(f"admin_event_distribution_positive_save: assignments santas={sorted(santas_seen)}, approved_ids={sorted(approved_ids)}")
    
    # Проверяем, что все участники из распределения есть в списке утвержденных
    all_santa_ids = set(pair[0] for pair in assignments)
    all_recipient_ids = set(pair[1] for pair in assignments)
    
    invalid_santas = all_santa_ids - approved_ids
    invalid_recipients = all_recipient_ids - approved_ids
    
    if invalid_santas or invalid_recipients:
        log_error(f"admin_event_distribution_positive_save: Invalid participants in distribution. Invalid santas: {sorted(invalid_santas)}, Invalid recipients: {sorted(invalid_recipients)}")
        return jsonify({
            'success': False, 
            'error': f'Распределение содержит участников, не входящих в список утверждённых. Некорректных Дедов Морозов: {len(invalid_santas)}, некорректных получателей: {len(invalid_recipients)}'
        }), 400
    
    # Проверяем, что количество пар соответствует количеству участников
    # Но не требуем строгого соответствия, так как некоторые участники могли быть удалены
    if len(assignments) != len(approved_ids):
        missing_santas = approved_ids - all_santa_ids
        missing_recipients = approved_ids - all_recipient_ids
        log_debug(f"admin_event_distribution_positive_save: Count mismatch (this is OK if participants were removed). "
                 f"Missing santas: {sorted(missing_santas)}, Missing recipients: {sorted(missing_recipients)}")
        # Это не ошибка - просто предупреждение в логах
        # Распределение может содержать меньше пар, если участники были удалены

    if locked_pairs_set:
        assignments_set = set(assignments)
        for santa_id, recipient_id in locked_pairs_set:
            if (santa_id, recipient_id) not in assignments_set:
                return jsonify({'success': False, 'error': 'Закреплённые пары должны соответствовать сохранённым значениям'}), 400

    success, result = save_event_assignments(
        event_id,
        assignments,
        user_id,
        locked_pairs=locked_pairs_set or None
    )
    if success:
        return jsonify({'success': True, 'message': f'Распределение сохранено ({result} пар).'})
    return jsonify({'success': False, 'error': result}), 500

@bp.route('/events/<int:event_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_event_edit(event_id):
    """Редактирование мероприятия"""
    conn = get_db_connection()
    event = conn.execute('SELECT * FROM events WHERE id = ?', (event_id,)).fetchone()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        conn.close()
        return redirect(url_for('admin.admin_events'))
    
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        award_id = request.form.get('award_id', '').strip()
        award_id = int(award_id) if award_id else None
        
        # Получаем настройки рейтинга
        rating_registration = request.form.get('rating_registration', '').strip()
        rating_registration = int(rating_registration) if rating_registration else None
        rating_gift_not_sent = request.form.get('rating_gift_not_sent', '').strip()
        rating_gift_not_sent = int(rating_gift_not_sent) if rating_gift_not_sent else None
        rating_gift_sent = request.form.get('rating_gift_sent', '').strip()
        rating_gift_sent = int(rating_gift_sent) if rating_gift_sent else None
        rating_order_coefficient = request.form.get('rating_order_coefficient', '').strip()
        rating_order_coefficient = float(rating_order_coefficient) if rating_order_coefficient else None
        
        if not name:
            flash('Название мероприятия обязательно', 'error')
            awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
            conn.close()
            return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
        
        try:
            # Получаем старые настройки рейтинга до обновления
            old_event = conn.execute('SELECT rating_registration, rating_gift_not_sent, rating_gift_sent, rating_order_coefficient FROM events WHERE id = ?', (event_id,)).fetchone()
            
            previous_end = None
            # Обновляем мероприятие
            conn.execute('''
                UPDATE events 
                SET name = ?, description = ?, updated_at = CURRENT_TIMESTAMP, award_id = ?,
                    rating_registration = ?, rating_gift_not_sent = ?, rating_gift_sent = ?, rating_order_coefficient = ?
                WHERE id = ?
            ''', (name, description, award_id, rating_registration, rating_gift_not_sent, rating_gift_sent, rating_order_coefficient, event_id))
            
            # Обновляем этапы
            for stage in EVENT_STAGES:
                start_datetime = None
                end_datetime = None
                
                if stage['has_start']:
                    start_str = request.form.get(f"stage_{stage['type']}_start", '').strip()
                    if start_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in start_str:
                                if len(start_str) == 16:  # YYYY-MM-DDTHH:MM
                                    start_datetime = datetime.strptime(start_str, '%Y-%m-%dT%H:%M')
                                elif len(start_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    start_datetime = datetime.strptime(start_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                start_datetime = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты начала этапа {stage['type']}: {e}, строка: {start_str}")
                            pass
                
                if stage['has_end']:
                    end_str = request.form.get(f"stage_{stage['type']}_end", '').strip()
                    if end_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in end_str:
                                if len(end_str) == 16:  # YYYY-MM-DDTHH:MM
                                    end_datetime = datetime.strptime(end_str, '%Y-%m-%dT%H:%M')
                                elif len(end_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    end_datetime = datetime.strptime(end_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                end_datetime = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты окончания этапа {stage['type']}: {e}, строка: {end_str}")
                            pass
                
                # Проверяем обязательность
                if stage['required'] and stage['has_start'] and not start_datetime:
                    flash(f'Дата начала этапа "{stage["name"]}" обязательна', 'error')
                    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
                
                # Проверяем последовательность дат
                if start_datetime and previous_end and start_datetime < previous_end:
                    flash(f'Дата начала этапа "{stage["name"]}" не может быть раньше окончания предыдущего этапа', 'error')
                    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
                
                # Обновляем или создаем этап
                if stage['type'] in stages_dict:
                    # Форматируем datetime для сохранения в БД
                    start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
                    end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None
                    
                    log_debug(f"Обновление этапа {stage['type']}: start={start_datetime_str}, end={end_datetime_str}")
                    
                    conn.execute('''
                        UPDATE event_stages 
                        SET start_datetime = ?, end_datetime = ?
                        WHERE event_id = ? AND stage_type = ?
                    ''', (start_datetime_str, end_datetime_str, event_id, stage['type']))
                else:
                    stage_order = len(stages_dict) + 1
                    # Форматируем datetime для сохранения в БД
                    start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
                    end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None
                    
                    log_debug(f"Создание этапа {stage['type']}: start={start_datetime_str}, end={end_datetime_str}")
                    
                    conn.execute('''
                        INSERT INTO event_stages 
                        (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (event_id, stage['type'], stage_order, start_datetime_str, end_datetime_str,
                          1 if stage['required'] else 0, 1 if not stage['required'] else 0))
                
                previous_end = end_datetime or previous_end
            
            # Если изменились настройки рейтинга, обновляем существующие события
            if old_event:
                # Обновляем события регистрации
                old_reg = old_event['rating_registration']
                if (old_reg is None) != (rating_registration is None) or (old_reg is not None and old_reg != rating_registration):
                    source = f'event:{event_id}:registration_bonus'
                    new_points = rating_registration if rating_registration is not None else get_rating_setting('rating_event_registration', 1)
                    conn.execute('''
                        UPDATE snowflake_events
                        SET points = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE source = ? AND active = 1
                    ''', (new_points, source))
                
                # Обновляем события неотправленного подарка
                old_not_sent = old_event['rating_gift_not_sent']
                if (old_not_sent is None) != (rating_gift_not_sent is None) or (old_not_sent is not None and old_not_sent != rating_gift_not_sent):
                    source = f'event:{event_id}:gift_not_sent'
                    new_points = rating_gift_not_sent if rating_gift_not_sent is not None else get_rating_setting('rating_event_gift_not_sent', 0)
                    if new_points != 0:
                        conn.execute('''
                            UPDATE snowflake_events
                            SET points = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE source = ? AND active = 1
                        ''', (new_points, source))
                
                # Обновляем события отправленного подарка
                old_sent = old_event['rating_gift_sent']
                if (old_sent is None) != (rating_gift_sent is None) or (old_sent is not None and old_sent != rating_gift_sent):
                    source = f'event:{event_id}:gift_sent'
                    new_points = rating_gift_sent if rating_gift_sent is not None else get_rating_setting('rating_event_gift_sent', 0)
                    if new_points != 0:
                        conn.execute('''
                            UPDATE snowflake_events
                            SET points = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE source = ? AND active = 1
                        ''', (new_points, source))
            
            conn.commit()
            flash('Мероприятие успешно обновлено', 'success')
            conn.close()
            return redirect(url_for('admin.admin_event_view', event_id=event_id))
        except Exception as e:
            log_error(f"Error updating event: {e}")
            flash(f'Ошибка обновления мероприятия: {str(e)}', 'error')
            conn.rollback()
            conn.close()
    
    # GET запрос - получаем список наград
    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
    conn.close()
    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)

@bp.route('/events/<int:event_id>/delete', methods=['POST'])
@require_role('admin')
def admin_event_delete(event_id):
    """Удаление мероприятия"""
    conn = get_db_connection()
    event = conn.execute('SELECT * FROM events WHERE id = ?', (event_id,)).fetchone()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        conn.close()
        return redirect(url_for('admin.admin_events'))
    
    try:
        conn.execute('DELETE FROM events WHERE id = ?', (event_id,))
        conn.commit()
        flash('Мероприятие успешно удалено', 'success')
    except Exception as e:
        log_error(f"Error deleting event: {e}")
        flash(f'Ошибка удаления мероприятия: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin.admin_events'))
