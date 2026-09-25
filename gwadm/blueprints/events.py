"""Public events routes."""

import sqlite3
from datetime import datetime

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.services.events_stages import (
    EVENT_STAGES,
    get_current_event_stage,
    get_event_now,
    get_event_registrations_count,
    get_event_stages,
    parse_event_datetime,
)
from gwadm.services.events import (
    distribute_event_awards,
    get_event_gifts_statistics,
    get_event_registrations_paginated,
    get_missing_required_fields,
    is_event_finished,
    is_registration_open,
)
from gwadm.services.activity import log_activity
from gwadm.services.settings import get_setting

bp = Blueprint("events", __name__)

@bp.route('/events')
def events():
    """Публичная страница со списком всех мероприятий"""
    conn = get_db_connection()
    events_list = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.deleted_at IS NULL
        ORDER BY e.created_at DESC
    ''').fetchall()
    conn.close()
    
    event_ids = [event['id'] for event in events_list]
    user_id = session.get('user_id')
    user_registrations = {}
    if user_id and event_ids:
        placeholders = ','.join(['?'] * len(event_ids))
        conn = get_db_connection()
        rows = conn.execute(
            f'''
            SELECT event_id, registered_at
            FROM event_registrations
            WHERE user_id = ? AND event_id IN ({placeholders})
            ''',
            (user_id, *event_ids)
        ).fetchall()
        conn.close()
        for row in rows:
            user_registrations[row['event_id']] = row['registered_at']
    
    # Определяем текущий этап и ближайший будущий этап для каждого мероприятия
    events_with_stages_raw = []
    now = get_event_now()
    stage_info_map = {stage['type']: stage for stage in EVENT_STAGES}

    def parse_dt(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    for event in events_list:
        current_stage = get_current_event_stage(event['id'])
        display_stage_name = None
        next_stage = None
        if current_stage:
            display_stage_name = current_stage['info']['name']
            if current_stage['info']['type'] == 'registration_closed':
                lottery_stage = next((stage for stage in EVENT_STAGES if stage['type'] == 'lottery'), None)
                display_stage_name = lottery_stage['name'] if lottery_stage else 'Жеребьёвка'
        
        # Определяем следующий этап для таймера
        stages = get_event_stages(event['id'])
        for stage in stages:
            start_dt = parse_dt(stage['start_datetime'])
            if not start_dt or start_dt <= now:
                continue

            stage_info = stage_info_map.get(stage['stage_type'])
            stage_name = stage_info['name'] if stage_info else stage['stage_type']

            if (not next_stage) or start_dt < next_stage['start_dt']:
                next_stage = {
                    'name': stage_name,
                    'start_dt': start_dt,
                    'start_iso': start_dt.isoformat()
                }

        registered_at_str = user_registrations.get(event['id'])
        registered_at = parse_dt(registered_at_str) if registered_at_str else None
        is_registered = registered_at is not None
        pre_stage_start_dt = None
        main_stage_start_dt = None
        for stage in stages:
            stage_type = stage['stage_type']
            stage_start = parse_dt(stage['start_datetime'])
            if stage_type == 'pre_registration':
                pre_stage_start_dt = stage_start
            elif stage_type == 'main_registration':
                main_stage_start_dt = stage_start

        needs_confirmation = False
        if (
            is_registered
            and pre_stage_start_dt
            and main_stage_start_dt
            and registered_at
            and registered_at >= pre_stage_start_dt
            and registered_at < main_stage_start_dt
            and now >= main_stage_start_dt
        ):
            needs_confirmation = True

        if not is_registration_open(event['id']):
            needs_confirmation = False

        current_stage = get_current_event_stage(event['id'])
        value = {
            'event': event,
            'current_stage': current_stage,
            'display_stage_name': display_stage_name,
            'next_stage': next_stage,
            'is_registered': is_registered,
            'needs_confirmation': needs_confirmation,
            'registration_open': is_registration_open(event['id'])
        }

        # если текущего этапа нет и следующего будущего этапа тоже нет, значит все этапы завершены
        value['next_stage_is_past'] = False
        if not current_stage and not next_stage:
            # Проверяем, были ли когда-то этапы
            value['next_stage_is_past'] = True

        events_with_stages_raw.append(value)

    events_with_stages = events_with_stages_raw

    for item in events_with_stages:
        event = item['event']
        item['registrations_count'] = get_event_registrations_count(event['id'])

    # Название проекта
    project_name = get_setting('project_name', 'Анонимные Деды Морозы')
    
    # Получаем тексты модальных окон
    modal_texts = {}
    conn = get_db_connection()
    modal_settings = conn.execute('SELECT key, value FROM settings WHERE category = ?', ('modals',)).fetchall()
    conn.close()
    for setting in modal_settings:
        modal_texts[setting['key']] = setting['value']
    
    return render_template('events.html', events_with_stages=events_with_stages, modal_texts=modal_texts)

@bp.route('/events/<int:event_id>')
def event_view(event_id):
    """Просмотр мероприятия для пользователей"""
    conn = get_db_connection()
    event = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.id = ?
    ''', (event_id,)).fetchone()
    conn.close()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        return redirect(url_for('events.events'))
    
    user_id = session.get('user_id')
    current_stage = get_current_event_stage(event_id)
    registration_open = is_registration_open(event_id)

    registration_row = None
    if user_id:
        conn = get_db_connection()
        registration_row = conn.execute(
            '''
            SELECT registered_at
            FROM event_registrations
            WHERE event_id = ? AND user_id = ?
            ''',
            (event_id, user_id)
        ).fetchone()
        conn.close()

    is_registered = registration_row is not None
    registrations_count = get_event_registrations_count(event_id)
    
    # Параметры пагинации для участников
    participants_page = request.args.get('participants_page', 1, type=int)
    participants_per_page = 20  # По 20 участников на странице
    
    # Получаем участников с пагинацией
    registrations_data = get_event_registrations_paginated(event_id, participants_page, participants_per_page)
    registrations = registrations_data['registrations']
    
    is_admin = 'admin' in session.get('roles', []) if session.get('roles') else False
    
    award_needed = False
    if current_stage and current_stage['info']['type'] == 'after_party':
        award_needed = True
    elif is_event_finished(event_id):
        award_needed = True

    if award_needed:
        distribute_event_awards(event_id, require_sent=True)
    
    # Получаем все этапы мероприятия
    conn = get_db_connection()
    stage_rows = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()

    stages = [dict(row) for row in stage_rows]
    for stage in stages:
        if (
            stage.get('stage_type') == 'after_party'
            and not stage.get('start_datetime')
            and stage.get('end_datetime')
        ):
            stage['start_datetime'] = stage['end_datetime']
    
    # Определяем статус каждого этапа (past, current, future)
    now = get_event_now()
    current_stage_type = current_stage['info']['type'] if current_stage else None
    
    stages_with_info = []
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    
    next_stage_candidate = None
    main_stage_start_dt = None
    pre_stage_start_dt = None
    if 'main_registration' in stages_dict:
        main_stage_row = stages_dict['main_registration']
        main_keys = main_stage_row.keys()
        main_start_val = main_stage_row['start_datetime'] if 'start_datetime' in main_keys else None
        if main_start_val:
            try:
                main_stage_start_dt = datetime.fromisoformat(str(main_start_val))
            except ValueError:
                main_stage_start_dt = None
    if 'pre_registration' in stages_dict:
        pre_stage_row = stages_dict['pre_registration']
        pre_keys = pre_stage_row.keys()
        pre_start_val = pre_stage_row['start_datetime'] if 'start_datetime' in pre_keys else None
        if pre_start_val:
            try:
                pre_stage_start_dt = datetime.fromisoformat(str(pre_start_val))
            except ValueError:
                pre_stage_start_dt = None

    registration_dt = None
    if registration_row and registration_row['registered_at']:
        try:
            registration_dt = datetime.fromisoformat(str(registration_row['registered_at']))
        except ValueError:
            registration_dt = None

    needs_main_confirmation = False
    if (
        is_registered
        and main_stage_start_dt
        and pre_stage_start_dt
        and registration_dt
        and registration_dt >= pre_stage_start_dt
        and registration_dt < main_stage_start_dt
        and now >= main_stage_start_dt
    ):
        needs_main_confirmation = True

    for stage_info in EVENT_STAGES:
        stage_type = stage_info['type']
        stage_data = stages_dict.get(stage_type, None)
        
        # Определяем статус этапа
        stage_status = 'future'  # по умолчанию будущий
        start_dt = None
        end_dt = None
        if stage_data:
            stage_data = dict(stage_data)
            stage_keys = stage_data.keys()
            start_value = stage_data['start_datetime'] if 'start_datetime' in stage_keys else None
            end_value = stage_data['end_datetime'] if 'end_datetime' in stage_keys else None

            # Пропускаем необязательные этапы без даты начала
            if stage_info.get('has_start') and not stage_info.get('required') and not start_value:
                log_debug(f"get_current_event_stage: skipping optional stage {stage_type} without start date for event {event_id}")
                continue

            if start_value:
                try:
                    start_dt = datetime.fromisoformat(str(start_value))
                except ValueError:
                    start_dt = None

            if end_value:
                try:
                    end_dt = datetime.fromisoformat(str(end_value))
                except ValueError:
                    end_dt = None

            # Проверяем, является ли это текущим этапом
            if current_stage_type == stage_type:
                stage_status = 'current'
            else:
                if start_dt:
                    if now < start_dt:
                        stage_status = 'future'
                    else:
                        # Этап уже начался или завершился
                        if end_dt and now < end_dt:
                            stage_status = 'past'
                        else:
                            stage_status = 'past'
                else:
                    stage_status = 'future'
        
        stages_with_info.append({
            'info': stage_info,
            'data': stage_data,
            'status': stage_status
        })

        if start_dt and start_dt > now:
            if (not next_stage_candidate) or start_dt < next_stage_candidate['start_dt']:
                next_stage_candidate = {
                    'name': stage_info['name'],
                    'start_datetime': stage_data['start_datetime'],
                    'start_dt': start_dt,
                    'stage_type': stage_type
                }
    
    if current_stage and not next_stage_candidate:
        current_type = current_stage['info']['type']
        try:
            current_index = next(i for i, s in enumerate(EVENT_STAGES) if s['type'] == current_type)
        except StopIteration:
            current_index = None

        if current_index is not None:
            for idx in range(current_index + 1, len(EVENT_STAGES)):
                next_info = EVENT_STAGES[idx]
                next_data = stages_dict.get(next_info['type'])
                candidate_raw = None
                candidate_dt = None

                if next_data and next_data.get('start_datetime'):
                    candidate_raw = next_data['start_datetime']
                elif next_data and next_data.get('end_datetime'):
                    candidate_raw = next_data['end_datetime']
                elif next_info['type'] == 'after_party' and current_stage['data'] and current_stage['data'].get('end_datetime'):
                    candidate_raw = current_stage['data']['end_datetime']

                if candidate_raw:
                    candidate_dt = parse_event_datetime(str(candidate_raw))

                if candidate_dt and candidate_dt > now:
                    next_stage_candidate = {
                        'name': next_info['name'],
                        'start_datetime': candidate_raw,
                        'start_dt': candidate_dt,
                        'stage_type': next_info['type']
                    }
                    break

    # Получаем тексты модальных окон
    modal_texts = {}
    conn = get_db_connection()
    modal_settings = conn.execute('SELECT key, value FROM settings WHERE category = ?', ('modals',)).fetchall()
    conn.close()
    for setting in modal_settings:
        modal_texts[setting['key']] = setting['value']
    
    if not next_stage_candidate:
        for stage_info in EVENT_STAGES:
            data = stages_dict.get(stage_info['type'])
            if not data:
                continue

            candidate_raw = None
            candidate_dt = None

            if data.get('start_datetime'):
                try:
                    candidate_dt = datetime.fromisoformat(str(data['start_datetime']))
                    candidate_raw = data['start_datetime']
                except ValueError:
                    candidate_dt = None

            if (not candidate_dt or candidate_dt <= now) and data.get('end_datetime') and stage_info['type'] == 'after_party':
                try:
                    candidate_dt = datetime.fromisoformat(str(data['end_datetime']))
                    candidate_raw = data['end_datetime']
                except ValueError:
                    candidate_dt = None

            if candidate_dt and candidate_dt > now:
                next_stage_candidate = {
                    'name': stage_info['name'],
                    'start_datetime': candidate_raw,
                    'start_dt': candidate_dt,
                    'stage_type': stage_info['type']
                }
                break

    next_stage_payload = None
    if next_stage_candidate:
        start_dt_local = next_stage_candidate.get('start_dt')
        if isinstance(start_dt_local, datetime):
            start_iso = start_dt_local.strftime('%Y-%m-%dT%H:%M:%S')
            next_stage_payload = {
                'name': next_stage_candidate.get('name'),
                'start_datetime': next_stage_candidate.get('start_datetime'),
                'start_iso': start_iso,
                'stage_type': next_stage_candidate.get('stage_type')
            }

    # Получаем статистику по подаркам (только после закрытия регистрации)
    gifts_stats = None
    show_gifts_stats = False
    if current_stage:
        stage_type = current_stage['info']['type']
        # Показываем статистику после закрытия регистрации
        if stage_type in ['registration_closed', 'lottery', 'celebration_date', 'after_party']:
            show_gifts_stats = True
            gifts_stats = get_event_gifts_statistics(event_id)
    
    return render_template('event_view.html', 
                         event=event,
                         current_stage=current_stage,
                         modal_texts=modal_texts,
                         registration_open=registration_open,
                         is_registered=is_registered,
                         needs_main_confirmation=needs_main_confirmation,
                         registrations_count=registrations_count,
                         registrations=registrations,
                         stages_with_info=stages_with_info,
                         is_admin=is_admin,
                         next_stage=next_stage_payload,
                         show_gifts_stats=show_gifts_stats,
                         gifts_stats=gifts_stats,
                         participants_page=registrations_data['page'],
                         participants_per_page=registrations_data['per_page'],
                         participants_total_count=registrations_data['total_count'],
                         participants_total_pages=registrations_data['total_pages'],
                         participants_has_prev=registrations_data['has_prev'],
                         participants_has_next=registrations_data['has_next'])


@bp.route('/events/<int:event_id>/register', methods=['POST'])
@require_login
def event_register(event_id):
    """Регистрация пользователя на мероприятие"""
    user_id = session.get('user_id')
    # Проверяем, является ли запрос AJAX/JSON запросом
    is_json_request = (
        request.headers.get('Content-Type') == 'application/json'
        or request.headers.get('Accept') == 'application/json'
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or request.is_json
    )
    payload = {}
    if is_json_request:
        payload = request.get_json(silent=True) or {}
    start_flow = bool(payload.get('start_registration_flow'))
    final_registration = bool(payload.get('final_registration'))

    if not user_id:
        if is_json_request:
            return jsonify({'success': False, 'error': 'Необходимо авторизоваться'}), 401
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('auth.login'))

    # Получаем информацию о регистрации пользователя
    registration_row = None
    main_stage_row = None
    pre_stage_row = None
    if user_id:
        conn = get_db_connection()
        registration_row = conn.execute(
            '''
            SELECT registered_at
            FROM event_registrations
            WHERE event_id = ? AND user_id = ?
            ''',
            (event_id, user_id)
        ).fetchone()
        main_stage_row = conn.execute(
            '''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'main_registration'
            ''',
            (event_id,)
        ).fetchone()
        pre_stage_row = conn.execute(
            '''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'pre_registration'
            ''',
            (event_id,)
        ).fetchone()
        conn.close()

    is_registered = registration_row is not None

    needs_confirmation = False
    registration_dt = None
    if registration_row and registration_row['registered_at']:
        try:
            registration_dt = datetime.fromisoformat(str(registration_row['registered_at']))
        except ValueError:
            registration_dt = None

    main_stage_start_dt = None
    pre_stage_start_dt = None
    if main_stage_row and main_stage_row['start_datetime']:
        try:
            main_stage_start_dt = datetime.fromisoformat(str(main_stage_row['start_datetime']))
        except ValueError:
            main_stage_start_dt = None
    if pre_stage_row and pre_stage_row['start_datetime']:
        try:
            pre_stage_start_dt = datetime.fromisoformat(str(pre_stage_row['start_datetime']))
        except ValueError:
            pre_stage_start_dt = None

    if (
        is_registered
        and main_stage_start_dt
        and pre_stage_start_dt
        and registration_dt
        and registration_dt >= pre_stage_start_dt
        and registration_dt < main_stage_start_dt
        and datetime.now() >= main_stage_start_dt
    ):
        needs_confirmation = True

    # Проверяем, открыта ли регистрация (или доступно подтверждение)
    if not is_registration_open(event_id):
        if is_json_request:
            return jsonify({'success': False, 'error': 'Регистрация на это мероприятие закрыта'}), 400
        flash('Регистрация на это мероприятие закрыта', 'error')
        return redirect(url_for('events.event_view', event_id=event_id))

    # Запрос на начало модального сценария
    if start_flow:
        if is_registered and not needs_confirmation:
            return jsonify({'success': False, 'error': 'Вы уже зарегистрированы на это мероприятие'}), 400
        missing_fields = get_missing_required_fields(user_id)
        return jsonify({
            'success': True,
            'missing_fields': missing_fields
        }), 200

    def complete_registration():
        """Проводит регистрацию и сохраняет слепок данных пользователя."""
        conn = get_db_connection()
        try:
            profile_row = conn.execute('''
                SELECT last_name, first_name, middle_name,
                       postal_code, country, city, street, house, building, apartment,
                       email, phone, telegram, whatsapp, viber, bio
                FROM users
                WHERE user_id = ?
            ''', (user_id,)).fetchone()

            if not profile_row:
                return {'status': 'error', 'message': 'Пользователь не найден'}

            profile = {key: (profile_row[key] or '').strip() for key in profile_row.keys()}
            required_fields = [
                ('last_name', 'Фамилия'),
                ('first_name', 'Имя'),
                ('middle_name', 'Отчество'),
                ('postal_code', 'Индекс'),
                ('country', 'Страна'),
                ('city', 'Город'),
                ('street', 'Улица'),
                ('house', 'Дом'),
                ('building', 'Корпус/строение'),
                ('apartment', 'Квартира'),
                ('phone', 'Номер телефона')
            ]
            missing_required = [label for field, label in required_fields if not profile.get(field)]
            if missing_required:
                return {
                    'status': 'missing',
                    'missing': missing_required
                }

            cursor = conn.execute('''
                INSERT OR IGNORE INTO event_registrations (event_id, user_id)
                VALUES (?, ?)
            ''', (event_id, user_id))
            already_registered = cursor.rowcount == 0

            conn.execute('''
                INSERT INTO event_registration_details (
                    event_id, user_id, last_name, first_name, middle_name,
                    postal_code, country, city, street, house, building, apartment,
                    email, phone, telegram, whatsapp, viber, bio
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    email = excluded.email,
                    phone = excluded.phone,
                    telegram = excluded.telegram,
                    whatsapp = excluded.whatsapp,
                    viber = excluded.viber,
                    bio = excluded.bio,
                    updated_at = CURRENT_TIMESTAMP
            ''', (
                event_id,
                user_id,
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
                profile.get('email'),
                profile.get('phone'),
                profile.get('telegram'),
                profile.get('whatsapp'),
                profile.get('viber'),
                profile.get('bio')
            ))

            if already_registered and needs_confirmation:
                conn.execute('''
                    UPDATE event_registrations
                    SET registered_at = CURRENT_TIMESTAMP
                    WHERE event_id = ? AND user_id = ?
                ''', (event_id, user_id))

            conn.commit()
            return {
                'status': 'success',
                'already_registered': already_registered,
                'reconfirmed': already_registered and needs_confirmation
            }
        except Exception as exc:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            log_error(f"Ошибка регистрации на мероприятие #{event_id}: {exc}")
            return {'status': 'error', 'message': 'Ошибка при регистрации'}
        finally:
            conn.close()

    # Финальный запрос из модального окна
    if final_registration:
        result = complete_registration()
        if result['status'] == 'success':
            if result.get('reconfirmed'):
                log_activity(
                    'event_confirm',
                    details=f'Подтверждение участия в мероприятии #{event_id}',
                    metadata={'event_id': event_id}
                )
                message = 'Ваше участие подтверждено!'
            elif not result.get('already_registered'):
                log_activity(
                    'event_register',
                    details=f'Регистрация на мероприятие #{event_id}',
                    metadata={'event_id': event_id}
                )
                message = 'Вы успешно зарегистрированы на мероприятие!'
            else:
                message = 'Вы уже зарегистрированы на это мероприятие'
            return jsonify({
                'success': True,
                'message': message,
                'already_registered': result.get('already_registered', False),
                'reconfirmed': result.get('reconfirmed', False)
            }), 200

        if result['status'] == 'missing':
            return jsonify({
                'success': False,
                'error': 'Пожалуйста, заполните все обязательные поля',
                'missing': result.get('missing', [])
            }), 400

        return jsonify({'success': False, 'error': result.get('message', 'Ошибка при регистрации')}), 500

    # Если это JSON-запрос без уточнения, возвращаем ошибку
    if is_json_request:
        return jsonify({'success': False, 'error': 'Некорректный запрос'}), 400

    # Обычный POST-запрос (без модальных окон) — пытаемся завершить регистрацию
    if is_registered and not needs_confirmation:
        flash('Вы уже зарегистрированы на это мероприятие', 'info')
        return redirect(url_for('events.event_view', event_id=event_id))

    result = complete_registration()
    if result['status'] == 'success':
        if result.get('reconfirmed'):
            log_activity(
                'event_confirm',
                details=f'Подтверждение участия в мероприятии #{event_id}',
                metadata={'event_id': event_id}
            )
            flash('Ваше участие подтверждено!', 'success')
        elif not result.get('already_registered'):
            log_activity(
                'event_register',
                details=f'Регистрация на мероприятие #{event_id}',
                metadata={'event_id': event_id}
            )
            flash('Вы успешно зарегистрированы на мероприятие!', 'success')
        else:
            flash('Вы уже зарегистрированы на это мероприятие', 'info')
    elif result['status'] == 'missing':
        missing_list = result.get('missing', [])
        flash(
            'Для регистрации необходимо заполнить обязательные поля: ' + ', '.join(missing_list),
            'error'
        )
    else:
        flash('Ошибка при регистрации', 'error')

    return redirect(url_for('events.event_view', event_id=event_id))

@bp.route('/events/<int:event_id>/unregister', methods=['POST'])
@require_login
def event_unregister(event_id):
    """Отмена регистрации пользователя на мероприятие"""
    user_id = session.get('user_id')
    if not user_id:
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('auth.login'))
    
    # Проверяем, открыта ли регистрация (можно отменить только если регистрация открыта)
    if not is_registration_open(event_id):
        flash('Регистрация закрыта, нельзя отменить участие', 'error')
        return redirect(url_for('events.event_view', event_id=event_id))
    
    conn = get_db_connection()
    try:
        cursor = conn.execute('''
            DELETE FROM event_registrations 
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id))
        conn.commit()
        
        if cursor.rowcount > 0:
            log_activity(
                'event_unregister',
                details=f'Отмена регистрации на мероприятие #{event_id}',
                metadata={'event_id': event_id}
            )
            flash('Регистрация отменена', 'success')
        else:
            flash('Вы не были зарегистрированы на это мероприятие', 'info')
    except Exception as e:
        log_error(f"Ошибка отмены регистрации: {e}")
        flash('Ошибка при отмене регистрации', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('events.event_view', event_id=event_id))
