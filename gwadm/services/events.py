"""Event queries: registration status, participants, awards, gift statistics."""

import sqlite3
import traceback
from datetime import datetime

from flask import session

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.events_stages import get_current_event_stage, get_event_now


def is_event_finished(event_id):
    """Проверяет, закончилось ли мероприятие полностью."""
    conn = get_db_connection()
    stage_rows = conn.execute('''
        SELECT * FROM event_stages
        WHERE event_id = ?
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()

    stages = [dict(row) for row in stage_rows]

    if not stages:
        return False

    now = get_event_now()

    after_party_stage = None
    for stage in stages:
        if stage['stage_type'] == 'after_party':
            after_party_stage = stage
            break

    if after_party_stage and after_party_stage['end_datetime']:
        try:
            end_dt = datetime.strptime(after_party_stage['end_datetime'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                end_dt = datetime.strptime(after_party_stage['end_datetime'], '%Y-%m-%dT%H:%M')
            except ValueError:
                return False

        return now > end_dt

    return False


def distribute_event_awards(event_id, require_sent=False):
    """Выдает награды участникам мероприятия."""
    conn = get_db_connection()

    event = conn.execute('SELECT award_id FROM events WHERE id = ?', (event_id,)).fetchone()
    if not event or not event['award_id']:
        conn.close()
        return False

    award_id = event['award_id']

    if require_sent:
        participants = conn.execute('''
            SELECT DISTINCT santa_user_id AS user_id
            FROM event_assignments
            WHERE event_id = ?
              AND santa_user_id IS NOT NULL
              AND santa_sent_at IS NOT NULL
        ''', (event_id,)).fetchall()
    else:
        participants = conn.execute('''
            SELECT DISTINCT user_id FROM event_registrations WHERE event_id = ?
        ''', (event_id,)).fetchall()

    if not participants:
        conn.close()
        return False

    admin_user_id = session.get('user_id') or 1
    awarded_count = 0

    for participant in participants:
        user_id = participant['user_id']
        try:
            existing = conn.execute('''
                SELECT id FROM user_awards WHERE user_id = ? AND award_id = ?
            ''', (user_id, award_id)).fetchone()

            if not existing:
                conn.execute('''
                    INSERT INTO user_awards (user_id, award_id, assigned_by)
                    VALUES (?, ?, ?)
                ''', (user_id, award_id, admin_user_id))
                awarded_count += 1
        except sqlite3.IntegrityError:
            pass
        except Exception as e:
            log_error(f"Error awarding user {user_id} with award {award_id}: {e}")

    if awarded_count > 0:
        conn.commit()
        log_debug(f"Distributed {awarded_count} awards for event {event_id} (require_sent={require_sent})")

    conn.close()
    return awarded_count > 0


def get_event_gifts_statistics(event_id):
    """Получает статистику по подаркам для мероприятия."""
    conn = get_db_connection()
    try:
        total_result = conn.execute('''
            SELECT COUNT(*) as count
            FROM event_assignments
            WHERE event_id = ?
        ''', (event_id,)).fetchone()
        total_assignments = total_result['count'] if total_result else 0

        sent_not_received_result = conn.execute('''
            SELECT COUNT(DISTINCT ea.id) as count
            FROM event_assignments ea
            WHERE ea.event_id = ?
              AND (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '')
              AND (ea.recipient_received_at IS NULL OR ea.recipient_received_at = '')
        ''', (event_id,)).fetchone()
        sent_not_received = sent_not_received_result['count'] if sent_not_received_result else 0

        sent_and_received_result = conn.execute('''
            SELECT COUNT(DISTINCT ea.id) as count
            FROM event_assignments ea
            WHERE ea.event_id = ?
              AND (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '')
              AND ea.recipient_received_at IS NOT NULL
              AND ea.recipient_received_at != ''
        ''', (event_id,)).fetchone()
        sent_and_received = sent_and_received_result['count'] if sent_and_received_result else 0

        not_sent = total_assignments - sent_not_received - sent_and_received

        return {
            'total': total_assignments,
            'sent_not_received': sent_not_received,
            'sent_and_received': sent_and_received,
            'not_sent': not_sent,
        }
    except Exception as e:
        log_error(f"Error getting gifts statistics for event {event_id}: {e}")
        log_error(traceback.format_exc())
        return {
            'total': 0,
            'sent_not_received': 0,
            'sent_and_received': 0,
            'not_sent': 0,
        }
    finally:
        conn.close()


def is_registration_open(event_id):
    """Проверяет, открыта ли регистрация на мероприятие."""
    current_stage = get_current_event_stage(event_id)
    if not current_stage:
        return False

    stage_type = current_stage['info']['type']
    return stage_type in ['pre_registration', 'main_registration']


def get_event_registrations_paginated(event_id, page=1, per_page=20):
    """Получает список зарегистрированных пользователей на мероприятие с пагинацией."""
    per_page = min(max(per_page, 10), 100)

    conn = get_db_connection()

    total_count = conn.execute('''
        SELECT COUNT(*) as count
        FROM event_registrations
        WHERE event_id = ?
    ''', (event_id,)).fetchone()
    total_count = total_count['count'] if total_count else 0

    offset = (page - 1) * per_page

    registrations = conn.execute('''
        SELECT er.*, u.user_id, u.username, u.avatar_seed, u.avatar_style, u.level, u.synd
        FROM event_registrations er
        JOIN users u ON er.user_id = u.user_id
        WHERE er.event_id = ?
        ORDER BY er.registered_at ASC
        LIMIT ? OFFSET ?
    ''', (event_id, per_page, offset)).fetchall()
    conn.close()

    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1

    return {
        'registrations': registrations,
        'total_count': total_count,
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'has_prev': page > 1,
        'has_next': page < total_pages,
    }


def get_missing_required_fields(user_id):
    """Возвращает информацию о незаполненных обязательных полях."""
    conn = get_db_connection()
    try:
        user = conn.execute('''
            SELECT email, phone, telegram, whatsapp, viber,
                   last_name, first_name, middle_name,
                   postal_code, country, city, street, house, building, apartment,
                   bio
            FROM users
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        conn.close()

        if not user:
            return {
                'has_personal_data': False,
                'has_address': False,
                'has_contact': False,
                'missing_personal': ['last_name', 'first_name', 'middle_name'],
                'missing_address': ['postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment'],
                'missing_contacts': ['email', 'phone', 'telegram', 'whatsapp', 'viber'],
            }

        missing_personal = [f for f in ('last_name', 'first_name', 'middle_name') if not user[f]]
        missing_address = [f for f in ('postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment') if not user[f]]
        missing_contacts = [f for f in ('email', 'phone', 'telegram', 'whatsapp', 'viber') if not user[f]]

        return {
            'has_personal_data': len(missing_personal) == 0,
            'has_address': len(missing_address) == 0,
            'has_contact': bool(user['email'] or user['phone'] or user['telegram'] or user['whatsapp'] or user['viber']),
            'missing_personal': missing_personal,
            'missing_address': missing_address,
            'missing_contacts': missing_contacts,
        }
    except Exception as e:
        log_error(f"Ошибка получения незаполненных полей: {e}")
        conn.close()
        return {
            'has_personal_data': False,
            'has_address': False,
            'has_contact': False,
            'missing_personal': ['last_name', 'first_name', 'middle_name'],
            'missing_address': ['postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment'],
            'missing_contacts': ['email', 'phone', 'telegram', 'whatsapp', 'viber'],
        }
