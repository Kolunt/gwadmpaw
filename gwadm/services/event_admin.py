"""Admin event participant review."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
def get_participants_for_review(event_id):
    """Получает список участников для ревью с полной информацией"""
    conn = get_db_connection()
    participants = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.last_name,
            u.first_name,
            u.middle_name,
            u.postal_code,
            u.country,
            u.city,
            u.street,
            u.house,
            u.building,
            u.apartment,
            u.email,
            u.phone,
            u.telegram,
            u.whatsapp,
            u.viber,
            epa.approved,
            epa.approved_at,
            epa.notes,
            epa.approved_by,
            er.registered_at
        FROM event_registrations er
        JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_participant_approvals epa ON er.event_id = epa.event_id AND er.user_id = epa.user_id
        WHERE er.event_id = ?
        ORDER BY er.registered_at ASC
    ''', (event_id,)).fetchall()
    conn.close()
    return participants


def approve_participant(event_id, user_id, approved_by, approved=True, notes=None):
    """Утверждает или отклоняет участника"""
    conn = get_db_connection()
    try:
        if approved:
            conn.execute('''
                UPDATE event_participant_approvals 
                SET approved = 1, approved_at = CURRENT_TIMESTAMP, approved_by = ?, notes = ?
                WHERE event_id = ? AND user_id = ?
            ''', (approved_by, notes, event_id, user_id))
        else:
            conn.execute('''
                UPDATE event_participant_approvals 
                SET approved = 0, approved_at = NULL, approved_by = ?, notes = ?
                WHERE event_id = ? AND user_id = ?
            ''', (approved_by, notes, event_id, user_id))
        conn.commit()
        return True
    except Exception as e:
        log_error(f"Error approving participant: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def get_approved_participants(event_id):
    """Получает список утвержденных участников"""
    conn = get_db_connection()
    participants = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.last_name,
            u.first_name,
            u.middle_name
        FROM event_participant_approvals epa
        JOIN users u ON epa.user_id = u.user_id
        WHERE epa.event_id = ? AND epa.approved = 1
        ORDER BY epa.approved_at ASC
    ''', (event_id,)).fetchall()
    conn.close()
    return [dict(row) for row in participants]

def get_events_requiring_review():
    """Получает список мероприятий, требующих модерации участников"""
    conn = get_db_connection()
    now = datetime.now()
    
    # Получаем мероприятия, где регистрация закрыта, но есть неутвержденные участники
    events = conn.execute('''
        SELECT DISTINCT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        INNER JOIN event_stages es ON e.id = es.event_id
        INNER JOIN event_registrations er ON e.id = er.event_id
        LEFT JOIN event_participant_approvals epa ON e.id = epa.event_id AND er.user_id = epa.user_id
        WHERE es.stage_type = 'registration_closed'
        AND es.start_datetime IS NOT NULL
        AND datetime(es.start_datetime) <= datetime(?)
        AND (epa.approved IS NULL OR epa.approved = 0)
        ORDER BY es.start_datetime DESC
    ''', (now,)).fetchall()
    
    conn.close()
    return events
