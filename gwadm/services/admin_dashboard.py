"""Admin dashboard metrics."""

from datetime import datetime

from gwadm.db import get_db_connection
from gwadm.services.events import get_event_gifts_statistics, get_last_finished_event
from gwadm.services.events_stages import create_participant_approvals_for_event, get_current_event_stage
from gwadm.services.presence import count_online_users
from gwadm.services.rating import rating_cache_is_populated


def _gift_stats_from_row(row):
    total = int(row['total'] or 0)
    sent_and_received = int(row['sent_and_received'] or 0)
    sent_not_received = int(row['sent_not_received'] or 0)
    not_sent = max(0, total - sent_and_received - sent_not_received)
    conversion = round((sent_and_received / total) * 100, 1) if total else None
    send_rate = round(((sent_and_received + sent_not_received) / total) * 100, 1) if total else None
    return {
        'total': total,
        'sent_and_received': sent_and_received,
        'sent_not_received': sent_not_received,
        'not_sent': not_sent,
        'conversion_pct': conversion,
        'send_rate_pct': send_rate,
    }


def _aggregate_gift_stats(conn, event_id=None):
    if event_id is not None:
        return _gift_stats_from_row(get_event_gifts_statistics(event_id))

    row = conn.execute(
        '''
        SELECT
            COUNT(*) AS total,
            SUM(CASE
                WHEN santa_sent_at IS NOT NULL AND santa_sent_at != ''
                 AND recipient_received_at IS NOT NULL AND recipient_received_at != ''
                THEN 1 ELSE 0 END) AS sent_and_received,
            SUM(CASE
                WHEN santa_sent_at IS NOT NULL AND santa_sent_at != ''
                 AND (recipient_received_at IS NULL OR recipient_received_at = '')
                THEN 1 ELSE 0 END) AS sent_not_received
        FROM event_assignments
        '''
    ).fetchone()
    return _gift_stats_from_row(row)


def _count_events_review_pending(conn) -> int:
    total = 0
    events = conn.execute('SELECT id FROM events WHERE deleted_at IS NULL').fetchall()
    for event in events:
        event_id = event['id']
        current_stage = get_current_event_stage(event_id)
        if not current_stage or current_stage.get('info', {}).get('type') != 'registration_closed':
            continue
        create_participant_approvals_for_event(event_id)
        row = conn.execute(
            '''
            SELECT SUM(CASE WHEN approved IS NULL OR approved = 0 THEN 1 ELSE 0 END) AS pending
            FROM event_participant_approvals
            WHERE event_id = ?
            ''',
            (event_id,),
        ).fetchone()
        total += int(row['pending'] or 0) if row else 0
    return total


def _rating_cache_updated_at(conn):
    if not rating_cache_is_populated(conn):
        return None
    row = conn.execute('SELECT MAX(updated_at) AS updated_at FROM user_rating_cache').fetchone()
    return row['updated_at'] if row else None


def get_admin_dashboard_stats(conn=None):
    """Collect all metrics for /admin/ dashboard."""
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()

    total_users = conn.execute('SELECT COUNT(*) AS c FROM users').fetchone()['c']
    rating_positive = conn.execute(
        'SELECT COUNT(*) AS c FROM user_rating_cache WHERE total_points > 0'
    ).fetchone()['c']
    rating_negative = conn.execute(
        'SELECT COUNT(*) AS c FROM user_rating_cache WHERE total_points < 0'
    ).fetchone()['c']
    never_participated = conn.execute(
        '''
        SELECT COUNT(*) AS c FROM users u
        WHERE u.user_id NOT IN (SELECT DISTINCT user_id FROM event_registrations)
        '''
    ).fetchone()['c']

    broadcast_row = conn.execute(
        '''
        SELECT COUNT(*) AS c FROM broadcast_queue
        WHERE status IN ('pending', 'processing')
        '''
    ).fetchone()
    broadcast_pending = int(broadcast_row['c'] or 0) if broadcast_row else 0

    last_event = get_last_finished_event(conn)
    gifts_all_time = _aggregate_gift_stats(conn)
    gifts_last_event = None
    if last_event:
        gifts_last_event = _aggregate_gift_stats(conn, last_event['id'])

    stats = {
        'total_users': int(total_users or 0),
        'rating_positive': int(rating_positive or 0),
        'rating_negative': int(rating_negative or 0),
        'never_participated': int(never_participated or 0),
        'online_users': count_online_users(conn),
        'broadcast_pending': broadcast_pending,
        'events_review_pending': _count_events_review_pending(conn),
        'rating_cache_updated_at': _rating_cache_updated_at(conn),
        'rating_cache_populated': rating_cache_is_populated(conn),
        'last_finished_event': last_event,
        'gifts_all_time': gifts_all_time,
        'gifts_last_event': gifts_last_event,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

    if own_conn:
        conn.close()
    return stats
