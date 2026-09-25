"""Admin dashboard metrics tests."""

import os
from datetime import datetime, timedelta

import gwadm.db as gwadm_db
from gwadm.db import get_db_connection
from gwadm.services.admin_dashboard import get_admin_dashboard_stats
from gwadm.services.events import get_last_finished_event


def _reset_db(db_path):
    import gwadm.config as config

    os.environ['DATABASE_PATH'] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None
    gwadm_db.ensure_db()


def test_never_participated_count(tmp_path):
    db_path = str(tmp_path / 'admin_dash.db')
    _reset_db(db_path)
    conn = get_db_connection()
    conn.execute('INSERT INTO users (user_id, username) VALUES (1, "a"), (2, "b")')
    conn.execute('INSERT INTO event_registrations (event_id, user_id) VALUES (1, 1)')
    conn.commit()
    stats = get_admin_dashboard_stats(conn)
    conn.close()
    assert stats['total_users'] == 2
    assert stats['never_participated'] == 1


def test_gift_conversion_aggregate(tmp_path):
    db_path = str(tmp_path / 'gifts.db')
    _reset_db(db_path)
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO event_assignments (event_id, santa_user_id, recipient_user_id, santa_sent_at, recipient_received_at) '
        'VALUES (1, 1, 2, "2025-01-01", "2025-01-02")'
    )
    conn.execute(
        'INSERT INTO event_assignments (event_id, santa_user_id, recipient_user_id, santa_sent_at, recipient_received_at) '
        'VALUES (1, 2, 3, NULL, NULL)'
    )
    conn.commit()
    stats = get_admin_dashboard_stats(conn)
    conn.close()
    assert stats['gifts_all_time']['total'] == 2
    assert stats['gifts_all_time']['sent_and_received'] == 1
    assert stats['gifts_all_time']['conversion_pct'] == 50.0


def test_get_last_finished_event_picks_latest(tmp_path):
    db_path = str(tmp_path / 'finished.db')
    _reset_db(db_path)
    conn = get_db_connection()
    past = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    recent = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute('INSERT INTO events (id, name) VALUES (1, "Old"), (2, "Recent")')
    for eid, end in ((1, past), (2, recent)):
        conn.execute(
            'INSERT INTO event_stages (event_id, stage_type, stage_order, end_datetime) VALUES (?, "after_party", 99, ?)',
            (eid, end),
        )
    conn.commit()
    result = get_last_finished_event(conn)
    conn.close()
    assert result is not None
    assert result['id'] == 2
    assert result['name'] == 'Recent'
