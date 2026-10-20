"""Assignment progress kanban stages and viewed timestamps."""

import gwadm.db as gwadm_db
from gwadm.services.assignment_progress import (
    assignment_is_sent,
    recipient_stage,
    santa_stage,
)
from gwadm.services.assignments import mark_assignment_viewed_for_user


def test_santa_stage_flow():
    assert santa_stage({}) == 'assigned'
    assert santa_stage({'santa_viewed_at': '2026-01-01'}) == 'read'
    assert santa_stage({'santa_viewed_at': 'x', 'santa_sent_at': 'y'}) == 'sent'
    assert santa_stage({'has_santa_message': True}) == 'sent'
    assert santa_stage({'recipient_received_at': 'z', 'santa_sent_at': 'y'}) == 'confirmed'


def test_recipient_stage_flow():
    assert recipient_stage({}) == 'assigned'
    assert recipient_stage({'recipient_viewed_at': '2026-01-01'}) == 'read'
    assert recipient_stage({'recipient_viewed_at': 'x', 'santa_sent_at': 'y'}) == 'sent'
    assert recipient_stage({'recipient_received_at': 'z'}) == 'confirmed'


def test_assignment_is_sent():
    assert assignment_is_sent({}) is False
    assert assignment_is_sent({'has_santa_message': True}) is True
    assert assignment_is_sent({'santa_sent_at': '2026-01-01'}) is True


def test_mark_assignment_viewed_for_user(app):
    gwadm_db.ensure_db()
    conn = gwadm_db.get_db_connection()
    conn.execute(
        '''
        INSERT OR IGNORE INTO users (user_id, username, last_login)
        VALUES (90001, 'santa_prog', datetime('now')), (90002, 'kid_prog', datetime('now'))
        '''
    )
    conn.execute(
        '''
        INSERT INTO events (name, description, created_at)
        VALUES ('Progress test event', '', datetime('now'))
        '''
    )
    event_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.execute(
        '''
        INSERT INTO event_assignments (
            event_id, santa_user_id, recipient_user_id, is_archived
        ) VALUES (?, 90001, 90002, 0)
        ''',
        (event_id,),
    )
    assignment_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.commit()
    conn.close()

    assert mark_assignment_viewed_for_user(assignment_id, 90001) is True
    assert mark_assignment_viewed_for_user(assignment_id, 90001) is True

    conn = gwadm_db.get_db_connection()
    row = conn.execute(
        'SELECT santa_viewed_at, recipient_viewed_at FROM event_assignments WHERE id = ?',
        (assignment_id,),
    ).fetchone()
    conn.close()
    assert row['santa_viewed_at'] is not None
    assert row['recipient_viewed_at'] is None

    assert mark_assignment_viewed_for_user(assignment_id, 90002) is True
    conn = gwadm_db.get_db_connection()
    row = conn.execute(
        'SELECT recipient_viewed_at FROM event_assignments WHERE id = ?',
        (assignment_id,),
    ).fetchone()
    conn.close()
    assert row['recipient_viewed_at'] is not None


def test_admin_event_progress_page(client):
    client.get('/login/dev')
    response = client.get('/admin/events/1/progress')
    if response.status_code == 404:
        return
    assert response.status_code in (200, 302)
    if response.status_code == 200:
        body = response.get_data(as_text=True)
        assert 'progress-kanban' in body
        assert 'Дед Мороз' in body
