"""Assignments hidden from participants until celebration_date."""

from datetime import datetime

import gwadm.db as gwadm_db
from gwadm.db import get_db_connection
from gwadm.services.assignments import mark_assignment_sent
from gwadm.services.events_stages import assignments_revealed_for_event

SANTA_ID = 88001
RECIPIENT_ID = 88002
SECRET_ADDRESS = 'VISIBILITY_TEST_STREET_42'


def _seed_event_with_assignment(conn, event_id=880):
    conn.execute(
        'INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?), (?, ?)',
        (SANTA_ID, 'santa_vis', RECIPIENT_ID, 'kid_vis'),
    )
    conn.execute(
        'INSERT OR IGNORE INTO events (id, name) VALUES (?, ?)',
        (event_id, 'Visibility test event'),
    )
    timeline = [
        ('pre_registration', 1, '2025-01-01 00:00:00', None),
        ('main_registration', 2, '2025-01-02 00:00:00', None),
        ('lottery', 3, '2025-01-10 00:00:00', None),
        ('celebration_date', 4, '2025-01-20 00:00:00', None),
        ('after_party', 5, None, '2099-02-01 00:00:00'),
    ]
    for stage_type, order, start, end in timeline:
        conn.execute(
            '''
            INSERT OR REPLACE INTO event_stages
            (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
            VALUES (?, ?, ?, ?, ?, 1, 0)
            ''',
            (event_id, stage_type, order, start, end),
        )
    conn.execute(
        '''
        INSERT OR REPLACE INTO event_assignments
        (id, event_id, santa_user_id, recipient_user_id, is_archived)
        VALUES (88001, ?, ?, ?, 0)
        ''',
        (event_id, SANTA_ID, RECIPIENT_ID),
    )
    conn.execute(
        '''
        INSERT OR REPLACE INTO event_registration_details
        (event_id, user_id, last_name, first_name, street, city, country)
        VALUES (?, ?, 'Testov', 'Recipient', ?, 'Moscow', 'Russia')
        ''',
        (event_id, RECIPIENT_ID, SECRET_ADDRESS),
    )
    conn.commit()


def test_assignments_revealed_for_event_lottery_vs_celebration(app, monkeypatch):
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_event_with_assignment(conn)
    conn.close()

    monkeypatch.setattr(
        'gwadm.services.events_stages.create_participant_approvals_for_event',
        lambda event_id: None,
    )
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 15, 12, 0, 0),
    )
    assert assignments_revealed_for_event(880) is False

    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 25, 12, 0, 0),
    )
    assert assignments_revealed_for_event(880) is True


def test_assignments_page_stub_during_lottery(client, app, monkeypatch):
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_event_with_assignment(conn)
    conn.close()

    monkeypatch.setattr(
        'gwadm.services.events_stages.create_participant_approvals_for_event',
        lambda event_id: None,
    )
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 15, 12, 0, 0),
    )

    with client.session_transaction() as sess:
        sess['user_id'] = SANTA_ID

    response = client.get('/assignments')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'Обмен подарками' in body
    assert SECRET_ADDRESS not in body
    assert 'Вы - Дед Мороз' not in body


def test_assignments_page_shows_details_on_celebration(client, app, monkeypatch):
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_event_with_assignment(conn)
    conn.close()

    monkeypatch.setattr(
        'gwadm.services.events_stages.create_participant_approvals_for_event',
        lambda event_id: None,
    )
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 25, 12, 0, 0),
    )

    with client.session_transaction() as sess:
        sess['user_id'] = SANTA_ID

    response = client.get('/assignments')
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert SECRET_ADDRESS in body
    assert 'Вы - Дед Мороз' in body


def test_mark_sent_blocked_during_lottery(app, monkeypatch):
    gwadm_db.ensure_db()
    conn = get_db_connection()
    _seed_event_with_assignment(conn)
    conn.close()

    monkeypatch.setattr(
        'gwadm.services.events_stages.create_participant_approvals_for_event',
        lambda event_id: None,
    )
    lottery_now = datetime(2025, 1, 15, 12, 0, 0)
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: lottery_now,
    )
    monkeypatch.setattr(
        'gwadm.services.events.get_event_now',
        lambda: lottery_now,
    )

    ok, message = mark_assignment_sent(88001, SANTA_ID, 'track 123')
    assert ok is False
    assert 'Обмен подарками' in message
