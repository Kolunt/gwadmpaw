"""Event stage lifecycle after removing registration_closed."""

import os
from datetime import datetime

import gwadm.db as gwadm_db
from gwadm.db import get_db_connection
from gwadm.services.events import is_registration_open
from gwadm.services.events_stages import get_current_event_stage


def _reset_db(db_path):
    import gwadm.config as config

    os.environ['DATABASE_PATH'] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None
    gwadm_db.ensure_db()


def _seed_timeline_event(conn):
    conn.execute('INSERT INTO events (id, name) VALUES (1, "Test event")')
    timeline = [
        ('pre_registration', 1, '2025-01-01 00:00:00', None),
        ('main_registration', 2, '2025-01-02 00:00:00', None),
        ('lottery', 3, '2025-01-10 00:00:00', None),
        ('celebration_date', 4, '2025-01-20 00:00:00', None),
        ('after_party', 5, None, '2025-02-01 00:00:00'),
    ]
    for stage_type, order, start, end in timeline:
        conn.execute(
            '''
            INSERT INTO event_stages
            (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
            VALUES (?, ?, ?, ?, ?, 1, 0)
            ''',
            (1, stage_type, order, start, end),
        )
    conn.commit()


def test_registration_open_only_before_lottery(tmp_path, monkeypatch):
    db_path = str(tmp_path / 'stages.db')
    _reset_db(db_path)
    conn = get_db_connection()
    _seed_timeline_event(conn)
    conn.close()

    monkeypatch.setattr(
        'gwadm.services.events_stages.create_participant_approvals_for_event',
        lambda event_id: None,
    )
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 5, 12, 0, 0),
    )
    assert is_registration_open(1) is True

    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 15, 12, 0, 0),
    )
    assert is_registration_open(1) is False


def test_current_stage_is_lottery_between_lottery_and_celebration(tmp_path, monkeypatch):
    db_path = str(tmp_path / 'lottery_stage.db')
    _reset_db(db_path)
    conn = get_db_connection()
    _seed_timeline_event(conn)
    conn.close()

    monkeypatch.setattr(
        'gwadm.services.events_stages.create_participant_approvals_for_event',
        lambda event_id: None,
    )
    monkeypatch.setattr(
        'gwadm.services.events_stages.get_event_now',
        lambda: datetime(2025, 1, 15, 12, 0, 0),
    )
    stage = get_current_event_stage(1)
    assert stage is not None
    assert stage['info']['type'] == 'lottery'
