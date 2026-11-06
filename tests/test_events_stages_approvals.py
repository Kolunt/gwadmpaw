"""Participant approvals seeding: skip when already complete, avoid redundant work."""

import os

import gwadm.db as gwadm_db
from gwadm.db import get_db_connection
from gwadm.services.events_stages import (
    _participant_approvals_need_seeding,
    create_participant_approvals_for_event,
)


def _reset_db(db_path):
    import gwadm.config as config

    os.environ['DATABASE_PATH'] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None
    gwadm_db.ensure_db()


def test_participant_approvals_need_seeding(tmp_path):
    db_path = str(tmp_path / 'appr.db')
    _reset_db(db_path)
    conn = get_db_connection()
    conn.execute('INSERT INTO events (id, name) VALUES (1, "e")')
    conn.execute('INSERT INTO event_registrations (event_id, user_id) VALUES (1, 10), (1, 20)')
    assert _participant_approvals_need_seeding(conn, 1) is True
    conn.execute(
        'INSERT INTO event_participant_approvals (event_id, user_id, approved) VALUES (1, 10, 0), (1, 20, 0)'
    )
    assert _participant_approvals_need_seeding(conn, 1) is False
    conn.close()


def test_create_participant_approvals_skips_when_complete(tmp_path, monkeypatch):
    db_path = str(tmp_path / 'appr_skip.db')
    _reset_db(db_path)
    conn = get_db_connection()
    conn.execute('INSERT INTO events (id, name) VALUES (1, "e")')
    conn.execute('INSERT INTO event_registrations (event_id, user_id) VALUES (1, 10)')
    conn.execute(
        'INSERT INTO event_participant_approvals (event_id, user_id, approved) VALUES (1, 10, 0)'
    )
    conn.commit()
    conn.close()

    called = []

    monkeypatch.setattr(
        'gwadm.services.events_stages._revoke_gift_events',
        lambda c, eid: called.append('revoke'),
    )

    create_participant_approvals_for_event(1)

    assert called == []
