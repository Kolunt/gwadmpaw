"""Admin localization SQL paths: no redundant seed checks per query."""

import os

import gwadm.db as gwadm_db
import gwadm.services.ui_strings as ui_strings
from gwadm.db import get_db_connection


def _reset_db(db_path: str) -> None:
    import gwadm.config as config

    os.environ['DATABASE_PATH'] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None
    gwadm_db.ensure_db()


def test_list_and_count_do_not_call_ensure_each(tmp_path, monkeypatch):
    db_path = str(tmp_path / 'ui_strings_admin.db')
    _reset_db(db_path)

    calls: list[int] = []
    orig = ui_strings.ensure_ui_strings_seeded

    def counted(conn):
        calls.append(1)
        return orig(conn)

    monkeypatch.setattr(ui_strings, 'ensure_ui_strings_seeded', counted)

    conn = get_db_connection()
    ui_strings.ensure_ui_strings_seeded(conn)
    assert len(calls) >= 1
    calls.clear()

    ui_strings.count_strings(conn, None, None)
    ui_strings.list_strings_for_admin(conn, category=None, q=None, limit=5, offset=0)
    ui_strings.list_categories(conn)
    assert calls == [], 'count/list should not re-invoke seed check'

    conn.close()
