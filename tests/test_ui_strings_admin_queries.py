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
    ui_strings.invalidate_ui_strings_cache()


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


def test_list_strings_shows_ru_without_en_pair(tmp_path):
    """INNER JOIN hid orphan ru rows — total > 0 but table empty."""
    db_path = str(tmp_path / 'ui_strings_orphan.db')
    _reset_db(db_path)

    conn = get_db_connection()
    conn.execute(
        '''
        INSERT INTO ui_strings
        (msg_key, category, locale, default_text, value, updated_at, updated_by)
        VALUES ('ORPHAN_RU_ONLY_UNIQUE', 'test', 'ru', 'x', 'ru-val', datetime('now'), NULL)
        '''
    )
    conn.commit()
    conn.close()

    conn = get_db_connection()
    q = 'ORPHAN_RU_ONLY_UNIQUE'
    total = ui_strings.count_strings(conn, None, q)
    rows = ui_strings.list_strings_for_admin(
        conn, category=None, q=q, limit=50, offset=0
    )
    conn.close()

    assert total == 1
    orphan = [r for r in rows if r['msg_key'] == 'ORPHAN_RU_ONLY_UNIQUE']
    assert len(orphan) == 1
    assert orphan[0]['en_value'] == ''


def test_seed_survives_connection_close(tmp_path):
    """Seed must COMMIT; otherwise close() rolls back and the admin table is empty."""
    db_path = str(tmp_path / 'ui_strings_seed_commit.db')
    _reset_db(db_path)
    ui_strings.invalidate_ui_strings_cache()

    conn = get_db_connection()
    ui_strings.ensure_ui_strings_seeded(conn)
    conn.close()

    conn = get_db_connection()
    ru_n = conn.execute(
        "SELECT COUNT(*) AS c FROM ui_strings WHERE locale = 'ru'"
    ).fetchone()['c']
    conn.close()
    assert ru_n >= ui_strings._EXPECTED_RU_ROWS


def test_apply_bulk_updates_upserts_missing_row(tmp_path):
    db_path = str(tmp_path / 'ui_strings_upsert.db')
    _reset_db(db_path)
    conn = get_db_connection()
    n = ui_strings.apply_bulk_updates(
        conn, [('BrandNewKeyForUpsert', 'ru', 'новое')], editor_id=None
    )
    conn.commit()
    row = conn.execute(
        "SELECT value FROM ui_strings WHERE msg_key = ? AND locale = 'ru'",
        ('BrandNewKeyForUpsert',),
    ).fetchone()
    conn.close()
    assert n >= 1
    assert row and row['value'] == 'новое'
