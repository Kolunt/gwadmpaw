"""Hero whispers: admin CRUD and homepage rendering."""

import gwadm.db as gwadm_db
from gwadm.services.hero_whispers import set_hero_whispers_enabled
def test_admin_hero_whispers_page(client):
    client.get('/login/dev')
    response = client.get('/admin/hero-whispers')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'Шёпоты на главной' in body
    assert 'hero_whispers_enabled' in body
    assert 'admin-table' in body


def test_home_includes_hero_whispers_when_enabled(client):
    response = client.get('/')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'hero-whispers' in body
    assert 'hero-whispers.js' in body
    assert 'window.HERO_WHISPERS' in body


def test_home_excludes_hero_whispers_script_when_disabled(client, app):
    set_hero_whispers_enabled(False)
    response = client.get('/')
    body = response.get_data(as_text=True)
    assert 'hero-whispers' in body
    assert 'hero-whispers.js' not in body
    assert 'window.HERO_WHISPERS' not in body
    set_hero_whispers_enabled(True)


def test_migration_creates_hero_whispers_table(tmp_path):
    db_path = str(tmp_path / 'hero_whispers.db')
    import os

    import gwadm.config as config

    os.environ['DATABASE_PATH'] = db_path
    config.DATABASE_PATH = db_path
    gwadm_db._db_initialized = False
    gwadm_db._db_path = None
    gwadm_db.ensure_db()

    conn = gwadm_db.get_db_connection()
    table = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hero_whispers'"
    ).fetchone()
    count = conn.execute('SELECT COUNT(*) FROM hero_whispers').fetchone()[0]
    setting = conn.execute(
        "SELECT value FROM settings WHERE key = 'hero_whispers_enabled'"
    ).fetchone()
    conn.close()

    assert table is not None
    assert count >= 8
    assert setting is not None
    assert setting[0] == '1'
