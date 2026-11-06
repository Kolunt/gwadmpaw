"""UI strings table and _() lookup."""

import gwadm.db as gwadm_db
from gwadm.i18n import _
from gwadm.services.ui_strings import invalidate_ui_strings_cache


def test_ui_string_overrides_from_db(app):
    gwadm_db.ensure_db()
    conn = gwadm_db.get_db_connection()
    conn.execute(
        '''
        INSERT OR REPLACE INTO ui_strings
        (msg_key, category, locale, default_text, value, updated_at, updated_by)
        VALUES ('Home', 'legacy', 'ru', 'Главная', 'Дом', datetime('now'), NULL)
        '''
    )
    conn.commit()
    conn.close()
    invalidate_ui_strings_cache()

    with app.test_request_context('/'):
        assert _('Home') == 'Дом'


def test_generated_key_fallback(app):
    with app.test_request_context('/'):
        assert _('') == ''
        # unknown key falls through to itself (not empty)
        assert _('no_such_key_xyz') == 'no_such_key_xyz'
