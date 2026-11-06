"""get_locale() must not hit the database on every _() call within one request."""

import gwadm.db as gwadm_db
from gwadm.db import get_db_connection
from gwadm.i18n import get_locale


def test_get_locale_queries_users_once_per_request(app, monkeypatch):
    gwadm_db.ensure_db()
    conn = get_db_connection()
    conn.execute(
        "INSERT OR REPLACE INTO users (user_id, username, language) VALUES (1, 'u', 'en')"
    )
    conn.commit()
    conn.close()

    opens: list[int] = []
    orig = gwadm_db.get_db_connection

    def counting_conn():
        opens.append(1)
        return orig()

    monkeypatch.setattr(gwadm_db, 'get_db_connection', counting_conn)

    with app.test_request_context('/'):
        from flask import session

        session['user_id'] = 1
        assert get_locale() == 'en'
        assert len(opens) == 1
        assert get_locale() == 'en'
        assert len(opens) == 1


def test_get_locale_without_user_no_db(app, monkeypatch):
    opens: list[int] = []
    orig = gwadm_db.get_db_connection

    def counting_conn():
        opens.append(1)
        return orig()

    monkeypatch.setattr(gwadm_db, 'get_db_connection', counting_conn)

    with app.test_request_context('/'):
        assert get_locale() == 'ru'
        assert get_locale() == 'ru'
        assert opens == []
