"""Blocked account screen and global request gate."""

import pytest

from gwadm.db import ensure_db, get_db_connection
from gwadm.migrations.runner import run_migrations


@pytest.fixture(autouse=True)
def _apply_migrations_for_blocking_tests(monkeypatch):
    ensure_db()
    conn = get_db_connection()
    run_migrations(conn)
    conn.commit()
    conn.close()
    monkeypatch.setattr(
        'gwadm.context.user_in_celebration_exchange_stage',
        lambda user_id: False,
    )


def _ensure_block_columns(conn):
    from gwadm.migrations.runner import add_column_if_missing

    add_column_if_missing(
        conn, 'users', 'is_blocked', 'ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0'
    )
    add_column_if_missing(conn, 'users', 'blocked_by', 'ALTER TABLE users ADD COLUMN blocked_by INTEGER')
    add_column_if_missing(conn, 'users', 'blocked_reason', 'ALTER TABLE users ADD COLUMN blocked_reason TEXT')
    add_column_if_missing(
        conn, 'users', 'blocked_at', 'ALTER TABLE users ADD COLUMN blocked_at TIMESTAMP'
    )


def _insert_user(user_id, username, blocked=False, reason='Тестовая блокировка', blocked_by=None):
    ensure_db()
    conn = get_db_connection()
    _ensure_block_columns(conn)
    conn.execute(
        '''
        INSERT OR REPLACE INTO users (user_id, username, is_blocked, blocked_reason, blocked_by)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (
            user_id,
            username,
            1 if blocked else 0,
            reason if blocked else None,
            blocked_by,
        ),
    )
    conn.commit()
    conn.close()


def test_blocked_user_redirected_from_dashboard(client):
    _insert_user(90100, 'blocked_user', blocked=True, blocked_by=283494)
    client.get('/login/dev')
    with client.session_transaction() as sess:
        sess['user_id'] = 90100
        sess['username'] = 'blocked_user'
        sess['roles'] = ['user']

    response = client.get('/dashboard', follow_redirects=False)
    assert response.status_code == 302
    assert '/account/blocked' in response.location


def test_account_blocked_page_shows_reason(client):
    _insert_user(90101, 'blocked101', blocked=True, reason='Нарушение правил', blocked_by=283494)
    client.get('/login/dev')
    with client.session_transaction() as sess:
        sess['user_id'] = 90101
        sess['username'] = 'blocked101'
        sess['roles'] = ['user']

    response = client.get('/account/blocked')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'Нарушение правил' in body
    assert 'Доступ к сайту ограничен' in body


def test_blocked_user_api_returns_403(client):
    _insert_user(90102, 'blocked102', blocked=True)
    client.get('/login')
    with client.session_transaction() as sess:
        sess['user_id'] = 90102
        sess['roles'] = ['user']
        token = sess.get('_csrf_token')

    response = client.post(
        '/api/profile/update',
        json={'bio': 'test'},
        headers={
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRF-Token': token or '',
        },
    )
    assert response.status_code == 403
    data = response.get_json()
    assert data.get('code') == 'account_blocked'


def test_impersonation_bypasses_block_gate(client):
    _insert_user(90103, 'blocked103', blocked=True)
    client.get('/login/dev')
    with client.session_transaction() as sess:
        sess['user_id'] = 90103
        sess['username'] = 'blocked103'
        sess['roles'] = ['user']
        sess['impersonation_original'] = {
            'user_id': 283494,
            'username': '_Колунт_',
            'roles': ['admin'],
            'level': 50,
            'synd': 5594,
        }

    response = client.get('/dashboard')
    assert response.status_code == 200


def test_unblocked_user_reaches_dashboard(client):
    _insert_user(90104, 'free104', blocked=False)
    client.get('/login/dev')
    with client.session_transaction() as sess:
        sess['user_id'] = 90104
        sess['username'] = 'free104'
        sess['roles'] = ['user']

    response = client.get('/dashboard')
    assert response.status_code == 200
