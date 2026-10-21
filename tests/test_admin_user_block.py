"""Admin user block/unblock from edit page."""

from gwadm.db import get_db_connection


def _admin_csrf_form(client):
    client.get('/login/dev')
    client.get('/')
    with client.session_transaction() as sess:
        token = sess.get('_csrf_token')
    assert token
    return token


def test_block_user_from_standalone_form(client):
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO users (user_id, username, email, is_blocked) VALUES (?, ?, ?, 0)',
        (90950, 'block_target', 'target@example.com'),
    )
    conn.commit()
    conn.close()

    token = _admin_csrf_form(client)
    response = client.post(
        '/admin/users/90950/edit',
        data={
            '_csrf_token': token,
            'block_action': 'block',
            'blocked_reason': 'Нарушение правил',
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'Имя пользователя обязательно' not in body
    assert 'Пользователь успешно заблокирован' in body

    conn = get_db_connection()
    row = conn.execute(
        'SELECT is_blocked, blocked_reason FROM users WHERE user_id = ?',
        (90950,),
    ).fetchone()
    conn.close()
    assert row['is_blocked'] == 1
    assert row['blocked_reason'] == 'Нарушение правил'
