"""Admin awards create/edit."""



def _csrf_form(client):
    client.get('/login/dev')
    client.get('/admin/awards/create')
    with client.session_transaction() as sess:
        return sess.get('_csrf_token')


def test_admin_award_create_post(client):
    token = _csrf_form(client)
    assert token
    response = client.post(
        '/admin/awards/create',
        data={
            '_csrf_token': token,
            'title': 'Test award',
            'icon': '🎖️',
            'sort_order': '100',
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert '/admin/awards' in response.headers.get('Location', '')
