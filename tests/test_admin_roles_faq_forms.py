"""Admin role and FAQ form pages (GET) after refactor imports."""


def test_admin_role_create_get(client):
    client.get('/login/dev')
    response = client.get('/admin/roles/create')
    assert response.status_code == 200


def test_admin_faq_create_get(client):
    client.get('/login/dev')
    response = client.get('/admin/faq/create')
    assert response.status_code == 200
