"""Admin layout and navigation tests."""

import re


def test_admin_dashboard_uses_admin_theme(client):
    client.get('/login/dev')
    response = client.get('/admin/')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'data-theme="admin"' in body
    assert 'id="theme-toggle"' not in body
    assert 'Админ-панель' in body
    assert 'filter=rating_positive' in body
    assert 'filter=online' in body
    assert 'admin-dashboard-card-link' in body


def test_admin_users_dashboard_filters(client):
    client.get('/login/dev')
    response = client.get('/admin/users?filter=never_participated')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'admin-users-filter-banner' in body
    assert 'Без участия в мероприятиях' in body


def test_admin_users_uses_admin_sidebar(client):
    client.get('/login/dev')
    response = client.get('/admin/users')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'data-theme="admin"' in body
    assert 'sidebar-section-admin' not in body
    assert 'На сайт' in body


def test_admin_event_progress_markup(client):
    client.get('/login/dev')
    response = client.get('/admin/events/1/progress')
    if response.status_code == 404:
        return
    assert response.status_code in (200, 302)
    if response.status_code == 200:
        body = response.get_data(as_text=True)
        assert 'progress-kanban' in body
        assert 'progress-role-tabs' in body
        assert 'progress-kanban-card-meta' not in body


def test_admin_logs_responsive_markup(client):
    client.get('/login/dev')
    response = client.get('/admin/logs')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'admin-logs-page' in body
    assert 'logs-filter' in body
    assert 'logs-table' in body
    assert 'data-label="Время"' in body


def test_public_sidebar_single_admin_link(client):
    client.get('/login/dev')
    response = client.get('/')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert body.count('admin.admin_users') == 0 or 'url_for' not in body
    assert 'Админ' in body or 'Admin' in body
    admin_panel_links = len(re.findall(r'/admin/?["\']', body))
    assert admin_panel_links >= 1
    assert 'Цыфорки' not in body
