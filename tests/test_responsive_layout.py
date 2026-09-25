"""Responsive layout smoke tests."""

import re


def _assert_responsive_shell(body):
    assert 'name="viewport"' in body
    assert 'width=device-width' in body
    assert 'id="sidebar-toggle"' in body
    assert 'content-wrapper' in body
    assert 'css/style.css' in body


def test_home_responsive_shell(client):
    response = client.get('/')
    assert response.status_code == 200
    _assert_responsive_shell(response.get_data(as_text=True))


def test_admin_responsive_shell(client):
    client.get('/login/dev')
    response = client.get('/admin/')
    assert response.status_code == 200
    _assert_responsive_shell(response.get_data(as_text=True))


def test_assignments_responsive_shell(client):
    client.get('/login/dev')
    response = client.get('/assignments')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    _assert_responsive_shell(body)
    assert '<style>' not in body or body.count('<style>') <= 1


def test_contacts_no_inline_page_styles(client):
    response = client.get('/contacts')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    _assert_responsive_shell(body)
    assert '.contacts-container' not in body
    assert 'contacts-header' in body


def test_admin_settings_no_inline_min_width(client):
    client.get('/login/dev')
    response = client.get('/admin/settings')
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'min-width: 520px' not in body
    assert 'table-scroll-desktop' in body


def test_letter_chat_styles_externalized(client):
    client.get('/login/dev')
    response = client.get('/letter')
    assert response.status_code in (200, 302)
    if response.status_code == 200:
        body = response.get_data(as_text=True)
        assert 'letter-switcher-btn' in body
        assert '.chat-wrapper' not in body
