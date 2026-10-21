"""Responsive layout smoke tests."""

import re
from pathlib import Path


def _assert_responsive_shell(body):
    assert 'name="viewport"' in body
    assert 'width=device-width' in body
    assert 'id="sidebar-toggle"' in body
    assert 'id="sidebar-collapse"' in body
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
    assert 'settings-tab-content active' in body
    assert 'settings-tab active' in body


def test_letter_chat_styles_externalized(client):
    client.get('/login/dev')
    response = client.get('/letter')
    assert response.status_code in (200, 302)
    if response.status_code == 200:
        body = response.get_data(as_text=True)
        assert 'letter-role-switcher' in body or 'letter-event-select' in body or 'letter-chat-select' in body
        assert '.chat-wrapper' not in body


def test_layout_main_on_home(client):
    body = client.get('/').get_data(as_text=True)
    assert 'layout-main' in body


def test_layout_main_on_admin(client):
    client.get('/login/dev')
    body = client.get('/admin/').get_data(as_text=True)
    assert 'layout-main' in body


def test_sidebar_toggle_desktop_rule_follows_base_rule():
    css = Path('static/css/style.css').read_text(encoding='utf-8')
    base_rule = '.sidebar-toggle {\n    display: none;'
    desktop_rule = '@media (min-width: 1024px) {\n    .sidebar-toggle {\n        display: flex;'
    assert base_rule in css
    assert desktop_rule in css
    assert css.index(desktop_rule) > css.index(base_rule)


def test_admin_mobile_no_global_btn_sm_full_width():
    css = Path('static/css/style.css').read_text(encoding='utf-8')
    marker = '/* Mobile Responsive for Admin */'
    idx = css.index(marker)
    admin_mobile = css[idx:]
    assert not re.search(
        r'(?m)^\s*\.btn-sm\s*\{[^}]*width:\s*100%',
        admin_mobile,
    )


def test_datetime_picker_btn_compact_styles():
    css = Path('static/css/style.css').read_text(encoding='utf-8')
    assert '.datetime-field-wrap .datetime-picker-btn' in css
    block_start = css.index('.datetime-field-wrap .datetime-picker-btn')
    block = css[block_start:block_start + 500]
    assert 'width: auto' in block or 'flex: 0 0 auto' in block


def test_event_form_minimal_outer_padding():
    css = Path('static/css/style.css').read_text(encoding='utf-8')
    idx = css.index('/* Events */')
    block = css[idx:idx + 600]
    assert '.event-form {' in block
    assert 'padding: 0' in block.split('.stage-form-group')[0]


def test_event_view_card_minimal_outer_padding():
    css = Path('static/css/style.css').read_text(encoding='utf-8')
    idx = css.index('.event-view-card {')
    block = css[idx:idx + 200]
    assert 'padding: 0' in block


def test_home_stats_section_no_nested_container(client):
    body = client.get('/').get_data(as_text=True)
    match = re.search(
        r'<section class="hero-section[^"]*">(.*?)</section>',
        body,
        re.DOTALL,
    )
    assert match is not None
    assert 'class="container"' not in match.group(1)
    assert 'hero-whispers' in match.group(1)
    assert 'stats-bar' in match.group(1)
    assert 'stats-bar-item' in match.group(1)


def test_container_class_not_used_as_page_wrapper(client):
    """`.container` is aliased to layout-main; page wrappers use layout-main."""
    for path in ('/', '/contacts'):
        body = client.get(path).get_data(as_text=True)
        assert 'class="layout-main"' in body


def test_home_event_cards_use_status_indicators(client):
    body = client.get('/').get_data(as_text=True)
    if 'event-card-home' not in body:
        return
    assert 'event-status-indicator' in body
    assert 'event-status-active' in body or 'event-status-finished' in body
    assert 'event-stage-badge' not in body
    assert 'event-card-home-link' in body
    assert 'Подробнее' not in body
    assert '/events/' in body


def test_home_hero_assignment_cta_during_celebration(client, monkeypatch):
    monkeypatch.setattr(
        'gwadm.context.user_in_celebration_exchange_stage',
        lambda user_id: True,
    )
    with client.session_transaction() as sess:
        sess['user_id'] = 1
    body = client.get('/').get_data(as_text=True)
    assert 'У вас есть задание!' in body
    assert 'letter-button-animated' in body
    assert '/assignments' in body
    hero = re.search(r'<div class="hero-content">(.*?)</div>\s*<div class="stats-bar', body, re.DOTALL)
    assert hero is not None
    assert 'Перейти в профиль' not in hero.group(1)
