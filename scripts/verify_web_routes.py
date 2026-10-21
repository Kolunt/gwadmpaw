#!/usr/bin/env python3
"""Проверка всех HTML-маршрутов: публичные, пользовательские, админ."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# (method, path) or (method, path, expected_status)
ADMIN_LIST_ROUTES = [
    ('GET', '/admin/'),
    ('GET', '/admin/users'),
    ('GET', '/admin/users?filter=online'),
    ('GET', '/admin/settings'),
    ('GET', '/admin/events'),
    ('GET', '/admin/faq'),
    ('GET', '/admin/awards'),
    ('GET', '/admin/logs'),
    ('GET', '/admin/letters'),
    ('GET', '/admin/rules'),
    ('GET', '/admin/broadcasts'),
    ('GET', '/admin/rating-settings'),
    ('GET', '/admin/titles'),
    ('GET', '/admin/hero-whispers'),
    ('GET', '/admin/letters/archived'),
    ('GET', '/admin/telegram/menu'),
]

ADMIN_FORM_CREATE_ROUTES = [
    ('GET', '/admin/users/create'),
    ('GET', '/admin/awards/create'),
    ('GET', '/admin/events/create'),
    ('GET', '/admin/titles/create'),
    ('GET', '/admin/roles/create'),
    ('GET', '/admin/faq/create'),
    ('GET', '/admin/faq/categories/create'),
    ('GET', '/admin/rules/edit'),
    # редирект на вкладку шаблонов
    ('GET', '/admin/broadcasts/templates', 302),
]


def _safe_query(conn, sql):
    try:
        return conn.execute(sql).fetchone()
    except Exception:
        return None


def _collect_ids(conn):
    ids = {}
    row = _safe_query(conn, 'SELECT user_id FROM users ORDER BY user_id LIMIT 1')
    ids['user_id'] = row['user_id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM events WHERE deleted_at IS NULL ORDER BY id LIMIT 1')
    ids['event_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM titles ORDER BY id LIMIT 1')
    ids['title_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM titles WHERE is_system = 0 ORDER BY id LIMIT 1')
    ids['title_edit_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM awards LIMIT 1')
    ids['award_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM roles WHERE is_system = 0 ORDER BY id LIMIT 1')
    ids['role_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM faq_items ORDER BY id LIMIT 1')
    ids['faq_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM faq_categories ORDER BY id LIMIT 1')
    ids['faq_category_id'] = row['id'] if row else None
    row = _safe_query(conn, 'SELECT id FROM broadcast_templates ORDER BY id LIMIT 1')
    ids['broadcast_template_id'] = row['id'] if row else None
    return ids


def _normalize_route(entry):
    if len(entry) == 2:
        return entry[0], entry[1], 200
    return entry[0], entry[1], entry[2]


def _routes(ids):
    uid = ids['user_id']
    eid = ids['event_id']
    tid = ids['title_id']
    tid_edit = ids.get('title_edit_id')
    aid = ids['award_id']
    rid = ids['role_id']
    fid = ids['faq_id']
    fcid = ids['faq_category_id']
    btid = ids['broadcast_template_id']

    public = [
        ('GET', '/'),
        ('GET', '/participants'),
        ('GET', '/faq'),
        ('GET', '/rules'),
        ('GET', '/contacts'),
        ('GET', '/rating'),
        ('GET', '/events'),
        ('GET', '/login'),
        ('GET', '/gwars-required'),
        ('GET', '/roles/admin'),
        ('GET', '/health'),
    ]
    if uid:
        public.append(('GET', f'/profile/{uid}'))
    if eid:
        public.append(('GET', f'/events/{eid}'))
    if tid:
        public.append(('GET', f'/titles/{tid}'))
    if aid:
        public.append(('GET', f'/awards/{aid}'))

    user = [
        ('GET', '/dashboard'),
        ('GET', '/profile/edit'),
        ('GET', '/assignments'),
        ('GET', '/letter'),
    ]

    admin = [_normalize_route(r) for r in ADMIN_LIST_ROUTES]
    admin.extend(_normalize_route(r) for r in ADMIN_FORM_CREATE_ROUTES)

    if uid:
        admin.extend([
            ('GET', f'/admin/users/{uid}/edit', 200),
            ('GET', f'/admin/users/{uid}/roles', 200),
            ('GET', f'/admin/users/{uid}/titles', 200),
            ('GET', f'/admin/rating/{uid}', 200),
        ])
    if aid:
        admin.append(('GET', f'/admin/awards/{aid}/edit', 200))
    if eid:
        admin.extend([
            ('GET', f'/admin/events/{eid}', 200),
            ('GET', f'/admin/events/{eid}/edit', 200),
            ('GET', f'/admin/events/{eid}/participants', 200),
            ('GET', f'/admin/events/{eid}/distribution/positive', 200),
            ('GET', f'/admin/events/{eid}/progress', 200),
        ])
    if tid_edit:
        admin.append(('GET', f'/admin/titles/{tid_edit}/edit', 200))
    if rid:
        admin.append(('GET', f'/admin/roles/{rid}/edit', 200))
    if fid:
        admin.append(('GET', f'/admin/faq/{fid}/edit', 200))
    if fcid:
        admin.append(('GET', f'/admin/faq/categories/{fcid}/edit', 200))
    if btid:
        admin.append(('GET', f'/admin/broadcasts/templates/{btid}', 200))

    return public, user, admin


def _login_admin(client):
    client.get('/login/dev')


def run_checks(flask_app=None):
    """Run all route checks. Returns (errors: list[str], ok_count: int)."""
    if flask_app is None:
        import app as app_module
        flask_app = app_module.app

    client = flask_app.test_client()

    from gwadm.db import get_db_connection

    conn = get_db_connection()
    ids = _collect_ids(conn)
    conn.close()

    public, user, admin = _routes(ids)
    errors = []
    ok = 0

    def check(label, method, path, expect=200):
        nonlocal ok
        if method == 'GET':
            resp = client.get(path, follow_redirects=False)
        else:
            resp = client.open(path, method=method, follow_redirects=False)
        if resp.status_code != expect:
            errors.append(f'{label} {method} {path} -> {resp.status_code} (expected {expect})')
            return
        if expect == 200:
            body = resp.get_data(as_text=True)
            if 'Internal Server Error' in body or 'Traceback' in body:
                errors.append(f'{label} {method} {path} -> 200 but error page in body')
                return
        ok += 1

    for method, path in public:
        check('public', method, path, 200)

    for method, path in user:
        check('anon-user', method, path, 302)

    _login_admin(client)
    for method, path in user:
        if path == '/letter':
            resp = client.get(path, follow_redirects=False)
            if resp.status_code not in (200, 302):
                errors.append(f'user GET {path} -> {resp.status_code} (expected 200 or 302)')
            else:
                ok += 1
            continue
        check('user', method, path, 200)

    for method, path, expect in admin:
        check('admin', method, path, expect)

    responsive_paths = ['/', '/contacts', '/admin/', '/assignments', '/letter']
    for path in responsive_paths:
        resp = client.get(path, follow_redirects=False)
        if resp.status_code == 200:
            body = resp.get_data(as_text=True)
            if 'width=device-width' not in body:
                errors.append(f'responsive {path} -> missing viewport meta')

    for path in ('/', '/admin/'):
        resp = client.get(path, follow_redirects=False)
        if resp.status_code == 200:
            body = resp.get_data(as_text=True)
            if 'layout-main' not in body:
                errors.append(f'layout {path} -> missing layout-main wrapper')

    return errors, ok


def main() -> int:
    errors, ok = run_checks()
    if errors:
        print(f'FAIL: {len(errors)} route(s), {ok} OK')
        for err in errors:
            print(f'  - {err}')
        return 1

    print(f'OK: verify_web_routes passed ({ok} routes)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
