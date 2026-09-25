#!/usr/bin/env python3
"""Проверка всех HTML-маршрутов: публичные, пользовательские, админ."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _collect_ids(conn):
    ids = {}
    row = conn.execute('SELECT user_id FROM users ORDER BY user_id LIMIT 1').fetchone()
    ids['user_id'] = row['user_id'] if row else None
    row = conn.execute('SELECT id FROM events WHERE deleted_at IS NULL ORDER BY id LIMIT 1').fetchone()
    ids['event_id'] = row['id'] if row else None
    row = conn.execute('SELECT id FROM titles LIMIT 1').fetchone()
    ids['title_id'] = row['id'] if row else None
    row = conn.execute('SELECT id FROM awards LIMIT 1').fetchone()
    ids['award_id'] = row['id'] if row else None
    return ids


def _routes(ids):
    uid = ids['user_id']
    eid = ids['event_id']
    tid = ids['title_id']
    aid = ids['award_id']

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

    admin = [
        ('GET', '/admin/'),
        ('GET', '/admin/users'),
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
    ]
    if uid:
        admin.extend([
            ('GET', f'/admin/users/{uid}/edit'),
            ('GET', f'/admin/users/{uid}/roles'),
            ('GET', f'/admin/users/{uid}/titles'),
            ('GET', f'/admin/rating/{uid}'),
        ])
    if eid:
        admin.extend([
            ('GET', f'/admin/events/{eid}'),
            ('GET', f'/admin/events/{eid}/edit'),
            ('GET', f'/admin/events/{eid}/participants'),
            ('GET', f'/admin/events/{eid}/distribution/positive'),
        ])

    return public, user, admin


def _login_admin(client):
    client.get('/login/dev')


def main() -> int:
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
            errors.append(f'{label} {method} {path} → {resp.status_code} (expected {expect})')
            return
        body = resp.get_data(as_text=True)
        if resp.status_code == 200 and ('Internal Server Error' in body or 'Traceback' in body):
            errors.append(f'{label} {method} {path} → 200 but error page in body')
            return
        ok += 1

    for method, path in public:
        expect = 200 if path != '/health' else 200
        check('public', method, path, expect)

    for method, path in user:
        check('anon-user', method, path, 302)

    _login_admin(client)
    for method, path in user:
        check('user', method, path, 200)

    for method, path in admin:
        check('admin', method, path, 200)

    if errors:
        print(f'FAIL: {len(errors)} route(s), {ok} OK')
        for err in errors:
            print(f'  - {err}')
        return 1

    print(f'OK: verify_web_routes passed ({ok} routes)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
