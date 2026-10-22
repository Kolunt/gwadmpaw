"""Admin workspace notes/tasks on assignment progress kanban."""

import gwadm.db as gwadm_db
from gwadm.services.assignment_admin_workspace import (
    build_workspace_entries,
    create_note,
    create_task,
    delete_note,
    delete_task,
    get_workspace,
    update_note,
    update_task,
)
from gwadm.services.assignment_progress import get_event_progress_board


def _seed_assignment(conn):
    conn.execute(
        '''
        INSERT OR IGNORE INTO users (user_id, username, last_login)
        VALUES (91001, 'ws_santa', datetime('now')), (91002, 'ws_kid', datetime('now'))
        '''
    )
    conn.execute(
        "INSERT INTO events (name, description, created_at) VALUES ('WS event', '', datetime('now'))"
    )
    event_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.execute(
        '''
        INSERT INTO event_assignments (event_id, santa_user_id, recipient_user_id, is_archived)
        VALUES (?, 91001, 91002, 0)
        ''',
        (event_id,),
    )
    assignment_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.commit()
    return event_id, assignment_id


def test_workspace_note_and_task_crud(app):
    gwadm_db.ensure_db()
    conn = gwadm_db.get_db_connection()
    event_id, assignment_id = _seed_assignment(conn)
    conn.close()

    note = create_note(assignment_id, 91001, 'Позвонить участнику')
    assert note['body'] == 'Позвонить участнику'
    assert note['author_username'] == 'ws_santa'

    updated = update_note(note['id'], assignment_id, 'Позвонить завтра')
    assert updated['body'] == 'Позвонить завтра'

    task = create_task(assignment_id, 91001, 'Проверить трек', '2026-12-01T10:00')
    assert task['body'] == 'Проверить трек'
    assert task['is_completed'] is False
    assert task['show_checkbox'] is True

    done = update_task(task['id'], assignment_id, completed=True)
    assert done['is_completed'] is True
    assert done.get('completed_at')

    reopened = update_task(task['id'], assignment_id, completed=False)
    assert reopened['is_completed'] is False

    ws = get_workspace(assignment_id)
    assert len(ws['notes']) == 1
    assert len(ws['tasks']) == 1
    assert ws['open_tasks_count'] == 1
    assert len(ws['entries']) == 2
    kinds = {e['kind'] for e in ws['entries']}
    assert kinds == {'note', 'task'}

    assert delete_task(task['id'], assignment_id)
    assert delete_note(note['id'], assignment_id)
    assert get_workspace(assignment_id)['open_tasks_count'] == 0


def test_open_tasks_count_requires_due_at(app):
    gwadm_db.ensure_db()
    conn = gwadm_db.get_db_connection()
    _, assignment_id = _seed_assignment(conn)
    conn.close()

    create_task(assignment_id, 91001, 'Без срока')
    ws = get_workspace(assignment_id)
    assert ws['open_tasks_count'] == 0
    assert ws['tasks'][0]['show_checkbox'] is False

    with_due = create_task(assignment_id, 91001, 'Со сроком', '2026-12-01T10:00')
    ws = get_workspace(assignment_id)
    assert ws['open_tasks_count'] == 1

    done = update_task(with_due['id'], assignment_id, completed=True)
    ws_done = get_workspace(assignment_id)
    assert ws_done['open_tasks_count'] == 0
    task_entry = next(e for e in ws_done['entries'] if e['kind'] == 'task' and e['id'] == with_due['id'])
    assert task_entry['is_completed'] is True
    assert task_entry.get('completed_at')


def test_build_workspace_entries_sorts_newest_first():
    notes = [{'id': 1, 'body': 'n', 'created_at': '2026-01-01 10:00:00', 'author_username': 'a'}]
    tasks = [
        {
            'id': 2,
            'body': 't',
            'created_at': '2026-01-02 10:00:00',
            'author_username': 'b',
            'due_at': '2026-12-01 10:00:00',
            'is_completed': False,
            'show_checkbox': True,
        },
    ]
    entries = build_workspace_entries(notes, tasks)
    assert [e['kind'] for e in entries] == ['task', 'note']


def test_progress_board_open_tasks_count(app):
    gwadm_db.ensure_db()
    conn = gwadm_db.get_db_connection()
    event_id, assignment_id = _seed_assignment(conn)
    create_task(assignment_id, 91001, 'Open task', '2026-12-01T10:00')
    conn.close()

    board = get_event_progress_board(event_id)
    assert board is not None
    found = False
    for stage_cards in board['boards']['santa'].values():
        for card in stage_cards:
            if card['assignment_id'] == assignment_id:
                assert card['open_tasks_count'] == 1
                found = True
    assert found


def _csrf_headers(client):
    with client.session_transaction() as sess:
        token = sess.get('_csrf_token')
    return {
        'X-CSRF-Token': token or '',
        'X-Requested-With': 'XMLHttpRequest',
    }


def test_workspace_api_requires_admin(client):
    gwadm_db.ensure_db()
    conn = gwadm_db.get_db_connection()
    event_id, assignment_id = _seed_assignment(conn)
    conn.close()

    url = f'/admin/events/{event_id}/assignments/{assignment_id}/workspace'
    assert client.get(url).status_code in (302, 403)

    client.get('/login/dev')
    client.get('/admin/')
    resp = client.get(url)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['assignment_id'] == assignment_id
    assert 'entries' in data
    assert 'notes' in data and 'tasks' in data

    headers = _csrf_headers(client)
    assert headers['X-CSRF-Token'], 'CSRF token must be in session after GET'
    post = client.post(
        f'{url}/notes',
        json={'body': 'API note'},
        headers=headers,
    )
    assert post.status_code == 200
    payload = post.get_json()
    assert payload['open_tasks_count'] >= 0
    assert any(e['kind'] == 'note' for e in payload.get('entries', []))


def test_admin_progress_page_workspace_markup(client):
    client.get('/login/dev')
    response = client.get('/admin/events/1/progress')
    if response.status_code == 404:
        return
    if response.status_code == 200:
        body = response.get_data(as_text=True)
        assert 'progress-workspace-modal' in body
        assert 'progress-workspace-entry-form' in body
        assert 'progress-workspace-entry-due' in body
        assert 'datetime-field-wrap' in body
        assert 'datetime-picker-btn' in body
        assert 'progress-workspace-entries-list' in body
        assert 'progress-workspace-notes-form' not in body
        assert 'progress-workspace-tasks-form' not in body
        assert 'admin-event-progress.js' in body
        assert 'progress-kanban-icon-btn' in body
        if 'progress-kanban-card' in body:
            assert 'js-progress-workspace' in body
            assert 'aria-label="Переписка"' in body
