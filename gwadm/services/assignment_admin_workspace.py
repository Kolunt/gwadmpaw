"""Admin workspace: notes and tasks per event assignment (kanban cards)."""

from datetime import datetime

from gwadm.db import get_db_connection


def _row_dict(row):
    return dict(row) if row else None


def _now_iso() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def parse_optional_due_at(raw) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return datetime.strptime(text, fmt).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def get_assignment_for_event(event_id: int, assignment_id: int, conn=None) -> dict | None:
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()
    try:
        row = conn.execute(
            '''
            SELECT ea.id, ea.event_id, ea.santa_user_id, ea.recipient_user_id,
                   santa.username AS santa_username,
                   recipient.username AS recipient_username
            FROM event_assignments ea
            JOIN users santa ON santa.user_id = ea.santa_user_id
            JOIN users recipient ON recipient.user_id = ea.recipient_user_id
            WHERE ea.id = ? AND ea.event_id = ?
              AND (ea.is_archived = 0 OR ea.is_archived IS NULL)
            ''',
            (assignment_id, event_id),
        ).fetchone()
        return _row_dict(row)
    finally:
        if own_conn:
            conn.close()


def count_open_tasks(conn, assignment_id: int) -> int:
    row = conn.execute(
        '''
        SELECT COUNT(*) AS c FROM assignment_admin_tasks
        WHERE assignment_id = ? AND completed_at IS NULL
        ''',
        (assignment_id,),
    ).fetchone()
    return int(row['c'] or 0)


def _serialize_note(row) -> dict:
    d = _row_dict(row)
    return {
        'id': d['id'],
        'body': d['body'],
        'author_user_id': d['author_user_id'],
        'created_at': d.get('created_at'),
        'updated_at': d.get('updated_at'),
    }


def _serialize_task(row) -> dict:
    d = _row_dict(row)
    return {
        'id': d['id'],
        'body': d['body'],
        'due_at': d.get('due_at'),
        'completed_at': d.get('completed_at'),
        'author_user_id': d['author_user_id'],
        'created_at': d.get('created_at'),
        'updated_at': d.get('updated_at'),
        'is_completed': d.get('completed_at') is not None and str(d.get('completed_at')).strip() != '',
    }


def get_workspace(assignment_id: int, conn=None) -> dict:
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()
    try:
        notes = [
            _serialize_note(r)
            for r in conn.execute(
                '''
                SELECT id, body, author_user_id, created_at, updated_at
                FROM assignment_admin_notes
                WHERE assignment_id = ?
                ORDER BY created_at DESC, id DESC
                ''',
                (assignment_id,),
            ).fetchall()
        ]
        tasks = [
            _serialize_task(r)
            for r in conn.execute(
                '''
                SELECT id, body, due_at, completed_at, author_user_id, created_at, updated_at
                FROM assignment_admin_tasks
                WHERE assignment_id = ?
                ORDER BY
                    CASE WHEN completed_at IS NULL THEN 0 ELSE 1 END,
                    due_at IS NULL,
                    due_at ASC,
                    id ASC
                ''',
                (assignment_id,),
            ).fetchall()
        ]
        open_tasks_count = count_open_tasks(conn, assignment_id)
        return {
            'notes': notes,
            'tasks': tasks,
            'open_tasks_count': open_tasks_count,
        }
    finally:
        if own_conn:
            conn.close()


def workspace_payload(assignment: dict, assignment_id: int, conn=None) -> dict:
    ws = get_workspace(assignment_id, conn=conn)
    return {
        'assignment_id': assignment_id,
        'santa_username': assignment.get('santa_username') or '—',
        'recipient_username': assignment.get('recipient_username') or '—',
        **ws,
    }


def create_note(assignment_id: int, author_user_id: int, body: str) -> dict:
    text = (body or '').strip()
    if not text:
        raise ValueError('Текст заметки не может быть пустым')
    conn = get_db_connection()
    try:
        now = _now_iso()
        cur = conn.execute(
            '''
            INSERT INTO assignment_admin_notes (assignment_id, author_user_id, body, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (assignment_id, author_user_id, text, now, now),
        )
        conn.commit()
        note_id = cur.lastrowid
        row = conn.execute(
            'SELECT id, body, author_user_id, created_at, updated_at FROM assignment_admin_notes WHERE id = ?',
            (note_id,),
        ).fetchone()
        return _serialize_note(row)
    finally:
        conn.close()


def update_note(note_id: int, assignment_id: int, body: str) -> dict | None:
    text = (body or '').strip()
    if not text:
        raise ValueError('Текст заметки не может быть пустым')
    conn = get_db_connection()
    try:
        row = conn.execute(
            'SELECT id FROM assignment_admin_notes WHERE id = ? AND assignment_id = ?',
            (note_id, assignment_id),
        ).fetchone()
        if not row:
            return None
        now = _now_iso()
        conn.execute(
            'UPDATE assignment_admin_notes SET body = ?, updated_at = ? WHERE id = ?',
            (text, now, note_id),
        )
        conn.commit()
        updated = conn.execute(
            'SELECT id, body, author_user_id, created_at, updated_at FROM assignment_admin_notes WHERE id = ?',
            (note_id,),
        ).fetchone()
        return _serialize_note(updated)
    finally:
        conn.close()


def delete_note(note_id: int, assignment_id: int) -> bool:
    conn = get_db_connection()
    try:
        cur = conn.execute(
            'DELETE FROM assignment_admin_notes WHERE id = ? AND assignment_id = ?',
            (note_id, assignment_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def create_task(assignment_id: int, author_user_id: int, body: str, due_at=None) -> dict:
    text = (body or '').strip()
    if not text:
        raise ValueError('Текст задачи не может быть пустым')
    due_parsed = parse_optional_due_at(due_at)
    conn = get_db_connection()
    try:
        now = _now_iso()
        cur = conn.execute(
            '''
            INSERT INTO assignment_admin_tasks (
                assignment_id, author_user_id, body, due_at, completed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, ?, ?)
            ''',
            (assignment_id, author_user_id, text, due_parsed, now, now),
        )
        conn.commit()
        task_id = cur.lastrowid
        row = conn.execute(
            '''
            SELECT id, body, due_at, completed_at, author_user_id, created_at, updated_at
            FROM assignment_admin_tasks WHERE id = ?
            ''',
            (task_id,),
        ).fetchone()
        return _serialize_task(row)
    finally:
        conn.close()


def update_task(
    task_id: int,
    assignment_id: int,
    *,
    body: str | None = None,
    due_at=...,
    completed: bool | None = None,
) -> dict | None:
    conn = get_db_connection()
    try:
        row = conn.execute(
            '''
            SELECT id, body, due_at, completed_at
            FROM assignment_admin_tasks WHERE id = ? AND assignment_id = ?
            ''',
            (task_id, assignment_id),
        ).fetchone()
        if not row:
            return None
        new_body = row['body']
        if body is not None:
            new_body = (body or '').strip()
            if not new_body:
                raise ValueError('Текст задачи не может быть пустым')
        new_due = row['due_at']
        if due_at is not ...:
            new_due = parse_optional_due_at(due_at)
        new_completed = row['completed_at']
        if completed is True:
            new_completed = _now_iso()
        elif completed is False:
            new_completed = None
        now = _now_iso()
        conn.execute(
            '''
            UPDATE assignment_admin_tasks
            SET body = ?, due_at = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            ''',
            (new_body, new_due, new_completed, now, task_id),
        )
        conn.commit()
        updated = conn.execute(
            '''
            SELECT id, body, due_at, completed_at, author_user_id, created_at, updated_at
            FROM assignment_admin_tasks WHERE id = ?
            ''',
            (task_id,),
        ).fetchone()
        return _serialize_task(updated)
    finally:
        conn.close()


def delete_task(task_id: int, assignment_id: int) -> bool:
    conn = get_db_connection()
    try:
        cur = conn.execute(
            'DELETE FROM assignment_admin_tasks WHERE id = ? AND assignment_id = ?',
            (task_id, assignment_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
