"""Admin notes and tasks on assignment progress kanban cards."""

import sqlite3


def upgrade(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS assignment_admin_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (assignment_id) REFERENCES event_assignments(id) ON DELETE CASCADE,
            FOREIGN KEY (author_user_id) REFERENCES users(user_id)
        )
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_assignment_admin_notes_assignment
        ON assignment_admin_notes(assignment_id)
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS assignment_admin_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            due_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (assignment_id) REFERENCES event_assignments(id) ON DELETE CASCADE,
            FOREIGN KEY (author_user_id) REFERENCES users(user_id)
        )
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_assignment_admin_tasks_assignment
        ON assignment_admin_tasks(assignment_id, completed_at)
        '''
    )
