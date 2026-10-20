"""First view timestamps for assignment progress kanban."""

import sqlite3

from gwadm.migrations.runner import add_column_if_missing


def upgrade(conn: sqlite3.Connection) -> None:
    add_column_if_missing(
        conn,
        'event_assignments',
        'santa_viewed_at',
        'ALTER TABLE event_assignments ADD COLUMN santa_viewed_at TIMESTAMP',
    )
    add_column_if_missing(
        conn,
        'event_assignments',
        'recipient_viewed_at',
        'ALTER TABLE event_assignments ADD COLUMN recipient_viewed_at TIMESTAMP',
    )
