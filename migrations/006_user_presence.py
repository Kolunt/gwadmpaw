"""Track user presence via last_seen for online counters."""

import sqlite3

from gwadm.migrations.runner import add_column_if_missing


def upgrade(conn: sqlite3.Connection) -> None:
    add_column_if_missing(
        conn,
        'users',
        'last_seen',
        'ALTER TABLE users ADD COLUMN last_seen TIMESTAMP',
    )
    conn.execute(
        '''
        UPDATE users
        SET last_seen = last_login
        WHERE last_seen IS NULL AND last_login IS NOT NULL
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_users_last_seen
        ON users(last_seen)
        '''
    )
