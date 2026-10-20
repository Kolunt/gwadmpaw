"""Default rating setting for Max messenger contact."""

import sqlite3


def upgrade(conn: sqlite3.Connection) -> None:
    existing = conn.execute(
        "SELECT key FROM settings WHERE key = 'rating_contact_max'"
    ).fetchone()
    if not existing:
        conn.execute(
            '''
            INSERT INTO settings (key, value, description, category)
            VALUES (?, ?, ?, ?)
            ''',
            ('rating_contact_max', '1', 'Очки за заполненный Max', 'rating'),
        )
