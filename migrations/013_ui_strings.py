"""UI strings table for admin-editable translations."""

import sqlite3


def upgrade(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS ui_strings (
            msg_key TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'misc',
            locale TEXT NOT NULL,
            default_text TEXT NOT NULL,
            value TEXT NOT NULL,
            updated_at TEXT,
            updated_by INTEGER,
            PRIMARY KEY (msg_key, locale),
            FOREIGN KEY (updated_by) REFERENCES users(user_id)
        )
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_ui_strings_category
        ON ui_strings(category)
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_ui_strings_locale
        ON ui_strings(locale)
        '''
    )
