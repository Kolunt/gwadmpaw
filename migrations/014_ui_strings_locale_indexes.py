"""Indexes for admin localization list queries (locale + sort/filter)."""

import sqlite3


def upgrade(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_ui_strings_locale_category_msg
        ON ui_strings(locale, category, msg_key)
        '''
    )
