"""Max messenger column and profile_data_policy setting."""

import json
import sqlite3

from gwadm.migrations.runner import add_column_if_missing
from gwadm.services.profile_fields import (
    POLICY_SETTING_KEY,
    default_profile_data_policy,
)


def upgrade(conn: sqlite3.Connection) -> None:
    add_column_if_missing(
        conn,
        'users',
        'max',
        'ALTER TABLE users ADD COLUMN max TEXT',
    )
    add_column_if_missing(
        conn,
        'event_registration_details',
        'max',
        'ALTER TABLE event_registration_details ADD COLUMN max TEXT',
    )

    existing = conn.execute(
        'SELECT key FROM settings WHERE key = ?',
        (POLICY_SETTING_KEY,),
    ).fetchone()
    if not existing:
        policy_json = json.dumps(default_profile_data_policy(), ensure_ascii=False)
        conn.execute(
            '''
            INSERT INTO settings (key, value, description, category)
            VALUES (?, ?, ?, ?)
            ''',
            (
                POLICY_SETTING_KEY,
                policy_json,
                'Какие поля профиля собираем и какие обязательны',
                'profile_data',
            ),
        )
