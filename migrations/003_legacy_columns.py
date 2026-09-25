"""Legacy column additions and snowflake_events points migration."""

import sqlite3

from gwadm.logging_config import log_debug
from gwadm.migrations.runner import add_column_if_missing


def upgrade(conn: sqlite3.Connection) -> None:
    c = conn.cursor()

    add_column_if_missing(conn, 'users', 'avatar_seed', 'ALTER TABLE users ADD COLUMN avatar_seed TEXT')
    add_column_if_missing(conn, 'users', 'language', 'ALTER TABLE users ADD COLUMN language TEXT')

    user_editable_fields = [
        'bio', 'contact_info', 'avatar_style', 'email', 'phone', 'telegram', 'whatsapp', 'viber',
        'last_name', 'first_name', 'middle_name',
        'postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment',
    ]
    for field in user_editable_fields:
        add_column_if_missing(conn, 'users', field, f'ALTER TABLE users ADD COLUMN {field} TEXT')

    add_column_if_missing(
        conn, 'users', 'is_blocked', 'ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0'
    )
    add_column_if_missing(conn, 'users', 'blocked_by', 'ALTER TABLE users ADD COLUMN blocked_by INTEGER')
    add_column_if_missing(
        conn, 'users', 'blocked_reason', 'ALTER TABLE users ADD COLUMN blocked_reason TEXT'
    )
    add_column_if_missing(conn, 'users', 'blocked_at', 'ALTER TABLE users ADD COLUMN blocked_at TIMESTAMP')

    add_column_if_missing(conn, 'awards', 'icon', 'ALTER TABLE awards ADD COLUMN icon TEXT')

    add_column_if_missing(
        conn, 'events', 'award_id', 'ALTER TABLE events ADD COLUMN award_id INTEGER REFERENCES awards(id)'
    )
    add_column_if_missing(conn, 'events', 'deleted_at', 'ALTER TABLE events ADD COLUMN deleted_at TIMESTAMP')
    add_column_if_missing(
        conn, 'events', 'rating_registration', 'ALTER TABLE events ADD COLUMN rating_registration INTEGER'
    )
    add_column_if_missing(
        conn, 'events', 'rating_gift_not_sent', 'ALTER TABLE events ADD COLUMN rating_gift_not_sent INTEGER'
    )
    add_column_if_missing(
        conn, 'events', 'rating_gift_sent', 'ALTER TABLE events ADD COLUMN rating_gift_sent INTEGER'
    )
    add_column_if_missing(
        conn,
        'events',
        'rating_order_coefficient',
        'ALTER TABLE events ADD COLUMN rating_order_coefficient REAL',
    )

    add_column_if_missing(
        conn, 'event_registration_details', 'email', 'ALTER TABLE event_registration_details ADD COLUMN email TEXT'
    )
    add_column_if_missing(
        conn, 'event_registration_details', 'bio', 'ALTER TABLE event_registration_details ADD COLUMN bio TEXT'
    )

    add_column_if_missing(
        conn,
        'snowflake_events',
        'points',
        'ALTER TABLE snowflake_events ADD COLUMN points INTEGER NOT NULL DEFAULT 1',
    )

    try:
        c.execute('PRAGMA table_info(snowflake_events)')
        columns = c.fetchall()
        points_col = next((col for col in columns if col[1] == 'points'), None)
        if points_col and points_col[2].upper() == 'INTEGER':
            log_debug('Migrating points column from INTEGER to REAL')
            c.execute('''
                CREATE TABLE snowflake_events_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    points REAL NOT NULL DEFAULT 1,
                    active INTEGER DEFAULT 1,
                    manual_revoked INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    revoked_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    UNIQUE(user_id, source)
                )
            ''')
            c.execute('''
                INSERT INTO snowflake_events_new
                SELECT id, user_id, source, reason, CAST(points AS REAL) as points,
                       active, manual_revoked, created_at, updated_at, revoked_at
                FROM snowflake_events
            ''')
            c.execute('DROP TABLE snowflake_events')
            c.execute('ALTER TABLE snowflake_events_new RENAME TO snowflake_events')
            log_debug('Points column migration completed successfully')
    except sqlite3.OperationalError as exc:
        log_debug(f'Points column migration skipped: {exc}')

    add_column_if_missing(
        conn,
        'snowflake_events',
        'manual_revoked',
        'ALTER TABLE snowflake_events ADD COLUMN manual_revoked INTEGER DEFAULT 0',
    )

    for index_sql in (
        'CREATE INDEX IF NOT EXISTS idx_snowflake_events_user_id ON snowflake_events(user_id)',
        'CREATE INDEX IF NOT EXISTS idx_snowflake_events_active ON snowflake_events(active)',
        'CREATE INDEX IF NOT EXISTS idx_snowflake_events_manual_revoked ON snowflake_events(manual_revoked)',
        'CREATE INDEX IF NOT EXISTS idx_snowflake_events_rating ON snowflake_events(active, manual_revoked, user_id)',
    ):
        try:
            c.execute(index_sql)
        except sqlite3.OperationalError:
            pass

    add_column_if_missing(
        conn,
        'snowflake_events',
        'updated_at',
        'ALTER TABLE snowflake_events ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
    )
    add_column_if_missing(
        conn, 'snowflake_events', 'revoked_at', 'ALTER TABLE snowflake_events ADD COLUMN revoked_at TIMESTAMP'
    )

    add_column_if_missing(
        conn, 'event_assignments', 'santa_sent_at', 'ALTER TABLE event_assignments ADD COLUMN santa_sent_at TIMESTAMP'
    )
    add_column_if_missing(
        conn, 'event_assignments', 'santa_send_info', 'ALTER TABLE event_assignments ADD COLUMN santa_send_info TEXT'
    )
    add_column_if_missing(
        conn,
        'event_assignments',
        'recipient_received_at',
        'ALTER TABLE event_assignments ADD COLUMN recipient_received_at TIMESTAMP',
    )
    add_column_if_missing(
        conn, 'event_assignments', 'locked', 'ALTER TABLE event_assignments ADD COLUMN locked INTEGER DEFAULT 0'
    )
    add_column_if_missing(
        conn,
        'event_assignments',
        'assignment_locked',
        'ALTER TABLE event_assignments ADD COLUMN assignment_locked INTEGER DEFAULT 0',
    )
    add_column_if_missing(
        conn,
        'event_assignments',
        'recipient_thanks_message',
        'ALTER TABLE event_assignments ADD COLUMN recipient_thanks_message TEXT',
    )
    add_column_if_missing(
        conn,
        'event_assignments',
        'recipient_receipt_image',
        'ALTER TABLE event_assignments ADD COLUMN recipient_receipt_image TEXT',
    )

    add_column_if_missing(
        conn,
        'letter_messages',
        'attachment_path',
        'ALTER TABLE letter_messages ADD COLUMN attachment_path TEXT',
    )

    add_column_if_missing(
        conn,
        'event_assignments',
        'is_archived',
        'ALTER TABLE event_assignments ADD COLUMN is_archived INTEGER DEFAULT 0',
    )

    add_column_if_missing(
        conn,
        'user_admin_comments',
        'is_admin_only',
        'ALTER TABLE user_admin_comments ADD COLUMN is_admin_only INTEGER DEFAULT 0',
    )
    add_column_if_missing(
        conn,
        'user_admin_comments',
        'is_thanks_from_recipient',
        'ALTER TABLE user_admin_comments ADD COLUMN is_thanks_from_recipient INTEGER DEFAULT 0',
    )
    add_column_if_missing(
        conn,
        'user_admin_comments',
        'assignment_id',
        'ALTER TABLE user_admin_comments ADD COLUMN assignment_id INTEGER',
    )
