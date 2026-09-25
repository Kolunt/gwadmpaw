"""Phase 8: rating cache table and broadcast queue."""

import sqlite3


def upgrade(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS user_rating_cache (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            total_points REAL NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_user_rating_cache_points
        ON user_rating_cache(total_points DESC, username COLLATE NOCASE)
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS broadcast_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_by INTEGER NOT NULL,
            created_by_username TEXT,
            recipient_type TEXT NOT NULL,
            delivery_method TEXT NOT NULL,
            subject TEXT,
            message TEXT NOT NULL,
            recipient_user_ids TEXT,
            total_recipients INTEGER DEFAULT 0,
            processed_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            errors TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(user_id)
        )
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_broadcast_queue_status
        ON broadcast_queue(status, created_at)
        '''
    )
    conn.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_users_username_nocase
        ON users(username COLLATE NOCASE)
        '''
    )
