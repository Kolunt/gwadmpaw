"""Tests for broadcast queue processing."""

import json
import sqlite3
from unittest.mock import patch

from gwadm.services.broadcast_queue import enqueue_broadcast, process_pending_broadcasts


def _create_queue_schema(conn):
    conn.executescript('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT,
            telegram TEXT,
            level INTEGER,
            synd TEXT,
            phone TEXT,
            first_name TEXT,
            last_name TEXT,
            city TEXT,
            country TEXT,
            is_blocked INTEGER DEFAULT 0
        );
        CREATE TABLE broadcast_queue (
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
            finished_at TIMESTAMP
        );
        CREATE TABLE broadcasts_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_by INTEGER NOT NULL,
            created_by_username TEXT,
            recipient_type TEXT NOT NULL,
            delivery_method TEXT NOT NULL,
            subject TEXT,
            message TEXT NOT NULL,
            total_recipients INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            errors TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            action TEXT NOT NULL,
            details TEXT,
            metadata TEXT,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO users (user_id, username, email, telegram, is_blocked)
        VALUES (1, 'alice', 'a@example.com', '@alice', 0),
               (2, 'bob', 'b@example.com', '@bob', 0);
    ''')
    conn.commit()


def _conn_factory(db_path):
    def _make_conn():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    return _make_conn


@patch('gwadm.services.broadcast_queue.get_db_connection')
@patch('gwadm.services.broadcast_queue.send_email_via_smtp', return_value=(True, 'ok'))
def test_enqueue_and_process_email_broadcast(mock_send, mock_get_conn, tmp_path):
    db_path = tmp_path / 'broadcast.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _create_queue_schema(conn)
    conn.close()
    mock_get_conn.side_effect = _conn_factory(db_path)

    queue_id = enqueue_broadcast(
        created_by=1,
        created_by_username='admin',
        recipient_type='selected',
        delivery_method='email',
        subject='Hello',
        message='Hi [name]',
        recipient_user_ids=[1, 2],
        total_recipients=2,
    )
    assert queue_id == 1

    processed = process_pending_broadcasts(limit_jobs=1)
    assert processed == 1
    verify = sqlite3.connect(db_path)
    verify.row_factory = sqlite3.Row
    job = verify.execute('SELECT status, success_count FROM broadcast_queue WHERE id = 1').fetchone()
    assert job['status'] == 'completed'
    assert job['success_count'] == 2
    history = verify.execute('SELECT COUNT(*) AS c FROM broadcasts_history').fetchone()['c']
    assert history == 1
    assert mock_send.call_count == 2
    verify.close()


@patch('gwadm.services.broadcast_queue.get_db_connection')
def test_enqueue_stores_recipient_ids(mock_get_conn, tmp_path):
    db_path = tmp_path / 'broadcast_enqueue.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _create_queue_schema(conn)
    conn.close()
    mock_get_conn.side_effect = _conn_factory(db_path)

    queue_id = enqueue_broadcast(
        created_by=1,
        created_by_username='admin',
        recipient_type='selected',
        delivery_method='telegram',
        subject='',
        message='Ping',
        recipient_user_ids=[2],
        total_recipients=1,
    )
    assert queue_id == 1
    verify = sqlite3.connect(str(db_path))
    verify.row_factory = sqlite3.Row
    row = verify.execute(
        'SELECT recipient_user_ids, status FROM broadcast_queue WHERE id = ?',
        (queue_id,),
    ).fetchone()
    assert row['status'] == 'pending'
    assert json.loads(row['recipient_user_ids']) == [2]
    verify.close()
