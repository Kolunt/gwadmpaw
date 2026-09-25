"""Broadcast queue for async delivery via systemd timer."""

import json
from datetime import datetime, timedelta

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.activity import log_activity
from gwadm.services.broadcast_helpers import replace_broadcast_placeholders
from gwadm.services.telegram import send_email_via_smtp, send_telegram_message

BATCH_SIZE = 50
STUCK_MINUTES = 30

_RECIPIENT_FIELDS = '''
    user_id, username, email, level, synd, phone, telegram,
    first_name, last_name, city, country
'''


def _parse_errors(raw):
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else [str(parsed)]
    except (json.JSONDecodeError, TypeError):
        return [str(raw)]


def _load_recipients(conn, job):
    delivery_method = job['delivery_method']
    recipient_type = job['recipient_type']
    if recipient_type == 'all':
        if delivery_method == 'email':
            return conn.execute(
                f'''
                SELECT {_RECIPIENT_FIELDS}
                FROM users
                WHERE email IS NOT NULL AND email != '' AND is_blocked = 0
                ORDER BY user_id
                '''
            ).fetchall()
        return conn.execute(
            f'''
            SELECT {_RECIPIENT_FIELDS}
            FROM users
            WHERE telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
            ORDER BY user_id
            '''
        ).fetchall()

    user_ids = json.loads(job['recipient_user_ids'] or '[]')
    if not user_ids:
        return []
    placeholders = ','.join(['?'] * len(user_ids))
    if delivery_method == 'email':
        return conn.execute(
            f'''
            SELECT {_RECIPIENT_FIELDS}
            FROM users
            WHERE user_id IN ({placeholders})
              AND email IS NOT NULL AND email != '' AND is_blocked = 0
            ORDER BY user_id
            ''',
            user_ids,
        ).fetchall()
    return conn.execute(
        f'''
        SELECT {_RECIPIENT_FIELDS}
        FROM users
        WHERE user_id IN ({placeholders})
          AND telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
        ORDER BY user_id
        ''',
        user_ids,
    ).fetchall()


def enqueue_broadcast(
    created_by,
    created_by_username,
    recipient_type,
    delivery_method,
    subject,
    message,
    recipient_user_ids=None,
    total_recipients=0,
):
    conn = get_db_connection()
    try:
        recipient_ids_json = None
        if recipient_user_ids:
            recipient_ids_json = json.dumps(recipient_user_ids)
        cursor = conn.execute(
            '''
            INSERT INTO broadcast_queue (
                status, created_by, created_by_username, recipient_type,
                delivery_method, subject, message, recipient_user_ids,
                total_recipients
            ) VALUES ('pending', ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                created_by,
                created_by_username,
                recipient_type,
                delivery_method,
                subject if delivery_method == 'email' else None,
                message,
                recipient_ids_json,
                total_recipients,
            ),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def reset_stuck_broadcast_jobs(conn) -> int:
    cutoff = (datetime.now() - timedelta(minutes=STUCK_MINUTES)).isoformat(sep=' ', timespec='seconds')
    cursor = conn.execute(
        '''
        UPDATE broadcast_queue
        SET status = 'pending'
        WHERE status = 'processing'
          AND started_at IS NOT NULL
          AND started_at < ?
        ''',
        (cutoff,),
    )
    return cursor.rowcount


def get_active_queue_jobs(conn):
    return conn.execute(
        '''
        SELECT id, status, delivery_method, total_recipients, processed_count,
               success_count, error_count, created_at, started_at
        FROM broadcast_queue
        WHERE status IN ('pending', 'processing')
        ORDER BY created_at ASC
        '''
    ).fetchall()


def _finalize_job(conn, job, errors):
    errors_json = json.dumps(errors, ensure_ascii=False) if errors else None
    conn.execute(
        '''
        INSERT INTO broadcasts_history (
            created_by, created_by_username, recipient_type, delivery_method,
            subject, message, total_recipients, success_count, error_count, errors
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            job['created_by'],
            job['created_by_username'],
            job['recipient_type'],
            job['delivery_method'],
            job['subject'],
            job['message'],
            job['total_recipients'],
            job['success_count'],
            job['error_count'],
            errors_json,
        ),
    )
    conn.execute(
        '''
        UPDATE broadcast_queue
        SET status = 'completed', finished_at = CURRENT_TIMESTAMP, errors = ?
        WHERE id = ?
        ''',
        (errors_json, job['id']),
    )
    log_activity(
        'broadcast_completed',
        details=(
            f'Рассылка #{job["id"]} завершена: успешно {job["success_count"]}, '
            f'ошибок {job["error_count"]}'
        ),
        metadata={
            'queue_id': job['id'],
            'delivery_method': job['delivery_method'],
            'success_count': job['success_count'],
            'error_count': job['error_count'],
            'total_recipients': job['total_recipients'],
        },
        user_id=job['created_by'],
        username=job['created_by_username'],
    )


def _send_to_recipient(job, recipient):
    message = job['message']
    subject = job['subject'] or ''
    delivery_method = job['delivery_method']
    personalized_message = replace_broadcast_placeholders(message, recipient)
    personalized_subject = replace_broadcast_placeholders(subject, recipient)

    if delivery_method == 'email':
        return send_email_via_smtp(
            to_email=recipient['email'],
            subject=personalized_subject,
            body=personalized_message,
        )
    return send_telegram_message(
        message=personalized_message,
        chat_id=recipient['telegram'],
    )


def process_pending_broadcasts(limit_jobs=1) -> int:
    """Process queued broadcasts in batches. Returns number of jobs touched."""
    conn = get_db_connection()
    processed_jobs = 0
    try:
        reset_stuck_broadcast_jobs(conn)
        conn.commit()

        for _ in range(limit_jobs):
            job = conn.execute(
                '''
                SELECT * FROM broadcast_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                '''
            ).fetchone()
            if not job:
                break

            job = dict(job)
            job['success_count'] = job.get('success_count') or 0
            job['error_count'] = job.get('error_count') or 0
            conn.execute(
                '''
                UPDATE broadcast_queue
                SET status = 'processing',
                    started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
                WHERE id = ?
                ''',
                (job['id'],),
            )
            conn.commit()

            recipients = _load_recipients(conn, job)
            if job['total_recipients'] == 0:
                job['total_recipients'] = len(recipients)
                conn.execute(
                    'UPDATE broadcast_queue SET total_recipients = ? WHERE id = ?',
                    (job['total_recipients'], job['id']),
                )
                conn.commit()

            errors = _parse_errors(job.get('errors'))
            start_index = job.get('processed_count') or 0
            end_index = min(start_index + BATCH_SIZE, len(recipients))

            for recipient in recipients[start_index:end_index]:
                recipient = dict(recipient)
                try:
                    success, result_message = _send_to_recipient(job, recipient)
                    if success:
                        job['success_count'] += 1
                        log_activity(
                            'broadcast_sent',
                            details=(
                                f'Рассылка #{job["id"]} → {recipient["username"]} '
                                f'через {job["delivery_method"]}'
                            ),
                            metadata={
                                'queue_id': job['id'],
                                'recipient_id': recipient['user_id'],
                                'delivery_method': job['delivery_method'],
                            },
                            user_id=job['created_by'],
                            username=job['created_by_username'],
                        )
                    else:
                        job['error_count'] += 1
                        errors.append(f"{recipient['username']}: {result_message}")
                except Exception as exc:
                    job['error_count'] += 1
                    errors.append(f"{recipient['username']}: {exc}")
                    log_error(f"Broadcast queue {job['id']} error for {recipient['username']}: {exc}")

            job['processed_count'] = end_index
            errors_json = json.dumps(errors, ensure_ascii=False) if errors else None
            conn.execute(
                '''
                UPDATE broadcast_queue
                SET processed_count = ?, success_count = ?, error_count = ?, errors = ?
                WHERE id = ?
                ''',
                (
                    job['processed_count'],
                    job['success_count'],
                    job['error_count'],
                    errors_json,
                    job['id'],
                ),
            )
            conn.commit()

            if job['processed_count'] >= len(recipients):
                _finalize_job(conn, job, errors)
                conn.commit()
            processed_jobs += 1
            log_debug(
                f'Broadcast queue #{job["id"]}: {job["processed_count"]}/{len(recipients)} processed'
            )
        return processed_jobs
    except Exception as exc:
        log_error(f'process_pending_broadcasts failed: {exc}')
        conn.rollback()
        return processed_jobs
    finally:
        conn.close()
