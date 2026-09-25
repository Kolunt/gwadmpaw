"""SQLite health audit for production monitoring."""

import os

from gwadm.config import DATABASE_PATH
from gwadm.db import get_db_connection
from gwadm.services.rating import _RATING_AGGREGATION_FROM

GUNICORN_WORKERS_RECOMMENDED = 2


def _explain(conn, label, query):
    lines = []
    try:
        plan = conn.execute(f'EXPLAIN QUERY PLAN {query}').fetchall()
        summary = '; '.join(row[3] for row in plan if len(row) > 3)
        lines.append(f'INFO: {label} plan: {summary or "n/a"}')
    except Exception as exc:
        lines.append(f'WARN: {label} explain failed: {exc}')
    return lines


def run_sqlite_audit():
    """Run SQLite audit. Returns list of LEVEL: message lines."""
    lines = []
    has_warn = False

    if not os.path.exists(DATABASE_PATH):
        lines.append(f'CRITICAL: database not found: {DATABASE_PATH}')
        return lines, True

    size_mb = os.path.getsize(DATABASE_PATH) // (1024 * 1024)
    lines.append(f'INFO: database size: {size_mb} MB ({DATABASE_PATH})')

    conn = get_db_connection()
    try:
        journal_mode = conn.execute('PRAGMA journal_mode').fetchone()[0]
        page_count = conn.execute('PRAGMA page_count').fetchone()[0]
        lines.append(f'INFO: journal_mode={journal_mode}, page_count={page_count}')

        cache_query = '''
            SELECT user_id, username, total_points
            FROM user_rating_cache
            ORDER BY total_points DESC, LOWER(username) ASC
            LIMIT 50 OFFSET 0
        '''
        live_query = f'''
            SELECT u.user_id, u.username,
                   COALESCE(SUM(CAST(se.points AS REAL)), 0.0) AS total_points
            {_RATING_AGGREGATION_FROM}
            ORDER BY total_points DESC, LOWER(u.username) ASC
            LIMIT 50 OFFSET 0
        '''
        lines.extend(_explain(conn, 'rating_cache', cache_query))
        lines.extend(_explain(conn, 'rating_live', live_query))

        indexes = conn.execute(
            '''
            SELECT name, tbl_name FROM sqlite_master
            WHERE type = 'index' AND tbl_name IN ('snowflake_events', 'users', 'user_rating_cache')
            ORDER BY tbl_name, name
            '''
        ).fetchall()
        for row in indexes:
            lines.append(f'INFO: index {row["tbl_name"]}.{row["name"]}')

        cache_count = conn.execute('SELECT COUNT(*) AS c FROM user_rating_cache').fetchone()['c']
        lines.append(f'INFO: user_rating_cache rows: {cache_count}')
        if cache_count == 0:
            lines.append('WARN: rating cache is empty; run rebuild_rating_cache.py')
            has_warn = True

        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM broadcast_queue WHERE status IN ('pending', 'processing')"
        ).fetchone()['c']
        lines.append(f'INFO: broadcast queue active jobs: {pending}')

        lines.append(
            f'INFO: gunicorn workers recommended: {GUNICORN_WORKERS_RECOMMENDED} '
            '(see deploy/gwadm-user.service)'
        )
    finally:
        conn.close()

    return lines, has_warn
