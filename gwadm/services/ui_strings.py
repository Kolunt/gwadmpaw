"""Load and cache UI strings from SQLite; seed from registry."""

from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any

from gwadm.logging_config import log_debug, log_error
from gwadm.ui_string_registry import UI_STRING_SEEDS

_CACHE_LOCK = threading.Lock()
_SEED_LOCK = threading.Lock()
_MSG_CACHE: dict[tuple[str, str], str] | None = None
_CACHE_BUILT_AT: float | None = None
# True when we know ui_strings has at least UI_STRING_SEEDS RU rows (see ensure_ui_strings_seeded).
_SEED_SATISFIED_FOR_EXPECTED: bool = False
_SEED_EXPECTED_N: int = -1

_LOCALE_FALLBACK = 'ru'
_EXPECTED_RU_ROWS = len(UI_STRING_SEEDS)


def invalidate_ui_strings_cache() -> None:
    global _MSG_CACHE, _CACHE_BUILT_AT, _SEED_SATISFIED_FOR_EXPECTED, _SEED_EXPECTED_N
    with _CACHE_LOCK:
        _MSG_CACHE = None
        _CACHE_BUILT_AT = None
        _SEED_SATISFIED_FOR_EXPECTED = False
        _SEED_EXPECTED_N = -1


def _load_cache_from_db(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    rows = conn.execute(
        'SELECT msg_key, locale, value FROM ui_strings'
    ).fetchall()
    out: dict[tuple[str, str], str] = {}
    for row in rows:
        out[(row['msg_key'], row['locale'])] = row['value']
    return out


def ensure_ui_strings_seeded(conn: sqlite3.Connection) -> None:
    """Insert missing rows from UI_STRING_SEEDS."""
    global _SEED_SATISFIED_FOR_EXPECTED, _SEED_EXPECTED_N
    if _SEED_SATISFIED_FOR_EXPECTED and _SEED_EXPECTED_N == _EXPECTED_RU_ROWS:
        return

    with _SEED_LOCK:
        if _SEED_SATISFIED_FOR_EXPECTED and _SEED_EXPECTED_N == _EXPECTED_RU_ROWS:
            return

        row = conn.execute(
            "SELECT COUNT(*) FROM ui_strings WHERE locale = 'ru'"
        ).fetchone()
        ru_count = int(row[0]) if row else 0
        if ru_count >= _EXPECTED_RU_ROWS:
            _SEED_SATISFIED_FOR_EXPECTED = True
            _SEED_EXPECTED_N = _EXPECTED_RU_ROWS
            return

        now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        for msg_key, category, ru_val, en_val in UI_STRING_SEEDS:
            conn.execute(
                '''
                INSERT OR IGNORE INTO ui_strings
                (msg_key, category, locale, default_text, value, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                ''',
                (msg_key, category, 'ru', ru_val, ru_val, now),
            )
            conn.execute(
                '''
                INSERT OR IGNORE INTO ui_strings
                (msg_key, category, locale, default_text, value, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                ''',
                (msg_key, category, 'en', ru_val, en_val, now),
            )

        _SEED_SATISFIED_FOR_EXPECTED = True
        _SEED_EXPECTED_N = _EXPECTED_RU_ROWS


def _get_cache(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    global _MSG_CACHE, _CACHE_BUILT_AT
    with _CACHE_LOCK:
        if _MSG_CACHE is None:
            ensure_ui_strings_seeded(conn)
            _MSG_CACHE = _load_cache_from_db(conn)
            _CACHE_BUILT_AT = time.monotonic()
            log_debug('ui_strings cache built')
        return _MSG_CACHE


def get_string(
    msg_key: str,
    locale: str,
    conn_factory: Any,
    static_fallback_ru: dict[str, str],
    static_fallback_en: dict[str, str],
) -> str:
    """Resolve msg_key for locale using DB cache; fallback to static dicts then key."""
    loc = locale if locale in ('ru', 'en') else _LOCALE_FALLBACK

    # Hot path: read-filled dict without taking the global lock (GIL + assignment atomicity).
    cache_snapshot = _MSG_CACHE
    if cache_snapshot is not None:
        hit = cache_snapshot.get((msg_key, loc))
        if hit is not None:
            return hit
        if loc != _LOCALE_FALLBACK:
            hit_fb = cache_snapshot.get((msg_key, _LOCALE_FALLBACK))
            if hit_fb is not None:
                return hit_fb
        # Full cache already loaded but key missing — do not open SQLite again.
        if loc == 'ru':
            return static_fallback_ru.get(msg_key, msg_key)
        return static_fallback_en.get(msg_key, msg_key)

    try:
        conn = conn_factory()
        try:
            cache = _get_cache(conn)
            hit = cache.get((msg_key, loc))
            if hit is not None:
                return hit
            if loc != _LOCALE_FALLBACK:
                hit_fb = cache.get((msg_key, _LOCALE_FALLBACK))
                if hit_fb is not None:
                    return hit_fb
        finally:
            conn.close()
    except Exception as e:
        log_error(f'ui_strings get_string: {e}')
    if loc == 'ru':
        return static_fallback_ru.get(msg_key, msg_key)
    return static_fallback_en.get(msg_key, msg_key)


def list_strings_for_admin(
    conn: sqlite3.Connection,
    *,
    category: str | None = None,
    q: str | None = None,
    limit: int = 500,
    offset: int = 0,
) -> list[sqlite3.Row]:
    """Rows: msg_key, category, default_ru, ru_value, en_value."""
    sql = '''
        SELECT
            r.msg_key AS msg_key,
            r.category AS category,
            r.default_text AS default_ru,
            r.value AS ru_value,
            e.value AS en_value
        FROM ui_strings r
        JOIN ui_strings e ON e.msg_key = r.msg_key AND e.locale = 'en'
        WHERE r.locale = 'ru'
    '''
    args: list[Any] = []
    if category:
        sql += ' AND r.category = ?'
        args.append(category)
    if q:
        term = f'%{q.strip()}%'
        sql += ''' AND (
            r.msg_key LIKE ? OR r.value LIKE ? OR e.value LIKE ?
            OR r.default_text LIKE ?
        )'''
        args.extend([term, term, term, term])
    sql += ' ORDER BY r.category, r.msg_key LIMIT ? OFFSET ?'
    args.extend([limit, offset])
    return conn.execute(sql, args).fetchall()


def list_categories(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        'SELECT DISTINCT category FROM ui_strings ORDER BY category'
    ).fetchall()
    return [str(r[0]) for r in rows]


def apply_bulk_updates(
    conn: sqlite3.Connection,
    updates: list[tuple[str, str, str]],
    *,
    editor_id: int | None,
) -> int:
    """
    updates: list of (msg_key, locale, value)
    Returns count of rows changed.
    """
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    count = 0
    for msg_key, locale, value in updates:
        if locale not in ('ru', 'en'):
            continue
        cur = conn.execute(
            '''
            UPDATE ui_strings
            SET value = ?, updated_at = ?, updated_by = ?
            WHERE msg_key = ? AND locale = ?
            ''',
            (value, now, editor_id, msg_key, locale),
        )
        count += cur.rowcount
    return count


def count_strings(conn: sqlite3.Connection, category: str | None, q: str | None) -> int:
    sql = '''
        SELECT COUNT(*) FROM ui_strings r
        WHERE r.locale = 'ru'
    '''
    args: list[Any] = []
    if category:
        sql += ' AND r.category = ?'
        args.append(category)
    if q:
        term = f'%{q.strip()}%'
        sql += ''' AND (
            r.msg_key LIKE ? OR r.value LIKE ? OR r.default_text LIKE ? OR EXISTS (
                SELECT 1 FROM ui_strings e
                WHERE e.msg_key = r.msg_key AND e.locale = 'en'
                AND e.value LIKE ?
            )
        )'''
        args.extend([term, term, term, term])
    row = conn.execute(sql, args).fetchone()
    return int(row[0]) if row else 0
