"""Apply versioned SQLite migrations from migrations/."""

import importlib.util
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from gwadm.config import ROOT_DIR
from gwadm.logging_config import log_debug, log_error

CURRENT_VERSION = 4
MIGRATIONS_DIR = ROOT_DIR / 'migrations'


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info({table})').fetchall()
    return any(row[1] == column for row in rows)


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if not column_exists(conn, table, column):
        conn.execute(ddl)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _get_current_version(conn: sqlite3.Connection) -> int:
    if not _table_exists(conn, 'schema_version'):
        return 0
    row = conn.execute('SELECT MAX(version) FROM schema_version').fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def _stamp_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        'INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?, ?)',
        (version, datetime.now().isoformat()),
    )


def _discover_migrations() -> list[tuple[int, Path]]:
    migrations: list[tuple[int, Path]] = []
    for path in sorted(MIGRATIONS_DIR.iterdir()):
        match = re.match(r'^(\d{3})_.+\.(sql|py)$', path.name)
        if match:
            migrations.append((int(match.group(1)), path))
    return sorted(migrations, key=lambda item: item[0])


def _apply_sql_migration(conn: sqlite3.Connection, path: Path) -> None:
    sql = path.read_text(encoding='utf-8')
    conn.executescript(sql)


def _apply_python_migration(conn: sqlite3.Connection, path: Path) -> None:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Cannot load migration module: {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    upgrade = getattr(module, 'upgrade', None)
    if not callable(upgrade):
        raise RuntimeError(f'Migration {path.name} must define upgrade(conn)')
    upgrade(conn)


def _apply_migration(conn: sqlite3.Connection, version: int, path: Path) -> None:
    log_debug(f'Applying migration {path.name}')
    if path.suffix == '.sql':
        _apply_sql_migration(conn, path)
    elif path.suffix == '.py':
        _apply_python_migration(conn, path)
    else:
        raise RuntimeError(f'Unsupported migration file: {path}')
    _stamp_version(conn, version)


def run_migrations(conn: sqlite3.Connection) -> None:
    """Run pending migrations; bootstrap legacy databases without re-applying alters."""
    current = _get_current_version(conn)
    if current == 0 and _table_exists(conn, 'users'):
        schema_path = MIGRATIONS_DIR / '001_schema_version.sql'
        if schema_path.exists() and not _table_exists(conn, 'schema_version'):
            _apply_sql_migration(conn, schema_path)
        log_debug(
            f'Legacy database detected; stamping schema_version to {CURRENT_VERSION}'
        )
        _stamp_version(conn, CURRENT_VERSION)
        return

    for version, path in _discover_migrations():
        if version <= current:
            continue
        if version > CURRENT_VERSION:
            log_error(f'Migration {path.name} exceeds CURRENT_VERSION={CURRENT_VERSION}')
            break
        try:
            _apply_migration(conn, version, path)
        except Exception as exc:
            log_error(f'Migration {path.name} failed: {exc}')
            raise
