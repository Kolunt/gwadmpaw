"""Avatar cache cleanup helpers."""

import os
import time

from gwadm.config import AVATAR_CACHE_DIR
from gwadm.logging_config import log_debug
from gwadm.services.avatars import get_avatar_cache_path

DEFAULT_MAX_MB = 500
DEFAULT_MAX_AGE_DAYS = 30
COMMON_SIZES = (40, 128)


def _dir_size_mb(directory):
    total = 0
    for root, _dirs, files in os.walk(directory):
        for name in files:
            path = os.path.join(root, name)
            try:
                total += os.path.getsize(path)
            except OSError:
                continue
    return total // (1024 * 1024)


def _expected_cache_paths(conn):
    rows = conn.execute(
        'SELECT avatar_seed, avatar_style FROM users WHERE avatar_seed IS NOT NULL'
    ).fetchall()
    expected = set()
    for row in rows:
        style = row['avatar_style']
        for size in COMMON_SIZES:
            path, _fmt = get_avatar_cache_path(row['avatar_seed'], style, size)
            expected.add(os.path.normpath(path))
    return expected


def cleanup_avatar_cache(conn, max_mb=None, max_age_days=None, dry_run=False):
    """Remove stale avatar cache files. Returns dict with stats."""
    max_mb = max_mb if max_mb is not None else int(
        os.environ.get('AVATAR_CACHE_MAX_MB', DEFAULT_MAX_MB)
    )
    max_age_days = max_age_days if max_age_days is not None else int(
        os.environ.get('AVATAR_CACHE_MAX_AGE_DAYS', DEFAULT_MAX_AGE_DAYS)
    )
    cache_dir = AVATAR_CACHE_DIR
    if not os.path.isdir(cache_dir):
        return {'deleted': 0, 'size_mb_before': 0, 'size_mb_after': 0, 'dry_run': dry_run}

    expected = _expected_cache_paths(conn)
    size_before = _dir_size_mb(cache_dir)
    deleted = 0
    cutoff = time.time() - (max_age_days * 86400)

    def _iter_files():
        for name in os.listdir(cache_dir):
            path = os.path.join(cache_dir, name)
            if os.path.isfile(path):
                yield path

    for path in _iter_files():
        norm = os.path.normpath(path)
        if norm in expected:
            continue
        try:
            if os.path.getmtime(path) >= cutoff:
                continue
        except OSError:
            continue
        if dry_run:
            deleted += 1
            continue
        try:
            os.remove(path)
            deleted += 1
        except OSError:
            continue

    size_after = _dir_size_mb(cache_dir)
    if size_after > max_mb:
        candidates = []
        for path in _iter_files():
            norm = os.path.normpath(path)
            if norm in expected:
                continue
            try:
                candidates.append((os.path.getmtime(path), path))
            except OSError:
                continue
        candidates.sort()
        for _mtime, path in candidates:
            if size_after <= max_mb:
                break
            if dry_run:
                deleted += 1
                size_after = max(0, size_after - 1)
                continue
            try:
                file_mb = max(1, os.path.getsize(path) // (1024 * 1024))
                os.remove(path)
                deleted += 1
                size_after = max(0, size_after - file_mb)
            except OSError:
                continue

    if not dry_run:
        log_debug(
            f'Avatar cache cleanup: deleted={deleted}, '
            f'size {size_before}MB -> {_dir_size_mb(cache_dir)}MB'
        )
    return {
        'deleted': deleted,
        'size_mb_before': size_before,
        'size_mb_after': _dir_size_mb(cache_dir) if not dry_run else size_before,
        'dry_run': dry_run,
    }
