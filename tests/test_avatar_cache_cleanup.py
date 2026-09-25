"""Tests for avatar cache cleanup."""

import os
import sqlite3
import time
from unittest.mock import patch

from gwadm.services.avatar_cache_cleanup import cleanup_avatar_cache


def test_cleanup_removes_stale_unexpected_files(tmp_path):
    cache_dir = tmp_path / 'cache'
    cache_dir.mkdir()
    stale = cache_dir / 'stale.png'
    stale.write_bytes(b'x' * 1024)
    old_mtime = time.time() - (40 * 86400)
    os.utime(stale, (old_mtime, old_mtime))

    db_path = tmp_path / 'db.sqlite'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        'CREATE TABLE users (user_id INTEGER PRIMARY KEY, avatar_seed TEXT, avatar_style TEXT)'
    )
    conn.commit()

    with patch('gwadm.services.avatar_cache_cleanup.AVATAR_CACHE_DIR', str(cache_dir)):
        stats = cleanup_avatar_cache(conn, max_mb=1, max_age_days=30, dry_run=False)

    assert stats['deleted'] == 1
    assert not stale.exists()
    conn.close()
