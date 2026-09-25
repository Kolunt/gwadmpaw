#!/usr/bin/env python3
"""Clean up stale avatar cache files (systemd timer)."""

import argparse
import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from gwadm.db import ensure_db, get_db_connection
from gwadm.services.avatar_cache_cleanup import cleanup_avatar_cache

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clean avatar cache directory')
    parser.add_argument('--dry-run', action='store_true', help='Report only, do not delete')
    args = parser.parse_args()

    ensure_db()
    conn = get_db_connection()
    try:
        stats = cleanup_avatar_cache(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    print(
        f"INFO: avatar cache cleanup dry_run={stats['dry_run']} "
        f"deleted={stats['deleted']} "
        f"size_mb={stats['size_mb_before']}->{stats['size_mb_after']}"
    )
    sys.exit(0)
