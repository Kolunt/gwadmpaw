#!/usr/bin/env python3
"""Run SQLite backup (for systemd timer or manual invoke)."""

import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from cron_tasks import backup_database

if __name__ == '__main__':
    sys.exit(0 if backup_database() else 1)
