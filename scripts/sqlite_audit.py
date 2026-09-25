#!/usr/bin/env python3
"""SQLite audit script for systemd timer."""

import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from gwadm.db import ensure_db
from gwadm.services.sqlite_audit import run_sqlite_audit

if __name__ == '__main__':
    ensure_db()
    lines, has_warn = run_sqlite_audit()
    for line in lines:
        print(line)
    sys.exit(2 if has_warn else 0)
