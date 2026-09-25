#!/usr/bin/env python3
"""Process pending broadcast queue jobs (systemd timer)."""

import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from gwadm.db import ensure_db
from gwadm.services.broadcast_queue import process_pending_broadcasts

if __name__ == '__main__':
    ensure_db()
    processed = process_pending_broadcasts(limit_jobs=1)
    print(f'INFO: processed broadcast jobs: {processed}')
    sys.exit(0)
