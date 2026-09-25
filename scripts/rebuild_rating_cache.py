#!/usr/bin/env python3
"""Rebuild rating cache and run pending snowflake recalc (systemd timer)."""

import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_path not in sys.path:
    sys.path.insert(0, project_path)

from gwadm.db import ensure_db
from gwadm.services.rating import run_rating_maintenance

if __name__ == '__main__':
    ensure_db()
    sys.exit(0 if run_rating_maintenance() else 1)
