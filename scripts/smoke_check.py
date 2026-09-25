#!/usr/bin/env python3
"""Minimal smoke tests before deploy (R-001)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    errors = []

    try:
        import app as app_module
        flask_app = app_module.app
    except Exception as exc:
        print(f"FAIL: import app — {exc}")
        return 1

    from gwadm.config import is_production
    if not callable(is_production):
        errors.append("is_production is not callable")

    from gwadm.db import get_db, get_db_connection, is_database_initialized

    if not is_database_initialized():
        errors.append("database should be initialized after import app")

    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        errors.append(f"get_db context manager failed: {exc}")

    try:
        conn = get_db_connection()
        conn.execute("SELECT 1")
        conn.close()
    except Exception as exc:
        errors.append(f"get_db_connection failed: {exc}")

    client = flask_app.test_client()

    response = client.get("/")
    if response.status_code != 200:
        errors.append(f"GET / expected 200, got {response.status_code}")

    response = client.get("/login")
    if response.status_code not in (302, 303):
        errors.append(f"GET /login expected redirect, got {response.status_code}")
    else:
        location = response.headers.get("Location", "")
        if "gwars.io" not in location:
            errors.append(f"GET /login Location missing gwars.io: {location}")

    response = client.get("/avatars/image?seed=test&style=avataaars&size=40")
    if response.status_code not in (200, 302):
        errors.append(
            f"GET /avatars/image expected 200 or 302, got {response.status_code}"
        )
    elif response.status_code == 200:
        content_type = response.headers.get("Content-Type", "")
        if not content_type.startswith(("image/png", "image/svg+xml")):
            errors.append(f"GET /avatars/image unexpected Content-Type: {content_type}")

    if errors:
        for err in errors:
            print(f"FAIL: {err}")
        return 1

    print("OK: smoke_check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
