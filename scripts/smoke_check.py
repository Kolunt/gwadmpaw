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

    for path in ("/participants", "/faq", "/rules", "/rating", "/contacts"):
        response = client.get(path)
        if response.status_code != 200:
            errors.append(f"GET {path} expected 200, got {response.status_code}")

    response = client.get("/dashboard")
    if response.status_code not in (302, 303, 308):
        errors.append(f"GET /dashboard expected redirect, got {response.status_code}")

    response = client.get("/api/avatar/candidates?style=avataaars")
    if response.status_code not in (302, 303, 401):
        errors.append(
            f"GET /api/avatar/candidates expected redirect or 401, got {response.status_code}"
        )

    try:
        conn = get_db_connection()
        user_row = conn.execute("SELECT user_id FROM users LIMIT 1").fetchone()
        conn.close()
        if user_row:
            response = client.get(f"/profile/{user_row['user_id']}")
            if response.status_code != 200:
                errors.append(
                    f"GET /profile/{user_row['user_id']} expected 200, got {response.status_code}"
                )
    except Exception as exc:
        errors.append(f"profile route DB lookup failed: {exc}")

    response = client.get("/login")
    if response.status_code not in (302, 303):
        errors.append(f"GET /login expected redirect, got {response.status_code}")
    else:
        location = response.headers.get("Location", "")
        if "gwars.io" not in location:
            errors.append(f"GET /login Location missing gwars.io: {location}")

    response = client.get("/gwars-required")
    if response.status_code != 200:
        errors.append(f"GET /gwars-required expected 200, got {response.status_code}")
    elif "gwars.io" not in response.get_data(as_text=True):
        errors.append("GET /gwars-required missing gwars.io link")

    response = client.get("/logout")
    if response.status_code not in (302, 303):
        errors.append(f"GET /logout expected redirect, got {response.status_code}")

    response = client.get("/avatars/image?seed=test&style=avataaars&size=40")
    if response.status_code not in (200, 302):
        errors.append(
            f"GET /avatars/image expected 200 or 302, got {response.status_code}"
        )
    elif response.status_code == 200:
        content_type = response.headers.get("Content-Type", "")
        if not content_type.startswith(("image/png", "image/svg+xml")):
            errors.append(f"GET /avatars/image unexpected Content-Type: {content_type}")

    response = client.get("/titles/999999999")
    if response.status_code not in (302, 303):
        errors.append(f"GET /titles/999999999 expected redirect, got {response.status_code}")

    response = client.get("/awards/999999999")
    if response.status_code not in (302, 303):
        errors.append(f"GET /awards/999999999 expected redirect, got {response.status_code}")

    response = client.get("/roles/admin")
    if response.status_code != 200:
        errors.append(f"GET /roles/admin expected 200, got {response.status_code}")

    try:
        conn = get_db_connection()
        title_row = conn.execute("SELECT id FROM titles LIMIT 1").fetchone()
        award_row = conn.execute("SELECT id FROM awards LIMIT 1").fetchone()
        conn.close()
        if title_row:
            response = client.get(f"/titles/{title_row['id']}")
            if response.status_code != 200:
                errors.append(
                    f"GET /titles/{title_row['id']} expected 200, got {response.status_code}"
                )
        if award_row:
            response = client.get(f"/awards/{award_row['id']}")
            if response.status_code != 200:
                errors.append(
                    f"GET /awards/{award_row['id']} expected 200, got {response.status_code}"
                )
    except Exception as exc:
        errors.append(f"meta route DB lookup failed: {exc}")


    response = client.get("/events")
    if response.status_code != 200:
        errors.append(f"GET /events expected 200, got {response.status_code}")

    try:
        conn = get_db_connection()
        event_row = conn.execute(
            "SELECT id FROM events WHERE deleted_at IS NULL LIMIT 1"
        ).fetchone()
        conn.close()
        if event_row:
            eid = event_row["id"]
            response = client.get(f"/events/{eid}")
            if response.status_code != 200:
                errors.append(
                    f"GET /events/{eid} expected 200, got {response.status_code}"
                )
    except Exception as exc:
        errors.append(f"events route DB lookup failed: {exc}")

    for path in ("/assignments", "/letter"):
        response = client.get(path)
        if response.status_code not in (302, 303):
            errors.append(f"GET {path} expected redirect, got {response.status_code}")

    response = client.get("/admin")
    if response.status_code not in (302, 303, 308):
        errors.append(f"GET /admin expected redirect, got {response.status_code}")

    response = client.get("/cron/run")
    if response.status_code != 401:
        errors.append(f"GET /cron/run expected 401, got {response.status_code}")

    response = client.post("/telegram/webhook", json={})
    if response.status_code == 404:
        errors.append("POST /telegram/webhook returned 404")

    if errors:
        for err in errors:
            print(f"FAIL: {err}")
        return 1

    print("OK: smoke_check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
