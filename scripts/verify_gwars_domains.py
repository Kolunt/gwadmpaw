#!/usr/bin/env python3
"""Smoke tests for GWars domain map resolution."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from gwars_domains import (
    DEFAULT_GWARS_DOMAIN_MAP,
    build_gwars_callback_url,
    build_gwars_login_url,
    parse_domain_map,
    resolve_gwars_site_id,
)


def main() -> int:
    errors = []

    raw = json.dumps(DEFAULT_GWARS_DOMAIN_MAP, ensure_ascii=False)
    parsed = parse_domain_map(raw)
    if len(parsed) != 3:
        errors.append(f"expected 3 default entries, got {len(parsed)}")

    if resolve_gwars_site_id("gwadm.ru", parsed) != 3:
        errors.append("gwadm.ru should resolve to site_id=3")

    if resolve_gwars_site_id("www.gwadm.ru", parsed) != 3:
        errors.append("www.gwadm.ru should resolve to site_id=3")

    if resolve_gwars_site_id("gwadm.pythonanywhere.com", parsed) != 4:
        errors.append("gwadm.pythonanywhere.com should resolve to site_id=4")

    callback = build_gwars_callback_url("gwadm.ru", domain_map=parsed)
    if callback != "https://gwadm.ru/login":
        errors.append(f"unexpected callback URL: {callback}")

    login_url = build_gwars_login_url("gwadm.ru", domain_map=parsed)
    if "site_id=3" not in login_url:
        errors.append(f"login URL missing site_id=3: {login_url}")
    if "url=https%3A%2F%2Fgwadm.ru%2Flogin" not in login_url:
        errors.append(f"login URL missing encoded callback: {login_url}")

    if errors:
        print("FAIL:")
        for err in errors:
            print(f"  - {err}")
        return 1

    print("OK: GWars domain map checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
