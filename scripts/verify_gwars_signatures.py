#!/usr/bin/env python3
"""Smoke tests for GWars login signature helpers (R-211, prep for R-503)."""

import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from gwadm.services.gwars_signatures import (
    build_dev_signatures,
    compute_sign2,
    compute_sign4,
    verify_sign,
    verify_sign2,
    verify_sign3,
    verify_sign4,
)


def main() -> int:
    errors = []

    name = '_Колунт_'
    user_id = 283494
    level = 50
    synd = 5594
    has_passport = 1
    has_mobile = 1
    old_passport = 0
    name_encoded = quote(name.encode('cp1251'), safe='')

    signatures = build_dev_signatures(
        name, user_id, level, synd, has_passport, has_mobile, old_passport, name_encoded
    )

    if not signatures.get('sign'):
        errors.append('build_dev_signatures returned empty sign')
    if not signatures.get('sign2'):
        errors.append('build_dev_signatures returned empty sign2')
    if len(signatures.get('sign3', '')) != 10:
        errors.append(f"sign3 should be 10 chars, got {signatures.get('sign3')}")
    if len(signatures.get('sign4', '')) != 10:
        errors.append(f"sign4 should be 10 chars, got {signatures.get('sign4')}")

    expected_sign2 = compute_sign2(level, synd, user_id)
    if signatures['sign2'] != expected_sign2:
        errors.append('sign2 from build_dev_signatures does not match compute_sign2')

    today = datetime.now().strftime('%Y-%m-%d')
    expected_sign4 = compute_sign4(signatures['sign3'], today)
    if signatures['sign4'] != expected_sign4:
        errors.append('sign4 from build_dev_signatures does not match compute_sign4 for today')

    if not verify_sign(name, user_id, signatures['sign'], name_encoded):
        errors.append('verify_sign failed for login_dev test vector (encoded)')
    if not verify_sign2(level, synd, user_id, signatures['sign2']):
        errors.append('verify_sign2 failed for login_dev test vector')
    if not verify_sign3(
        name, user_id, has_passport, has_mobile, old_passport, signatures['sign3'], name_encoded
    ):
        errors.append('verify_sign3 failed for login_dev test vector')
    if not verify_sign4(signatures['sign3'], signatures['sign4']):
        errors.append('verify_sign4 failed for login_dev test vector')

    if verify_sign(name, user_id, 'invalid_sign', name_encoded):
        errors.append('verify_sign should reject invalid sign')
    if verify_sign2(level, synd, user_id, 'invalid'):
        errors.append('verify_sign2 should reject invalid sign2')

    if errors:
        print('FAIL:')
        for err in errors:
            print(f'  - {err}')
        return 1

    print('OK: GWars signature checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
