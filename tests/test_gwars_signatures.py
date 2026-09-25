"""Tests for GWars login signature helpers."""

from datetime import datetime
from urllib.parse import quote

from gwadm.services.gwars_signatures import (
    build_dev_signatures,
    compute_sign2,
    compute_sign4,
    verify_sign,
    verify_sign2,
    verify_sign3,
    verify_sign4,
)


def test_build_and_verify_dev_signatures():
    name = "_Колунт_"
    user_id = 283494
    level = 50
    synd = 5594
    has_passport = 1
    has_mobile = 1
    old_passport = 0
    name_encoded = quote(name.encode("cp1251"), safe="")

    signatures = build_dev_signatures(
        name, user_id, level, synd, has_passport, has_mobile, old_passport, name_encoded
    )

    assert signatures.get("sign")
    assert signatures.get("sign2")
    assert len(signatures.get("sign3", "")) == 10
    assert len(signatures.get("sign4", "")) == 10

    expected_sign2 = compute_sign2(level, synd, user_id)
    assert signatures["sign2"] == expected_sign2

    today = datetime.now().strftime("%Y-%m-%d")
    expected_sign4 = compute_sign4(signatures["sign3"], today)
    assert signatures["sign4"] == expected_sign4

    assert verify_sign(name, user_id, signatures["sign"], name_encoded)
    assert verify_sign2(level, synd, user_id, signatures["sign2"])
    assert verify_sign3(
        name, user_id, has_passport, has_mobile, old_passport, signatures["sign3"], name_encoded
    )
    assert verify_sign4(signatures["sign3"], signatures["sign4"])


def test_reject_invalid_signatures():
    name = "_Колунт_"
    user_id = 283494
    level = 50
    synd = 5594
    name_encoded = quote(name.encode("cp1251"), safe="")

    assert not verify_sign(name, user_id, "invalid_sign", name_encoded)
    assert not verify_sign2(level, synd, user_id, "invalid")
