"""Tests for GWars domain map resolution."""

import json

from gwars_domains import (
    DEFAULT_GWARS_DOMAIN_MAP,
    build_gwars_callback_url,
    build_gwars_login_url,
    parse_domain_map,
    resolve_gwars_site_id,
)


def test_default_domain_map_has_three_entries():
    raw = json.dumps(DEFAULT_GWARS_DOMAIN_MAP, ensure_ascii=False)
    parsed = parse_domain_map(raw)
    assert len(parsed) == 3


def test_resolve_gwadm_ru_site_id():
    parsed = parse_domain_map(json.dumps(DEFAULT_GWARS_DOMAIN_MAP, ensure_ascii=False))
    assert resolve_gwars_site_id("gwadm.ru", parsed) == 3
    assert resolve_gwars_site_id("www.gwadm.ru", parsed) == 3
    assert resolve_gwars_site_id("gwadm.pythonanywhere.com", parsed) == 4


def test_build_gwars_urls():
    parsed = parse_domain_map(json.dumps(DEFAULT_GWARS_DOMAIN_MAP, ensure_ascii=False))
    callback = build_gwars_callback_url("gwadm.ru", domain_map=parsed)
    assert callback == "https://gwadm.ru/login"

    login_url = build_gwars_login_url("gwadm.ru", domain_map=parsed)
    assert "site_id=3" in login_url
    assert "url=https%3A%2F%2Fgwadm.ru%2Flogin" in login_url
