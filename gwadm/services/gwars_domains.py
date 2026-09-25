"""GWars domain mirror resolution (host -> site_id, callback URLs)."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from urllib.parse import quote

GWARS_LOGIN_ENDPOINT = "https://www.gwars.io/cross-server-login.php"

DEFAULT_GWARS_DOMAIN_MAP: list[dict] = [
    {"host": "gwadm.ru", "site_id": 3, "primary": True},
    {"host": "www.gwadm.ru", "site_id": 3},
    {"host": "gwadm.pythonanywhere.com", "site_id": 4},
]

LOCAL_DEV_HOSTS = frozenset(
    {"127.0.0.1", "localhost", "127.0.0.1:5000", "localhost:5000"}
)

_HOST_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$",
    re.IGNORECASE,
)


def normalize_host(host: str) -> str:
    host = (host or "").strip().lower()
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def is_local_dev_host(host: str) -> bool:
    return (host or "").strip().lower() in LOCAL_DEV_HOSTS


def parse_domain_map(raw: str) -> list[dict]:
    if not raw or not str(raw).strip():
        return deepcopy(DEFAULT_GWARS_DOMAIN_MAP)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Некорректный JSON в gwars_domain_map: {exc}") from exc

    if not isinstance(data, list) or not data:
        raise ValueError("gwars_domain_map должен быть непустым JSON-массивом")

    normalized: list[dict] = []
    seen_hosts: set[str] = set()
    primary_count = 0

    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Элемент #{index} карты доменов должен быть объектом")

        host = normalize_host(str(item.get("host", "")))
        if not host or not _HOST_RE.match(host):
            raise ValueError(f"Элемент #{index}: некорректный домен '{item.get('host')}'")

        if host in seen_hosts:
            raise ValueError(f"Домен '{host}' указан более одного раза")
        seen_hosts.add(host)

        try:
            site_id = int(item.get("site_id"))
        except (TypeError, ValueError):
            raise ValueError(f"Элемент #{index}: site_id должен быть целым числом")
        if site_id <= 0:
            raise ValueError(f"Элемент #{index}: site_id должен быть > 0")

        is_primary = bool(item.get("primary"))
        if is_primary:
            primary_count += 1

        normalized.append({"host": host, "site_id": site_id, "primary": is_primary})

    if primary_count != 1:
        raise ValueError("В карте доменов должен быть ровно один primary=true")

    return normalized


def validate_domain_map_list(domain_map: list[dict]) -> list[dict]:
    return parse_domain_map(json.dumps(domain_map))


def serialize_domain_map(domain_map: list[dict]) -> str:
    return json.dumps(validate_domain_map_list(domain_map), ensure_ascii=False)


def get_primary_entry(domain_map: list[dict]) -> dict:
    for entry in domain_map:
        if entry.get("primary"):
            return entry
    return domain_map[0]


def get_primary_host(domain_map: list[dict] | None = None) -> str:
    entries = domain_map or DEFAULT_GWARS_DOMAIN_MAP
    return get_primary_entry(entries)["host"]


def find_domain_entry(host: str, domain_map: list[dict] | None = None) -> dict | None:
    entries = domain_map or DEFAULT_GWARS_DOMAIN_MAP
    normalized = normalize_host(host)
    for entry in entries:
        if entry["host"] == normalized:
            return entry
    return None


def resolve_gwars_site_id(host: str, domain_map: list[dict] | None = None) -> int:
    entries = domain_map or DEFAULT_GWARS_DOMAIN_MAP
    entry = find_domain_entry(host, entries)
    if entry:
        return int(entry["site_id"])
    return int(get_primary_entry(entries)["site_id"])


def build_gwars_callback_url(host: str, is_local: bool = False, domain_map: list[dict] | None = None) -> str:
    entries = domain_map or DEFAULT_GWARS_DOMAIN_MAP
    if is_local:
        callback_host = get_primary_host(entries)
    else:
        callback_host = normalize_host(host) or get_primary_host(entries)
    return f"https://{callback_host}/login"


def build_gwars_login_url(
    host: str,
    is_local: bool = False,
    domain_map: list[dict] | None = None,
) -> str:
    entries = domain_map or DEFAULT_GWARS_DOMAIN_MAP
    callback_url = build_gwars_callback_url(host, is_local=is_local, domain_map=entries)
    site_id = resolve_gwars_site_id(host if not is_local else get_primary_host(entries), entries)
    return f"{GWARS_LOGIN_ENDPOINT}?site_id={site_id}&url={quote(callback_url, safe='')}"


def load_gwars_domain_map():
    """Загружает карту доменов GWars из настроек с fallback на дефолт."""
    from gwadm.logging_config import log_error
    from gwadm.services.settings import get_setting

    raw = get_setting('gwars_domain_map', '')
    if raw:
        try:
            return parse_domain_map(raw)
        except ValueError as exc:
            log_error(f"Invalid gwars_domain_map in settings: {exc}")
    return list(DEFAULT_GWARS_DOMAIN_MAP)
