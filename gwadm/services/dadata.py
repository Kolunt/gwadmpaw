"""DaData address suggestions (server-side proxy)."""

from __future__ import annotations

try:
    import requests
except ImportError:
    requests = None

from gwadm.logging_config import log_error
from gwadm.services.settings import get_setting

DADATA_SUGGEST_URL = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address'
MAX_QUERY_LEN = 200


class DadataError(Exception):
    """User-safe DaData integration error."""


def _str_field(value) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _typed_address_part(payload: dict, key: str) -> str:
    return _str_field(payload.get(f'{key}_with_type')) or _str_field(payload.get(key))


_LOCALITY_MARKERS = (
    'хутор',
    'деревня',
    'село',
    'посёлок',
    'поселок',
    'пгт',
    'рп',
    'ст-ца',
    'станица',
    'мкр',
    'аул',
    'сл',
)


def _map_city(payload: dict) -> str:
    city = _str_field(payload.get('city'))
    settlement = _str_field(payload.get('settlement'))
    area = _str_field(payload.get('area'))
    if city and settlement:
        return f'{_typed_address_part(payload, "city")}, {_typed_address_part(payload, "settlement")}'
    if city:
        return _typed_address_part(payload, 'city')
    if settlement:
        if area:
            return f'{_typed_address_part(payload, "area")}, {_typed_address_part(payload, "settlement")}'
        return _typed_address_part(payload, 'settlement')
    if area:
        return _typed_address_part(payload, 'area')
    return ''


def _enrich_city_from_display(city: str, payload: dict, display_value: str | None) -> str:
    """Append locality from suggestion text when DaData omits settlement in structured data."""
    if _str_field(payload.get('settlement')):
        return city
    street = _str_field(payload.get('street_with_type')) or _str_field(payload.get('street'))
    if street:
        return city
    value = _str_field(display_value)
    if not value:
        return city
    parts = [p.strip() for p in value.split(',') if p.strip()]
    if parts and parts[0].isdigit() and len(parts[0]) == 6:
        parts = parts[1:]
    if not parts:
        return city
    tail = parts[-1]
    tail_lower = tail.lower()
    if not any(marker in tail_lower for marker in _LOCALITY_MARKERS):
        return city
    if city and tail_lower in city.lower():
        return city
    if city:
        return f'{city}, {tail}'
    return tail


def map_dadata_address_data(data: dict | None, display_value: str | None = None) -> dict:
    payload = data or {}
    street = payload.get('street_with_type') or payload.get('street') or ''
    city = _enrich_city_from_display(_map_city(payload), payload, display_value)
    return {
        'postal_code': _str_field(payload.get('postal_code')),
        'country': _str_field(payload.get('country')),
        'city': city,
        'street': _str_field(street),
        'house': _str_field(payload.get('house')),
        'building': _str_field(payload.get('block')),
        'apartment': _str_field(payload.get('flat')),
    }


def is_dadata_address_enabled() -> bool:
    if get_setting('dadata_enabled', '0') != '1':
        return False
    if get_setting('dadata_verified', '0') != '1':
        return False
    api_key = (get_setting('dadata_api_key', '') or '').strip()
    secret_key = (get_setting('dadata_secret_key', '') or '').strip()
    return bool(api_key and secret_key)


def suggest_address(query: str, count: int = 7) -> list[dict]:
    text = (query or '').strip()
    if len(text) < 2:
        raise DadataError('Введите не менее 2 символов')
    if len(text) > MAX_QUERY_LEN:
        text = text[:MAX_QUERY_LEN]

    if not is_dadata_address_enabled():
        raise DadataError('Интеграция DaData отключена')

    if requests is None:
        raise DadataError('Модуль requests недоступен на сервере')

    api_key = (get_setting('dadata_api_key', '') or '').strip()
    secret_key = (get_setting('dadata_secret_key', '') or '').strip()

    try:
        response = requests.post(
            DADATA_SUGGEST_URL,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': f'Token {api_key}',
                'X-Secret': secret_key,
            },
            json={'query': text, 'count': max(1, min(count, 10))},
            timeout=8,
        )
    except requests.RequestException as exc:
        log_error(f'DaData suggest request failed: {exc}')
        raise DadataError('Сервис подсказок временно недоступен') from exc

    if response.status_code in (401, 403):
        raise DadataError('Ключи DaData не приняты сервисом')
    if response.status_code >= 400:
        log_error(f'DaData suggest HTTP {response.status_code}: {response.text[:300]}')
        raise DadataError('Не удалось получить подсказки адреса')

    try:
        body = response.json()
    except ValueError as exc:
        log_error('DaData suggest returned non-JSON response')
        raise DadataError('Сервис подсказок вернул неожиданный ответ') from exc

    suggestions = body.get('suggestions')
    if not isinstance(suggestions, list):
        return []
    return suggestions
