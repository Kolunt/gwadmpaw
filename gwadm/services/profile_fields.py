"""Registry and admin policy for user profile / registration fields."""

import json
from typing import Any

from gwadm.logging_config import log_error
from gwadm.services.settings import get_setting, set_setting

POLICY_SETTING_KEY = 'profile_data_policy'

GROUP_LABELS = {
    'personal': 'ФИО',
    'address': 'Адрес',
    'contact': 'Контакты',
    'other': 'Прочее',
}

PROFILE_FIELD_REGISTRY: list[dict[str, Any]] = [
    {'id': 'last_name', 'column': 'last_name', 'label': 'Фамилия', 'group': 'personal', 'kind': 'text'},
    {'id': 'first_name', 'column': 'first_name', 'label': 'Имя', 'group': 'personal', 'kind': 'text'},
    {'id': 'middle_name', 'column': 'middle_name', 'label': 'Отчество', 'group': 'personal', 'kind': 'text'},
    {'id': 'postal_code', 'column': 'postal_code', 'label': 'Индекс', 'group': 'address', 'kind': 'text'},
    {'id': 'country', 'column': 'country', 'label': 'Страна', 'group': 'address', 'kind': 'text'},
    {'id': 'city', 'column': 'city', 'label': 'Город', 'group': 'address', 'kind': 'text'},
    {'id': 'street', 'column': 'street', 'label': 'Улица', 'group': 'address', 'kind': 'text'},
    {'id': 'house', 'column': 'house', 'label': 'Дом', 'group': 'address', 'kind': 'text'},
    {'id': 'building', 'column': 'building', 'label': 'Корпус/строение', 'group': 'address', 'kind': 'text'},
    {'id': 'apartment', 'column': 'apartment', 'label': 'Квартира', 'group': 'address', 'kind': 'text'},
    {'id': 'email', 'column': 'email', 'label': 'Email', 'group': 'contact', 'kind': 'email'},
    {'id': 'phone', 'column': 'phone', 'label': 'Телефон', 'group': 'contact', 'kind': 'phone'},
    {'id': 'telegram', 'column': 'telegram', 'label': 'Telegram', 'group': 'contact', 'kind': 'messenger'},
    {'id': 'whatsapp', 'column': 'whatsapp', 'label': 'WhatsApp', 'group': 'contact', 'kind': 'phone'},
    {'id': 'viber', 'column': 'viber', 'label': 'Viber', 'group': 'contact', 'kind': 'phone'},
    {'id': 'max', 'column': 'max', 'label': 'Max', 'group': 'contact', 'kind': 'phone'},
    {'id': 'bio', 'column': 'bio', 'label': 'О себе', 'group': 'other', 'kind': 'text'},
]

REGISTRATION_STEP_ORDER = [
    'intro',
    'personal',
    'address',
    'email',
    'phone',
    'telegram',
    'whatsapp',
    'viber',
    'max',
    'bio',
    'commitments',
    'final',
]

STEP_TO_FIELD = {
    'email': 'email',
    'phone': 'phone',
    'telegram': 'telegram',
    'whatsapp': 'whatsapp',
    'viber': 'viber',
    'max': 'max',
    'bio': 'bio',
}


def _registry_by_id() -> dict[str, dict]:
    return {f['id']: f for f in PROFILE_FIELD_REGISTRY}


def default_profile_data_policy() -> dict[str, dict[str, bool]]:
    """Defaults matching legacy behaviour before admin overrides."""
    policy: dict[str, dict[str, bool]] = {}
    required_ids = {
        'last_name', 'first_name', 'middle_name',
        'postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment',
        'phone',
    }
    for field in PROFILE_FIELD_REGISTRY:
        fid = field['id']
        collect = True
        required = fid in required_ids
        policy[fid] = {'collect': collect, 'required': required}
    return policy


def normalize_policy(raw: dict | None) -> dict[str, dict[str, bool]]:
    defaults = default_profile_data_policy()
    if not raw:
        return defaults
    merged = {fid: dict(defaults[fid]) for fid in defaults}
    for fid in defaults:
        if fid in raw and isinstance(raw[fid], dict):
            collect = bool(raw[fid].get('collect', merged[fid]['collect']))
            required = bool(raw[fid].get('required', merged[fid]['required']))
            if required:
                collect = True
            merged[fid] = {'collect': collect, 'required': required}
    return merged


def get_profile_data_policy() -> dict[str, dict[str, bool]]:
    raw = get_setting(POLICY_SETTING_KEY, '')
    if not raw:
        return default_profile_data_policy()
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError('policy must be object')
        return normalize_policy(parsed)
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        log_error(f'Invalid {POLICY_SETTING_KEY}: {e}')
        return default_profile_data_policy()


def validate_policy_payload(policy: dict[str, dict[str, bool]]) -> str | None:
    """Return error message or None if valid."""
    by_id = _registry_by_id()
    for fid, flags in policy.items():
        if flags.get('required') and not flags.get('collect'):
            label = by_id.get(fid, {}).get('label', fid)
            return f'Поле «{label}» не может быть обязательным, если сбор отключён.'
    normalized = normalize_policy(policy)
    contact_collect = any(
        normalized[fid]['collect']
        for fid in contact_field_ids()
    )
    if not contact_collect:
        return 'Должен быть включён хотя бы один контакт для сбора (правило «минимум один канал»).'
    return None


def save_profile_data_policy(policy: dict[str, dict[str, bool]]) -> str | None:
    normalized = normalize_policy(policy)
    err = validate_policy_payload(normalized)
    if err:
        return err
    set_setting(
        POLICY_SETTING_KEY,
        json.dumps(normalized, ensure_ascii=False),
        category='profile_data',
    )
    return None


def get_collectable_field_ids(policy: dict | None = None) -> list[str]:
    policy = policy or get_profile_data_policy()
    return [fid for fid, flags in policy.items() if flags.get('collect')]


def contact_field_ids() -> list[str]:
    return [f['id'] for f in PROFILE_FIELD_REGISTRY if f['group'] == 'contact']


def is_field_required(field_id: str, policy: dict | None = None) -> bool:
    policy = policy or get_profile_data_policy()
    flags = policy.get(field_id, {})
    return bool(flags.get('required')) and bool(flags.get('collect'))


def profile_snapshot_columns() -> list[str]:
    return [f['column'] for f in PROFILE_FIELD_REGISTRY]


def validate_profile_dict(profile: dict, policy: dict | None = None) -> dict:
    """
    Returns:
        ok: bool
        missing_labels: list[str] — required scalar fields empty
        missing_contact: bool — no filled contact among collected contact fields
    """
    policy = policy or get_profile_data_policy()
    by_id = _registry_by_id()
    missing_labels: list[str] = []

    for fid, flags in policy.items():
        if not flags.get('collect') or not flags.get('required'):
            continue
        col = by_id.get(fid, {}).get('column', fid)
        val = (profile.get(col) or profile.get(fid) or '').strip()
        if not val:
            missing_labels.append(by_id.get(fid, {}).get('label', fid))

    contact_ids = contact_field_ids()
    has_any_contact = False
    for fid in contact_ids:
        if not policy.get(fid, {}).get('collect'):
            continue
        col = by_id[fid]['column']
        val = (profile.get(col) or profile.get(fid) or '').strip()
        if val and val.lower() not in ('не использую', '-', '—'):
            has_any_contact = True
            break

    any_contact_collected = any(policy.get(fid, {}).get('collect') for fid in contact_ids)
    missing_contact = any_contact_collected and not has_any_contact

    ok = not missing_labels and not missing_contact
    return {
        'ok': ok,
        'missing_labels': missing_labels,
        'missing_contact': missing_contact,
    }


def build_registration_step_plan(policy: dict | None = None) -> list[str]:
    policy = policy or get_profile_data_policy()
    steps: list[str] = ['intro']

    personal_ids = [f['id'] for f in PROFILE_FIELD_REGISTRY if f['group'] == 'personal']
    if any(policy.get(fid, {}).get('collect') for fid in personal_ids):
        steps.append('personal')

    address_ids = [f['id'] for f in PROFILE_FIELD_REGISTRY if f['group'] == 'address']
    if any(policy.get(fid, {}).get('collect') for fid in address_ids):
        steps.append('address')

    for step_key, field_id in (
        ('email', 'email'),
        ('phone', 'phone'),
        ('telegram', 'telegram'),
        ('whatsapp', 'whatsapp'),
        ('viber', 'viber'),
        ('max', 'max'),
        ('bio', 'bio'),
    ):
        if policy.get(field_id, {}).get('collect'):
            steps.append(step_key)

    steps.extend(['commitments', 'final'])
    return steps


def fields_by_group() -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for field in PROFILE_FIELD_REGISTRY:
        g = field['group']
        groups.setdefault(g, []).append(field)
    return groups


def policy_for_api(policy: dict | None = None) -> dict[str, dict[str, bool]]:
    return get_profile_data_policy() if policy is None else normalize_policy(policy)
