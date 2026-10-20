"""Profile field registry and admin policy."""

from gwadm.services.profile_fields import (
    build_registration_step_plan,
    default_profile_data_policy,
    normalize_policy,
    save_profile_data_policy,
    validate_policy_payload,
    validate_profile_dict,
)


def test_default_policy_requires_phone_and_address():
    policy = default_profile_data_policy()
    assert policy['phone']['required'] is True
    assert policy['last_name']['required'] is True
    assert policy['telegram']['required'] is False
    assert policy['max']['collect'] is True


def test_required_without_collect_rejected():
    raw = default_profile_data_policy()
    raw['email'] = {'collect': False, 'required': True}
    assert validate_policy_payload(raw) is not None


def test_validate_profile_required_phone():
    policy = default_profile_data_policy()
    profile = {fid: '' for fid in policy}
    profile.update({
        'last_name': 'A',
        'first_name': 'B',
        'middle_name': 'C',
        'postal_code': '1',
        'country': 'RU',
        'city': 'M',
        'street': 'S',
        'house': '1',
        'building': '-',
        'apartment': '-',
        'phone': '+79991234567',
    })
    result = validate_profile_dict(profile, policy)
    assert result['ok'] is True

    profile['phone'] = ''
    result = validate_profile_dict(profile, policy)
    assert result['ok'] is False
    assert 'Телефон' in result['missing_labels']


def test_build_registration_step_plan_skips_disabled():
    policy = normalize_policy(default_profile_data_policy())
    policy['telegram']['collect'] = False
    policy['max']['collect'] = False
    steps = build_registration_step_plan(policy)
    assert 'telegram' not in steps
    assert 'max' not in steps
    assert 'intro' in steps
    assert 'final' in steps


def test_save_policy_roundtrip():
    policy = default_profile_data_policy()
    policy['bio']['collect'] = False
    err = save_profile_data_policy(policy)
    assert err is None
