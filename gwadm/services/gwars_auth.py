"""GWars cross-server login signatures and session finalization."""

import hashlib
from datetime import datetime, timedelta
from urllib.parse import unquote_plus, unquote_to_bytes

from flask import session

from gwadm.config import ADMIN_USER_IDS, GWARS_PASSWORD
from gwadm.logging_config import log_debug, log_error
from gwadm.services.activity import log_activity
from gwadm.services.roles import assign_role, get_user_role_names, get_user_roles, has_role


def verify_sign(username, user_id, sign, encoded_name=None):
    variants = []

    if encoded_name:
        encoded_variations = [encoded_name]
        if '+' in encoded_name:
            encoded_variations.append(encoded_name.replace('+', '%20'))
        for encoded_variant in encoded_variations:
            try:
                name_bytes = unquote_to_bytes(encoded_variant)
                expected_sign_bytes = hashlib.md5(
                    GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8')
                ).hexdigest()
                suffix = '' if encoded_variant == encoded_name else '_space'
                variants.append((f'bytes{suffix}', expected_sign_bytes))
            except Exception:
                pass

    expected_sign_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id)).encode('utf-8')
    ).hexdigest()
    variants.append(('decoded', expected_sign_decoded))

    if encoded_name:
        encoded_variations = [encoded_name]
        if '+' in encoded_name:
            encoded_variations.append(encoded_name.replace('+', '%20'))
        for encoded_variant in encoded_variations:
            expected_sign_encoded = hashlib.md5(
                (GWARS_PASSWORD + encoded_variant + str(user_id)).encode('utf-8')
            ).hexdigest()
            suffix = '' if encoded_variant == encoded_name else '_space'
            variants.append((f'encoded{suffix}', expected_sign_encoded))

        try:
            name_cp1251 = unquote_plus(encoded_name, encoding='cp1251')
            expected_sign_cp1251 = hashlib.md5(
                (GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8')
            ).hexdigest()
            variants.append(('cp1251', expected_sign_cp1251))
        except Exception:
            pass

        try:
            name_latin1 = unquote_plus(encoded_name, encoding='latin1')
            name_latin1_bytes = name_latin1.encode('latin1')
            expected_sign_latin1_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_latin1_bytes + str(user_id).encode('utf-8')
            ).hexdigest()
            variants.append(('latin1_bytes', expected_sign_latin1_bytes))
        except Exception:
            pass

    log_error(f"verify_sign: username={username}, user_id={user_id}")
    log_error(f"verify_sign: encoded_name={encoded_name}")
    for variant_name, variant_sign in variants:
        match_status = "MATCH" if variant_sign == sign else "NO MATCH"
        log_error(f"verify_sign: variant {variant_name}={variant_sign}, {match_status}")

    for variant_name, variant_sign in variants:
        if variant_sign == sign:
            log_error(f"verify_sign: SUCCESS with variant {variant_name}!")
            return True

    log_error(f"verify_sign: ALL VARIANTS FAILED! Received sign={sign}")
    return False


def verify_sign2(level, synd, user_id, sign2):
    expected_sign2 = hashlib.md5(
        (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
    ).hexdigest()
    return expected_sign2 == sign2


def verify_sign3(username, user_id, has_passport, has_mobile, old_passport, sign3, encoded_name=None):
    variants = []

    if encoded_name:
        encoded_variations = [encoded_name]
        if '+' in encoded_name:
            encoded_variations.append(encoded_name.replace('+', '%20'))
        for encoded_variant in encoded_variations:
            try:
                name_bytes = unquote_to_bytes(encoded_variant)
                expected_sign3_bytes = hashlib.md5(
                    GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8')
                    + str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8')
                    + str(old_passport).encode('utf-8')
                ).hexdigest()[:10]
                suffix = '' if encoded_variant == encoded_name else '_space'
                variants.append((f'bytes{suffix}', expected_sign3_bytes))
            except Exception:
                pass

    expected_sign3_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id) + str(has_passport) + str(has_mobile) + str(old_passport)).encode('utf-8')
    ).hexdigest()[:10]
    variants.append(('decoded', expected_sign3_decoded))

    for variant_name, variant_sign in variants:
        if variant_sign == sign3:
            log_error(f"verify_sign3: SUCCESS with variant {variant_name}!")
            return True

    log_error(f"verify_sign3: ALL VARIANTS FAILED! Received sign3={sign3}")
    return False


def verify_sign4(sign3, sign4):
    """Проверяет sign4 с учётом возможной разницы в часовых поясах."""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    dates_to_check = [
        today.strftime("%Y-%m-%d"),
        yesterday.strftime("%Y-%m-%d"),
        tomorrow.strftime("%Y-%m-%d"),
    ]

    for date_str in dates_to_check:
        expected_sign4 = hashlib.md5(
            (date_str + sign3 + GWARS_PASSWORD).encode('utf-8')
        ).hexdigest()[:10]
        if expected_sign4 == sign4:
            log_debug(f"verify_sign4: SUCCESS with date {date_str}")
            return True

    log_error(f"verify_sign4: FAILED. Received sign4={sign4}, sign3={sign3}")
    log_error(f"verify_sign4: Checked dates: {dates_to_check}")
    return False


def finalize_user_login(user_id, name, level, synd, source='gwars', details=None):
    """Назначает роли, заполняет session и пишет activity log после успешного входа."""
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        user_id_int = user_id

    if user_id_int in ADMIN_USER_IDS:
        if not has_role(user_id, 'admin'):
            assign_role(user_id, 'admin', assigned_by=user_id)
            log_debug(f"Admin role automatically assigned to user_id {user_id}")

    if not get_user_roles(user_id):
        assign_role(user_id, 'user', assigned_by=user_id)
        log_debug(f"Default 'user' role assigned to user_id {user_id}")

    session['user_id'] = user_id
    session['username'] = name
    session['level'] = level
    session['synd'] = synd
    session['roles'] = get_user_role_names(user_id)
    session.pop('gwars_auth_attempt', None)

    log_activity(
        'login',
        details=details or ('Вход через GWars' if source == 'gwars' else 'Тестовый вход через login_dev'),
        metadata={'source': source, 'user_id': user_id, 'username': name},
    )
