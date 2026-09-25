"""GWars cross-server login signature computation and verification."""

import hashlib
from datetime import datetime, timedelta
from urllib.parse import quote, unquote, unquote_plus, unquote_to_bytes

from gwadm.config import GWARS_PASSWORD, is_debug
from gwadm.logging_config import log_debug, log_error


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _encoded_variations(encoded_name: str) -> list[str]:
    variations = [encoded_name]
    if '+' in encoded_name:
        variations.append(encoded_name.replace('+', '%20'))
    return variations


def compute_sign_bytes(name_bytes: bytes, user_id) -> str:
    return _md5_hex(GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8'))


def compute_sign_decoded(username: str, user_id) -> str:
    return _md5_hex((GWARS_PASSWORD + username + str(user_id)).encode('utf-8'))


def compute_sign_encoded(encoded_name: str, user_id) -> str:
    return _md5_hex((GWARS_PASSWORD + encoded_name + str(user_id)).encode('utf-8'))


def compute_sign_swapped(user_id, name: str) -> str:
    return _md5_hex((GWARS_PASSWORD + str(user_id) + name).encode('utf-8'))


def compute_sign_swapped_encoded(user_id, encoded_name: str) -> str:
    return _md5_hex((GWARS_PASSWORD + str(user_id) + encoded_name).encode('utf-8'))


def compute_sign_cp1251(name_cp1251: str, user_id) -> str:
    return _md5_hex((GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8'))


def compute_sign_latin1_bytes(name_latin1_bytes: bytes, user_id) -> str:
    return _md5_hex(GWARS_PASSWORD.encode('utf-8') + name_latin1_bytes + str(user_id).encode('utf-8'))


def compute_sign2(level, synd, user_id) -> str:
    return _md5_hex(
        (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
    )


def compute_sign3_bytes(name_bytes: bytes, user_id, has_passport, has_mobile, old_passport) -> str:
    return _md5_hex(
        GWARS_PASSWORD.encode('utf-8')
        + name_bytes
        + str(user_id).encode('utf-8')
        + str(has_passport).encode('utf-8')
        + str(has_mobile).encode('utf-8')
        + str(old_passport).encode('utf-8')
    )[:10]


def compute_sign3_decoded(username: str, user_id, has_passport, has_mobile, old_passport) -> str:
    return _md5_hex(
        (
            GWARS_PASSWORD
            + username
            + str(user_id)
            + str(has_passport)
            + str(has_mobile)
            + str(old_passport)
        ).encode('utf-8')
    )[:10]


def compute_sign4(sign3: str, date_str: str) -> str:
    return _md5_hex((date_str + sign3 + GWARS_PASSWORD).encode('utf-8'))[:10]


def iter_sign_variants(username, user_id, encoded_name=None) -> list[tuple[str, str]]:
    """All sign variants used for verification and debug pages."""
    variants: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(name: str, value: str) -> None:
        if value not in seen:
            seen.add(value)
            variants.append((name, value))

    if encoded_name:
        for encoded_variant in _encoded_variations(encoded_name):
            try:
                name_bytes = unquote_to_bytes(encoded_variant)
                suffix = '' if encoded_variant == encoded_name else '_space'
                add(f'bytes{suffix}', compute_sign_bytes(name_bytes, user_id))
            except Exception:
                pass

    add('decoded', compute_sign_decoded(username, user_id))

    if encoded_name:
        for encoded_variant in _encoded_variations(encoded_name):
            suffix = '' if encoded_variant == encoded_name else '_space'
            add(f'encoded{suffix}', compute_sign_encoded(encoded_variant, user_id))

        try:
            name_cp1251 = unquote_plus(encoded_name, encoding='cp1251')
            add('cp1251', compute_sign_cp1251(name_cp1251, user_id))
        except Exception:
            pass

        try:
            name_latin1 = unquote_plus(encoded_name, encoding='latin1')
            name_latin1_bytes = name_latin1.encode('latin1')
            add('latin1_bytes', compute_sign_latin1_bytes(name_latin1_bytes, user_id))
        except Exception:
            pass

    return variants


def iter_sign3_variants(
    username, user_id, has_passport, has_mobile, old_passport, encoded_name=None
) -> list[tuple[str, str]]:
    variants: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(name: str, value: str) -> None:
        if value not in seen:
            seen.add(value)
            variants.append((name, value))

    if encoded_name:
        for encoded_variant in _encoded_variations(encoded_name):
            try:
                name_bytes = unquote_to_bytes(encoded_variant)
                suffix = '' if encoded_variant == encoded_name else '_space'
                add(
                    f'bytes{suffix}',
                    compute_sign3_bytes(
                        name_bytes, user_id, has_passport, has_mobile, old_passport
                    ),
                )
            except Exception:
                pass

    add(
        'decoded',
        compute_sign3_decoded(username, user_id, has_passport, has_mobile, old_passport),
    )

    return variants


def verify_sign(username, user_id, sign, encoded_name=None) -> bool:
    variants = iter_sign_variants(username, user_id, encoded_name)

    if is_debug():
        log_debug(f"verify_sign: username={username}, user_id={user_id}, encoded_name={encoded_name}")
        for variant_name, variant_sign in variants:
            match_status = 'MATCH' if variant_sign == sign else 'NO MATCH'
            log_debug(f"verify_sign: variant {variant_name}={variant_sign}, {match_status}")

    for variant_name, variant_sign in variants:
        if variant_sign == sign:
            if is_debug():
                log_debug(f"verify_sign: SUCCESS with variant {variant_name}!")
            return True

    log_error(f"verify_sign: ALL VARIANTS FAILED! Received sign={sign}")
    return False


def verify_sign2(level, synd, user_id, sign2) -> bool:
    return compute_sign2(level, synd, user_id) == sign2


def verify_sign3(
    username, user_id, has_passport, has_mobile, old_passport, sign3, encoded_name=None
) -> bool:
    variants = iter_sign3_variants(
        username, user_id, has_passport, has_mobile, old_passport, encoded_name
    )

    for variant_name, variant_sign in variants:
        if variant_sign == sign3:
            if is_debug():
                log_debug(f"verify_sign3: SUCCESS with variant {variant_name}!")
            return True

    log_error(f"verify_sign3: ALL VARIANTS FAILED! Received sign3={sign3}")
    return False


def verify_sign4(sign3, sign4) -> bool:
    """Проверяет sign4 с учётом возможной разницы в часовых поясах."""
    today = datetime.now()
    dates_to_check = [
        today.strftime('%Y-%m-%d'),
        (today - timedelta(days=1)).strftime('%Y-%m-%d'),
        (today + timedelta(days=1)).strftime('%Y-%m-%d'),
    ]

    for date_str in dates_to_check:
        if compute_sign4(sign3, date_str) == sign4:
            log_debug(f"verify_sign4: SUCCESS with date {date_str}")
            return True

    log_error(f"verify_sign4: FAILED. Received sign4={sign4}, sign3={sign3}")
    log_error(f"verify_sign4: Checked dates: {dates_to_check}")
    return False


def extract_name_encoded_from_request(request) -> str | None:
    """Extract raw URL-encoded name from query string."""
    try:
        query_string = request.query_string.decode('utf-8', errors='replace')
    except Exception:
        query_string = request.query_string.decode('utf-8') if request.query_string else ''

    name_encoded = None
    if query_string:
        for param in query_string.split('&'):
            if param.startswith('name='):
                name_encoded = param.split('=', 1)[1]
                break

    if not name_encoded:
        name_encoded = request.args.get('name', '') or None

    return name_encoded


def decode_gwars_name(name_encoded: str | None, name_from_args: str = '') -> dict:
    """Decode GWars username from URL encoding (CP1251 primary)."""
    name = name_encoded or ''
    name_cp1251 = None
    name_latin1 = None

    if name_encoded:
        try:
            name_cp1251 = unquote_plus(name_encoded, encoding='cp1251')
            name = name_cp1251
        except Exception:
            try:
                name = unquote_plus(name_encoded, encoding='utf-8')
            except Exception:
                try:
                    name = unquote_plus(name_encoded, encoding='latin1')
                    name_latin1 = name
                except Exception:
                    name = name_encoded
                    name_latin1 = name_encoded

        if not name_cp1251:
            try:
                name_cp1251 = unquote_plus(name_encoded, encoding='cp1251')
            except Exception:
                name_cp1251 = None

        if not name_latin1:
            try:
                name_latin1 = unquote_plus(name_encoded, encoding='latin1')
            except Exception:
                name_latin1 = None

    if (not name or name == '') and name_from_args:
        name = name_from_args
        if not name_encoded:
            name_encoded = name_from_args

    return {
        'name': name,
        'name_encoded': name_encoded,
        'name_cp1251': name_cp1251,
        'name_latin1': name_latin1,
    }


def build_dev_signatures(
    name: str,
    user_id,
    level,
    synd,
    has_passport,
    has_mobile,
    old_passport,
    name_encoded: str | None = None,
) -> dict:
    """Compute all login signatures for login_dev (CP1251 name bytes)."""
    name_bytes = name.encode('cp1251')
    sign = compute_sign_bytes(name_bytes, user_id)
    sign2 = compute_sign2(level, synd, user_id)
    sign3 = compute_sign3_bytes(
        name_bytes, user_id, has_passport, has_mobile, old_passport
    )
    today = datetime.now().strftime('%Y-%m-%d')
    sign4 = compute_sign4(sign3, today)

    return {
        'sign': sign,
        'sign2': sign2,
        'sign3': sign3,
        'sign4': sign4,
        'name_encoded': name_encoded or quote(name.encode('cp1251'), safe=''),
    }


def build_sign_debug_info(
    request,
    name: str,
    name_encoded: str | None,
    name_cp1251,
    name_latin1,
    user_id,
    sign: str,
    sign2: str,
    level,
    synd,
) -> dict:
    """Build debug_info dict for templates/debug.html."""
    try:
        query_string = request.query_string.decode('utf-8', errors='replace')
    except Exception:
        query_string = request.query_string.decode('utf-8') if request.query_string else ''

    name_from_args = request.args.get('name', '')

    variant_bytes = None
    if name_encoded:
        try:
            name_bytes = unquote_to_bytes(name_encoded)
            variant_bytes = compute_sign_bytes(name_bytes, user_id)
        except Exception:
            pass

    variant1 = compute_sign_decoded(name, user_id)
    encoded_for_sign = name_encoded or ''
    variant2 = compute_sign_encoded(encoded_for_sign, user_id) if encoded_for_sign else ''
    variant3 = compute_sign_swapped(user_id, name)
    variant4 = compute_sign_swapped_encoded(user_id, encoded_for_sign) if encoded_for_sign else ''

    variant5 = None
    if name_cp1251:
        variant5 = compute_sign_cp1251(name_cp1251, user_id)
    elif name_encoded:
        try:
            cp1251_name = unquote(name_encoded, encoding='cp1251')
            variant5 = compute_sign_cp1251(cp1251_name, user_id)
        except Exception:
            variant5 = None

    variant_latin1_bytes = None
    try:
        latin1_name = name_latin1
        if not latin1_name and name_encoded:
            latin1_name = unquote(name_encoded, encoding='latin1')
        if latin1_name:
            variant_latin1_bytes = compute_sign_latin1_bytes(
                latin1_name.encode('latin1'), user_id
            )
    except Exception:
        variant_latin1_bytes = None

    variant6 = None
    if name:
        try:
            re_encoded = quote(name, safe='')
            variant6 = compute_sign_encoded(re_encoded, user_id)
        except Exception:
            variant6 = None

    variant7 = None
    if name_from_args and name_from_args != name:
        variant7 = compute_sign_decoded(name_from_args, user_id)

    variant8 = None
    variant9 = None
    if not name or name == '':
        variant8 = compute_sign_decoded('', user_id)
        variant9 = compute_sign_swapped(user_id, '')

    expected_sign2 = compute_sign2(level, synd, user_id)

    def _match(value, received):
        if value is None:
            return False
        return value == received

    return {
        'received_params': dict(request.args),
        'password': GWARS_PASSWORD,
        'encoded_name': name_encoded if name_encoded else 'EMPTY',
        'decoded_name': name if name else 'EMPTY',
        'decoded_name_cp1251': name_cp1251 if name_cp1251 else 'N/A',
        'decoded_name_latin1': name_latin1 if name_latin1 else 'N/A',
        'name_from_args': name_from_args if name_from_args else 'EMPTY',
        'user_id': user_id,
        'query_string': query_string,
        'full_url': request.url,
        'variant_bytes': variant_bytes if variant_bytes else 'N/A',
        'variant1': variant1,
        'variant2': variant2,
        'variant3': variant3,
        'variant4': variant4,
        'variant5': variant5 if variant5 else 'N/A',
        'variant_latin1_bytes': variant_latin1_bytes if variant_latin1_bytes else 'N/A',
        'variant6': variant6 if variant6 else 'N/A',
        'variant7': variant7 if variant7 else 'N/A',
        'variant8': variant8 if variant8 else 'N/A',
        'variant9': variant9 if variant9 else 'N/A',
        'received_sign': sign,
        'sign_match_bytes': _match(variant_bytes, sign),
        'sign_match_v1': variant1 == sign,
        'sign_match_v2': variant2 == sign,
        'sign_match_v3': variant3 == sign,
        'sign_match_v4': variant4 == sign,
        'sign_match_v5': _match(variant5, sign),
        'sign_match_latin1_bytes': _match(variant_latin1_bytes, sign),
        'sign_match_v6': _match(variant6, sign),
        'sign_match_v7': _match(variant7, sign),
        'sign_match_v8': _match(variant8, sign),
        'sign_match_v9': _match(variant9, sign),
        'expected_sign2': expected_sign2,
        'received_sign2': sign2,
        'sign2_match': expected_sign2 == sign2,
    }


def build_sign3_debug_info(
    request,
    name: str,
    name_encoded: str | None,
    user_id,
    has_passport,
    has_mobile,
    old_passport,
    sign3: str,
    sign4: str,
) -> dict:
    """Build debug_info dict for templates/debug_sign3.html."""
    sign3_variant_bytes = None
    if name_encoded:
        try:
            name_bytes = unquote_to_bytes(name_encoded)
            sign3_variant_bytes = compute_sign3_bytes(
                name_bytes, user_id, has_passport, has_mobile, old_passport
            )
        except Exception:
            pass

    sign3_variant_decoded = compute_sign3_decoded(
        name, user_id, has_passport, has_mobile, old_passport
    )

    today = datetime.now().strftime('%Y-%m-%d')
    sign4_variant1 = compute_sign4(sign3, today)

    return {
        'received_params': dict(request.args),
        'password': GWARS_PASSWORD,
        'encoded_name': name_encoded if name_encoded else 'EMPTY',
        'decoded_name': name if name else 'EMPTY',
        'user_id': user_id,
        'has_passport': has_passport,
        'has_mobile': has_mobile,
        'old_passport': old_passport,
        'sign3_received': sign3,
        'sign3_variant_bytes': sign3_variant_bytes if sign3_variant_bytes else 'N/A',
        'sign3_variant_decoded': sign3_variant_decoded,
        'sign3_match_bytes': sign3_variant_bytes == sign3 if sign3_variant_bytes else False,
        'sign3_match_decoded': sign3_variant_decoded == sign3,
        'sign4_received': sign4,
        'sign4_variant1': sign4_variant1,
        'sign4_match': sign4_variant1 == sign4,
    }
