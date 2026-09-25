"""Avatar generation, DiceBear proxy cache, and URL helpers."""

import hashlib
import os
import secrets
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

from flask import url_for

try:
    import requests
except ImportError:
    requests = None

from gwadm.config import AVATAR_CACHE_DIR
from gwadm.db import get_db_connection
from gwadm.logging_config import log_error

VALID_AVATAR_STYLES = frozenset({
    'adventurer', 'adventurer-neutral', 'avataaars', 'avataaars-neutral',
    'big-ears', 'big-ears-neutral', 'big-smile', 'bottts', 'bottts-neutral',
    'croodles', 'croodles-neutral', 'fun-emoji', 'icons', 'identicon', 'initials',
    'lorelei', 'lorelei-neutral', 'micah', 'miniavs', 'open-peeps', 'personas',
    'pixel-art', 'pixel-art-neutral', 'rings', 'shapes', 'thumbs',
})


def generate_unique_avatar_seed(user_id):
    """Генерирует уникальный seed для аватара пользователя."""
    random_part = secrets.token_hex(8)
    return f"{user_id}_{random_part}"


def get_used_avatar_seeds(exclude_user_id=None):
    """Получает список всех используемых avatar_seed в системе."""
    conn = get_db_connection()
    if exclude_user_id:
        used_seeds = conn.execute(
            'SELECT avatar_seed FROM users WHERE avatar_seed IS NOT NULL AND user_id != ?',
            (exclude_user_id,),
        ).fetchall()
    else:
        used_seeds = conn.execute(
            'SELECT avatar_seed FROM users WHERE avatar_seed IS NOT NULL'
        ).fetchall()
    conn.close()
    return set(seed['avatar_seed'] for seed in used_seeds if seed['avatar_seed'])


def generate_unique_avatar_candidates(style, count=20, exclude_user_id=None):
    """Генерирует список уникальных кандидатов аватаров для выбранного стиля."""
    used_seeds = get_used_avatar_seeds(exclude_user_id)
    candidates = []
    attempts = 0
    max_attempts = count * 10

    while len(candidates) < count and attempts < max_attempts:
        seed = secrets.token_hex(12)
        if seed not in used_seeds and seed not in candidates:
            candidates.append(seed)
        attempts += 1

    return candidates


def normalize_avatar_style(style):
    """Возвращает валидный стиль DiceBear."""
    style_value = (style or 'avataaars').strip()
    if not style_value or style_value.lower() in ('none', 'null'):
        style_value = 'avataaars'
    if style_value not in VALID_AVATAR_STYLES:
        style_value = 'avataaars'
    return style_value


def build_dicebear_avatar_url(avatar_seed, style=None, size=128):
    """Прямой URL DiceBear (для серверной загрузки)."""
    style_value = normalize_avatar_style(style)
    try:
        size_value = int(size)
    except (TypeError, ValueError):
        size_value = 128
    size_value = max(16, min(size_value, 256))
    fmt = 'png' if size_value <= 64 else 'svg'
    return (
        f"https://api.dicebear.com/7.x/{style_value}/{fmt}"
        f"?seed={quote(str(avatar_seed), safe='')}&size={size_value}"
    ), style_value, size_value, fmt


def get_avatar_cache_path(avatar_seed, style=None, size=128):
    """Путь к файлу кэша аватара."""
    _, style_value, size_value, fmt = build_dicebear_avatar_url(avatar_seed, style, size)
    cache_name = hashlib.sha256(
        f'{style_value}:{avatar_seed}:{size_value}:{fmt}'.encode()
    ).hexdigest()
    return os.path.join(AVATAR_CACHE_DIR, f'{cache_name}.{fmt}'), fmt


def ensure_avatar_cached(avatar_seed, style=None, size=40):
    """Скачивает аватар в локальный кэш, если его ещё нет."""
    if not avatar_seed or not requests:
        return False
    cache_path, _fmt = get_avatar_cache_path(avatar_seed, style, size)
    if os.path.exists(cache_path):
        return True
    dicebear_url, _, _, _ = build_dicebear_avatar_url(avatar_seed, style, size)
    try:
        response = requests.get(dicebear_url, timeout=20)
        if response.status_code != 200:
            return False
        os.makedirs(AVATAR_CACHE_DIR, exist_ok=True)
        with open(cache_path, 'wb') as cache_file:
            cache_file.write(response.content)
        return True
    except Exception as exc:
        log_error(f"ensure_avatar_cached failed for seed={avatar_seed}: {exc}")
        return False


def warm_user_avatars(users, size=40, max_workers=4):
    """Прогревает кэш аватаров для списка пользователей."""
    seeds = [
        (user.get('avatar_seed'), user.get('avatar_style'))
        for user in users
        if user.get('avatar_seed')
    ]
    if not seeds:
        return
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(ensure_avatar_cached, seed, style, size)
            for seed, style in seeds
        ]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                log_error(f"warm_user_avatars task failed: {exc}")


def get_avatar_url(avatar_seed, style=None, size=128):
    """URL аватара через локальный прокси (кэш + без лимитов DiceBear в браузере)."""
    if not avatar_seed:
        return None
    style_value = normalize_avatar_style(style)
    try:
        size_value = int(size)
    except (TypeError, ValueError):
        size_value = 128
    size_value = max(16, min(size_value, 256))
    try:
        return url_for(
            'meta.avatar_image',
            seed=str(avatar_seed),
            style=style_value,
            size=size_value,
        )
    except RuntimeError:
        return (
            f"/avatars/image?seed={quote(str(avatar_seed), safe='')}"
            f"&style={quote(style_value, safe='')}&size={size_value}"
        )


def get_user_avatar_url(user, size=128):
    """Получает URL аватара пользователя с учетом его стиля."""
    if not user or not user.get('avatar_seed'):
        return None
    style = user.get('avatar_style') or 'avataaars'
    return get_avatar_url(user['avatar_seed'], style, size)
