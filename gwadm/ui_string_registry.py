"""
Registry of UI translation keys (msg_key), category, and default RU/EN strings.

Used for SQLite seed (migration) and runtime fallback when DB has no row.
Manual keys use dot notation or legacy English msgids; generated keys are often
full Russian phrases (see ``_ui_string_registry_generated.py``).
"""

from __future__ import annotations

from gwadm._ui_string_registry_generated import GENERATED_UI_STRINGS

# (msg_key, category, default_ru, default_en)
_MANUAL_SEEDS: list[tuple[str, str, str, str]] = [
    # Legacy English msgids (category: legacy) — same keys as historical _() calls
    ('Home', 'legacy', 'Главная', 'Home'),
    ('Events', 'legacy', 'Мероприятия', 'Events'),
    ('Participants', 'legacy', 'Участники', 'Participants'),
    ('FAQ', 'legacy', 'FAQ', 'FAQ'),
    ('Admin Panel', 'legacy', 'Админ-панель', 'Admin Panel'),
    ('Users', 'legacy', 'Пользователи', 'Users'),
    ('Roles', 'legacy', 'Роли', 'Roles'),
    ('Titles', 'legacy', 'Звания', 'Titles'),
    ('Settings', 'legacy', 'Настройки', 'Settings'),
    ('Localization', 'legacy', 'Локализация', 'Localization'),
    ('Profile', 'legacy', 'Профиль', 'Profile'),
    ('Logout', 'legacy', 'Выйти', 'Logout'),
    ('Login via GWars', 'legacy', 'Войти через GWars', 'Login via GWars'),
    ('Edit Profile', 'legacy', 'Редактировать профиль', 'Edit Profile'),
    ('Main', 'legacy', 'Основное', 'Main'),
    ('Contacts', 'legacy', 'Контакты', 'Contacts'),
    ('About', 'legacy', 'О себе', 'About'),
    ('User Profile', 'legacy', 'Профиль пользователя', 'User Profile'),
    ('User ID:', 'legacy', 'ID пользователя:', 'User ID:'),
    ('Name:', 'legacy', 'Имя:', 'Name:'),
    ('Level:', 'legacy', 'Уровень:', 'Level:'),
    ('Syndicate:', 'legacy', 'Синдикат:', 'Syndicate:'),
    ('Gender:', 'legacy', 'Пол:', 'Gender:'),
    ('Passport:', 'legacy', 'Паспорт:', 'Passport:'),
    ('Mobile:', 'legacy', 'Мобильный:', 'Mobile:'),
    ('Last login:', 'legacy', 'Последний вход:', 'Last login:'),
    ('Yes', 'legacy', 'Есть', 'Yes'),
    ('No', 'legacy', 'Нет', 'No'),
    ('Not specified', 'legacy', 'Не указан', 'Not specified'),
    (
        'Contact information not specified',
        'legacy',
        'Контактная информация не указана',
        'Contact information not specified',
    ),
    (
        'Additional information not specified',
        'legacy',
        'Дополнительная информация не указана',
        'Additional information not specified',
    ),
    ('Toggle theme', 'legacy', 'Переключить тему', 'Toggle theme'),
    # Navigation & chrome
    ('nav.reputation', 'nav_public', 'Репутация', 'Reputation'),
    ('nav.rules', 'nav_public', 'Правила', 'Rules'),
    ('nav.contacts', 'nav_public', 'Контакты', 'Contacts'),
    ('nav.assignments', 'nav_public', 'Задания', 'Assignments'),
    ('aria.open_menu', 'nav_public', 'Открыть меню', 'Open menu'),
    ('aria.collapse_sidebar', 'nav_public', 'Свернуть меню', 'Collapse menu'),
    ('img.avatar', 'common', 'Аватар', 'Avatar'),
    ('brand.mobile_abbr', 'nav_public', 'АДМ', 'ADM'),
    ('navbar.max_channel', 'nav_public', 'Наш канал в MAX!', 'Our MAX channel'),
    ('navbar.telegram_channel', 'nav_public', 'Наш Telegram-канал', 'Our Telegram channel'),
    ('navbar.stop_impersonation', 'nav_public', 'Вернуться к своей учетной записи', 'Return to your account'),
    ('Меню аккаунта', 'nav_public', 'Меню аккаунта', 'Account menu'),
    ('Профиль', 'nav_public', 'Профиль', 'Profile'),
    ('Выход', 'nav_public', 'Выход', 'Log out'),
    ('admin.brand.desktop', 'nav_admin', 'АДМ · Админ', 'ADM · Admin'),
    ('admin.brand.mobile', 'nav_admin', 'ADM', 'ADM'),
    # Admin sidebar (RU keys — same string in templates)
    ('Главная', 'nav_admin', 'Главная', 'Home'),
    ('Мероприятия', 'nav_admin', 'Мероприятия', 'Events'),
    ('Награды', 'nav_admin', 'Награды', 'Awards'),
    ('Звания', 'nav_admin', 'Звания', 'Titles'),
    ('Логи', 'nav_admin', 'Логи', 'Logs'),
    ('Чаты', 'nav_admin', 'Чаты', 'Chats'),
    ('Правила', 'nav_admin', 'Правила', 'Rules'),
    ('Рассылки', 'nav_admin', 'Рассылки', 'Broadcasts'),
    ('Цыфорки', 'nav_admin', 'Цыфорки', 'Reputation bells'),
    ('Шёпоты главной', 'nav_admin', 'Шёпоты главной', 'Hero whispers'),
    ('Локализация', 'nav_admin', 'Локализация', 'Localization'),
    (
        'Сбросьте категорию или поиск выше — возможно, совпадений нет.',
        'admin_localization',
        'Сбросьте категорию или поиск выше — возможно, совпадений нет.',
        'Clear the category or search above — there may be no matches.',
    ),
]


def _merge_seeds() -> list[tuple[str, str, str, str]]:
    merged: list[tuple[str, str, str, str]] = []
    seen: set[str] = set()
    for row in _MANUAL_SEEDS:
        merged.append(row)
        seen.add(row[0])
    for row in GENERATED_UI_STRINGS:
        if row[0] not in seen:
            merged.append(row)
            seen.add(row[0])
    return merged


UI_STRING_SEEDS: list[tuple[str, str, str, str]] = _merge_seeds()
