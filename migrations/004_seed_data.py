"""Default roles, permissions, settings, and seed data."""

import json
import sqlite3

from gwadm.config import ADMIN_USER_IDS
from gwadm.logging_config import log_error
from gwadm.services.gwars_domains import DEFAULT_GWARS_DOMAIN_MAP


def upgrade(conn: sqlite3.Connection) -> None:
    c = conn.cursor()

    default_titles = [
        ('author', 'Автор идеи', 'Автор идеи проекта', '#28a745', '💡', 1),
        ('developer', 'Разработчик', 'Разработчик проекта', '#007bff', '💻', 1),
        ('ambassador', 'Амбассадор', 'Амбассадор проекта', '#ffc107', '⭐', 1),
        ('designer', 'Дизайнер', 'Дизайнер проекта', '#e83e8c', '🎨', 1),
    ]
    for title_name, title_display, title_desc, title_color, title_icon, is_system in default_titles:
        c.execute(
            '''
            INSERT OR IGNORE INTO titles (name, display_name, description, color, icon, is_system)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (title_name, title_display, title_desc, title_color, title_icon, is_system),
        )

    default_permissions = [
        ('users.view', 'Просмотр пользователей', 'Возможность просматривать список пользователей', 'users'),
        ('users.edit', 'Редактирование пользователей', 'Возможность редактировать данные пользователей', 'users'),
        ('users.delete', 'Удаление пользователей', 'Возможность удалять пользователей', 'users'),
        ('users.roles', 'Управление ролями пользователей', 'Возможность назначать роли пользователям', 'users'),
        ('roles.view', 'Просмотр ролей', 'Возможность просматривать список ролей', 'roles'),
        ('roles.create', 'Создание ролей', 'Возможность создавать новые роли', 'roles'),
        ('roles.edit', 'Редактирование ролей', 'Возможность редактировать роли', 'roles'),
        ('roles.delete', 'Удаление ролей', 'Возможность удалять роли', 'roles'),
        ('events.view', 'Просмотр мероприятий', 'Возможность просматривать мероприятия', 'events'),
        ('events.create', 'Создание мероприятий', 'Возможность создавать мероприятия', 'events'),
        ('events.edit', 'Редактирование мероприятий', 'Возможность редактировать мероприятия', 'events'),
        ('events.delete', 'Удаление мероприятий', 'Возможность удалять мероприятия', 'events'),
        ('settings.view', 'Просмотр настроек', 'Возможность просматривать настройки системы', 'settings'),
        ('settings.edit', 'Редактирование настроек', 'Возможность редактировать настройки системы', 'settings'),
        ('moderate.content', 'Модерация контента', 'Возможность модерировать контент пользователей', 'moderation'),
        ('moderate.users', 'Модерация пользователей', 'Возможность модерировать пользователей', 'moderation'),
    ]
    for perm_name, perm_display, perm_desc, perm_category in default_permissions:
        c.execute(
            '''
            INSERT OR IGNORE INTO permissions (name, display_name, description, category)
            VALUES (?, ?, ?, ?)
            ''',
            (perm_name, perm_display, perm_desc, perm_category),
        )

    default_categories = [
        ('general', 'Общие вопросы', 'Общие вопросы о проекте', 10),
        ('events', 'Мероприятия', 'Вопросы о мероприятиях', 20),
        ('profile', 'Профиль и настройки', 'Вопросы о профиле и настройках', 30),
        ('technical', 'Технические вопросы', 'Технические вопросы и помощь', 40),
        ('security', 'Безопасность', 'Безопасность и конфиденциальность', 50),
    ]
    for name, display_name, description, sort_order in default_categories:
        c.execute(
            '''
            INSERT OR IGNORE INTO faq_categories (name, display_name, description, sort_order, is_active)
            VALUES (?, ?, ?, ?, 1)
            ''',
            (name, display_name, description, sort_order),
        )

    default_settings = [
        ('admin_user_ids', ','.join(map(str, ADMIN_USER_IDS)), 'ID администраторов по умолчанию (через запятую)', 'system'),
        ('project_name', 'Анонимные Деды Морозы', 'Название проекта', 'general'),
        ('site_title', 'Анонимные Деды Морозы', 'Заголовок сайта (title)', 'general'),
        ('site_description', 'Проект для организации анонимных подарков', 'Описание сайта (meta description)', 'general'),
        ('logo_text', 'Анонимные Деды Морозы', 'Надпись рядом с логотипом', 'general'),
        ('default_theme', 'dark', 'Тема по умолчанию (light или dark)', 'general'),
        ('site_icon', '🎅', 'Иконка сайта (favicon)', 'general'),
        ('site_logo', '🎅', 'Логотип сайта', 'general'),
        ('accent_color', '#007bff', 'Основной цвет интерфейса (светлая тема)', 'design'),
        ('accent_color_hover', '#0056b3', 'Цвет при наведении (светлая тема)', 'design'),
        ('accent_color_dark', '#4a9eff', 'Основной цвет интерфейса (темная тема)', 'design'),
        ('accent_color_hover_dark', '#357abd', 'Цвет при наведении (темная тема)', 'design'),
        ('dadata_api_key', '', 'Dadata API ключ', 'integrations'),
        ('dadata_secret_key', '', 'Dadata Secret ключ', 'integrations'),
        ('dadata_enabled', '0', 'Dadata интеграция включена', 'integrations'),
        ('dadata_verified', '0', 'Dadata ключи проверены', 'integrations'),
        ('site_url', '', 'Базовый URL сайта (для Telegram бота и ссылок)', 'integrations'),
        ('gwars_domain_map', json.dumps(DEFAULT_GWARS_DOMAIN_MAP, ensure_ascii=False), 'Карта доменов-зеркал GWars (JSON)', 'integrations'),
    ]
    for key, value, description, category in default_settings:
        c.execute(
            '''
            INSERT OR IGNORE INTO settings (key, value, description, category)
            VALUES (?, ?, ?, ?)
            ''',
            (key, value, description, category),
        )
        if key in ('site_icon', 'site_logo'):
            c.execute(
                '''
                UPDATE settings
                SET value = ?
                WHERE key = ? AND (value IS NULL OR value = '' OR value LIKE '/static/uploads/%')
                ''',
                (value, key),
            )

    existing_map = c.execute(
        'SELECT value FROM settings WHERE key = ?', ('gwars_domain_map',)
    ).fetchone()
    legacy_host = c.execute('SELECT value FROM settings WHERE key = ?', ('gwars_host',)).fetchone()
    legacy_site_id = c.execute('SELECT value FROM settings WHERE key = ?', ('gwars_site_id',)).fetchone()

    if not existing_map or not existing_map['value']:
        if legacy_host and legacy_host['value'] and legacy_site_id and legacy_site_id['value']:
            try:
                migrated_map = [{
                    'host': legacy_host['value'].strip().lower(),
                    'site_id': int(legacy_site_id['value']),
                    'primary': True,
                }]
                c.execute(
                    'UPDATE settings SET value = ? WHERE key = ?',
                    (json.dumps(migrated_map, ensure_ascii=False), 'gwars_domain_map'),
                )
            except (TypeError, ValueError):
                pass

    c.execute('DELETE FROM settings WHERE key IN (?, ?)', ('gwars_host', 'gwars_site_id'))

    default_menu_items = [
        ('Мероприятия', 'command', 'events', 10, 1),
        ('Задания', 'command', 'assignments', 20, 1),
        ('FAQ', 'url', '/faq', 30, 1),
        ('Правила', 'url', '/rules', 40, 1),
    ]
    for button_text, button_type, action, sort_order, is_active in default_menu_items:
        c.execute(
            '''
            INSERT OR IGNORE INTO telegram_bot_menu
            (button_text, button_type, action, sort_order, is_active)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (button_text, button_type, action, sort_order, is_active),
        )

    try:
        c.execute(
            '''
            UPDATE settings
            SET value = 'dark'
            WHERE key = 'default_theme' AND value = 'light'
            '''
        )
        c.execute(
            '''
            UPDATE settings
            SET value = 'ru'
            WHERE key = 'default_language' AND (value IS NULL OR value = '' OR value != 'ru')
            '''
        )
        c.execute(
            '''
            UPDATE users
            SET language = 'ru'
            WHERE language IS NULL OR language = ''
            '''
        )
    except sqlite3.OperationalError as exc:
        log_error(f'Migration error (non-critical): {exc}')

    rating_defaults = [
        ('rating_contact_telegram', '1', 'Очки за заполненный Telegram', 'rating'),
        ('rating_contact_whatsapp', '1', 'Очки за заполненный WhatsApp', 'rating'),
        ('rating_contact_viber', '1', 'Очки за заполненный Viber', 'rating'),
        ('rating_event_registration', '1', 'Очки за регистрацию на мероприятие (начисляются автоматически всем зарегистрированным участникам при закрытии регистрации администратором)', 'rating'),
        ('rating_event_gift_not_sent', '0', 'Очки за неотправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации не отправили подарок)', 'rating'),
        ('rating_event_gift_sent', '0', 'Очки за отправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации отправили подарок)', 'rating'),
    ]
    try:
        for key, value, description, category in rating_defaults:
            existing = c.execute('SELECT key FROM settings WHERE key = ?', (key,)).fetchone()
            if not existing:
                c.execute(
                    '''
                    INSERT INTO settings (key, value, description, category)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (key, value, description, category),
                )
    except Exception as exc:
        log_error(f'Error initializing rating settings: {exc}')

    system_roles = [
        ('admin', 'Администратор', 'Полный доступ ко всем функциям системы', 1),
        ('moderator', 'Модератор', 'Права на модерацию контента', 1),
        ('user', 'Пользователь', 'Обычный пользователь', 1),
        ('guest', 'Гость', 'Неавторизованный пользователь', 1),
    ]
    for role_name, display_name, description, is_system in system_roles:
        c.execute(
            '''
            INSERT OR IGNORE INTO roles (name, display_name, description, is_system)
            VALUES (?, ?, ?, ?)
            ''',
            (role_name, display_name, description, is_system),
        )
