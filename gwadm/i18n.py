"""Internationalization helpers and Russian fallback translations."""

from flask import current_app, session

from gwadm.db import get_db_connection
from gwadm.logging_config import log_error

_RUSSIAN_TRANSLATIONS = {
    'Home': 'Главная',
    'Events': 'Мероприятия',
    'Participants': 'Участники',
    'FAQ': 'FAQ',
    'Admin Panel': 'Админ-панель',
    'Users': 'Пользователи',
    'Roles': 'Роли',
    'Titles': 'Звания',
    'Settings': 'Настройки',
    'Localization': 'Локализация',
    'Profile': 'Профиль',
    'Logout': 'Выйти',
    'Login via GWars': 'Войти через GWars',
    'Edit Profile': 'Редактировать профиль',
    'Main': 'Основное',
    'Contacts': 'Контакты',
    'About': 'О себе',
    'User Profile': 'Профиль пользователя',
    'User ID:': 'ID пользователя:',
    'Name:': 'Имя:',
    'Level:': 'Уровень:',
    'Syndicate:': 'Синдикат:',
    'Gender:': 'Пол:',
    'Passport:': 'Паспорт:',
    'Mobile:': 'Мобильный:',
    'Last login:': 'Последний вход:',
    'Yes': 'Есть',
    'No': 'Нет',
    'Not specified': 'Не указан',
    'Contact information not specified': 'Контактная информация не указана',
    'Additional information not specified': 'Дополнительная информация не указана',
    'Toggle theme': 'Переключить тему',
}


def get_locale():
    """Определяет текущую локаль. Для неавторизованных — русский."""
    try:
        if 'user_id' in session:
            try:
                conn = get_db_connection()
                user = conn.execute(
                    'SELECT language FROM users WHERE user_id = ?',
                    (session['user_id'],),
                ).fetchone()
                conn.close()
                languages = current_app.config.get('LANGUAGES', {})
                if user and dict(user).get('language') and user['language'] in languages:
                    return user['language']
            except Exception as e:
                log_error(f"Error getting user language: {e}")
    except Exception:
        pass
    return 'ru'


def _(text):
    """Функция перевода — fallback на русский словарь."""
    return _RUSSIAN_TRANSLATIONS.get(text, text)


def format_date(date, format=None):
    """Форматирование даты (fallback)."""
    return str(date)


def format_datetime(datetime, format=None):
    """Форматирование даты и времени (fallback)."""
    return str(datetime)
