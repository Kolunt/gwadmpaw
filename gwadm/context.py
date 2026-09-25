"""Jinja context processors."""

from flask import session

from gwadm.config import is_dev_login_enabled, is_production
from gwadm.db import get_db_connection
from gwadm.i18n import _, get_locale
from gwadm.logging_config import log_error
from gwadm.services.awards import get_user_awards
from gwadm.services.avatars import get_avatar_url
from gwadm.services.permissions import get_role_permissions
from gwadm.services.settings import get_setting
from gwadm.services.titles import get_user_titles


def register_context_processors(app):
    @app.context_processor
    def inject_default_theme():
        """Добавляет настройку темы по умолчанию и функции во все шаблоны"""
        try:
            default_theme = get_setting('default_theme', 'dark')
            # Получаем аватар текущего пользователя для хэдера
            current_user_avatar_seed = None
            current_user_avatar_style = None
            if 'user_id' in session:
                try:
                    conn = get_db_connection()
                    user = conn.execute('SELECT avatar_seed, avatar_style FROM users WHERE user_id = ?', (session['user_id'],)).fetchone()
                    if user:
                        current_user_avatar_seed = user['avatar_seed']
                        current_user_avatar_style = user['avatar_style']
                    conn.close()
                except Exception as e:
                    log_error(f"Error getting user avatar in context processor: {e}")

            # Получаем текущую локаль
            try:
                current_locale = get_locale()
            except Exception:
                current_locale = 'ru'
            available_languages = app.config.get('LANGUAGES', {'ru': 'Русский', 'en': 'English'})

            # Получаем цвета из настроек
            accent_color = get_setting('accent_color', '#007bff')
            accent_color_hover = get_setting('accent_color_hover', '#0056b3')
            accent_color_dark = get_setting('accent_color_dark', '#4a9eff')
            accent_color_hover_dark = get_setting('accent_color_hover_dark', '#357abd')

            return dict(
                default_theme=default_theme, 
                get_avatar_url=get_avatar_url,
                current_user_avatar_seed=current_user_avatar_seed,
                current_user_avatar_style=current_user_avatar_style,
                get_role_permissions=get_role_permissions,
                get_setting=get_setting,
                get_user_titles=get_user_titles,
                get_user_awards=get_user_awards,
                _=_,
                current_locale=current_locale,
                accent_color=accent_color,
                accent_color_hover=accent_color_hover,
                accent_color_dark=accent_color_dark,
                accent_color_hover_dark=accent_color_hover_dark,
                available_languages=available_languages
            )
        except Exception as e:
            log_error(f"Error in context processor: {e}")
            # Возвращаем минимальный набор значений в случае ошибки
            return dict(
                default_theme='dark',
                get_avatar_url=get_avatar_url,
                current_user_avatar_seed=None,
                current_user_avatar_style=None,
                get_role_permissions=get_role_permissions,
                get_setting=get_setting,
                get_user_titles=get_user_titles,
                get_user_awards=get_user_awards,
                _=_,
                current_locale='ru',
                accent_color='#007bff',
                accent_color_hover='#0056b3',
                accent_color_dark='#4a9eff',
                accent_color_hover_dark='#357abd',
                available_languages={'ru': 'Русский', 'en': 'English'}
            )

    @app.context_processor

    @app.context_processor
    def inject_common_flags():
        return {
            'is_production': is_production(),
            'is_dev_login_enabled': is_dev_login_enabled(),
            'app_config': app.config,
        }

