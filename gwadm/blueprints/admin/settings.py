"""Admin: settings."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/settings', methods=['GET', 'POST'])
@require_role('admin')
def admin_settings():
    """Страница настроек"""
    # Инициализируем дефолтные тексты модальных окон, если их еще нет
    init_default_modal_texts()
    
    conn = get_db_connection()
    
    if request.method == 'POST':
        # Обработка настройки локализации
        if 'default_language' in request.form:
            default_language = request.form.get('default_language', 'ru').strip()
            if default_language in app.config['LANGUAGES']:
                set_setting('default_language', default_language, 'Язык по умолчанию (ru или en)', 'general')
        
        # Обновляем настройки
        settings_dict = {}
        for key in request.form:
            if key.startswith('setting_'):
                setting_key = key.replace('setting_', '')
                # Для checkbox используем последнее значение (если есть несколько с одинаковым именем)
                setting_values = request.form.getlist(key)
                setting_value = setting_values[-1] if setting_values else request.form.get(key, '0')
                settings_dict[setting_key] = setting_value

        if 'gwars_domain_map' in settings_dict:
            try:
                validated_map = parse_domain_map(settings_dict['gwars_domain_map'])
                settings_dict['gwars_domain_map'] = serialize_domain_map(validated_map)
            except ValueError as exc:
                flash(f'Ошибка в карте доменов GWars: {exc}', 'error')
                conn.close()
                return redirect(url_for('admin.admin_settings') + '#integrations-gwars')
        
        # Сохраняем настройки
        for key, value in settings_dict.items():
            try:
                # Если изменяются API ключи, сбрасываем флаг проверки
                if key in ('dadata_api_key', 'dadata_secret_key'):
                    # Получаем текущее значение
                    current_setting = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
                    if current_setting and current_setting['value'] != value:
                        # Ключ изменился, сбрасываем флаг проверки и отключаем интеграцию
                        conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('0', 'dadata_verified'))
                        conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('0', 'dadata_enabled'))
                
                # Если изменяются SMTP настройки, сбрасываем флаг проверки
                if key in ('smtp_host', 'smtp_port', 'smtp_username', 'smtp_password', 'smtp_use_tls'):
                    # Получаем текущее значение
                    current_setting = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
                    if current_setting and current_setting['value'] != value:
                        # Настройка изменилась, сбрасываем флаг проверки и отключаем SMTP
                        conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('0', 'smtp_verified'))
                        conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('0', 'smtp_enabled'))
                
                # Если изменяется токен Telegram бота, сбрасываем флаг проверки
                if key == 'telegram_bot_token':
                    # Получаем текущее значение
                    current_setting = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
                    if current_setting and current_setting['value'] != value:
                        # Токен изменился, сбрасываем флаг проверки и отключаем бота
                        conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('0', 'telegram_verified'))
                        conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('0', 'telegram_enabled'))
                
                # Проверяем, существует ли настройка
                existing_setting = conn.execute('SELECT key FROM settings WHERE key = ?', (key,)).fetchone()
                if existing_setting:
                    # Обновляем существующую настройку
                    conn.execute('''
                        UPDATE settings 
                        SET value = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                        WHERE key = ?
                    ''', (value, session.get('user_id'), key))
                else:
                    # Создаем новую настройку
                    # Определяем категорию настройки
                    category = 'general'
                    if key.startswith('dadata_'):
                        category = 'integrations'
                    elif key.startswith('smtp_'):
                        category = 'integrations'
                    elif key.startswith('telegram_'):
                        category = 'integrations'
                    elif key == 'gwars_domain_map':
                        category = 'integrations'
                    elif key.startswith('rating_'):
                        category = 'rating'
                    
                    # Получаем описание настройки, если оно есть
                    description = ''
                    if key == 'telegram_bot_token':
                        description = 'Токен бота от @BotFather в Telegram'
                    elif key == 'telegram_chat_id':
                        description = 'Chat ID для отправки уведомлений'
                    elif key == 'telegram_enabled':
                        description = 'Включить интеграцию Telegram'
                    elif key == 'telegram_verified':
                        description = 'Флаг проверки Telegram интеграции'
                    
                    # Проверяем структуру таблицы settings
                    columns_info = conn.execute("PRAGMA table_info(settings)").fetchall()
                    columns = [col[1] for col in columns_info]
                    
                    if 'created_at' in columns and 'created_by' in columns and 'updated_at' in columns and 'updated_by' in columns:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category, created_at, created_by, updated_at, updated_by)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP, ?)
                        ''', (key, value, description, category, session.get('user_id'), session.get('user_id')))
                    elif 'updated_at' in columns and 'updated_by' in columns:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category, updated_at, updated_by)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                        ''', (key, value, description, category, session.get('user_id')))
                    elif 'updated_at' in columns:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category, updated_at)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (key, value, description, category))
                    else:
                        conn.execute('''
                            INSERT INTO settings (key, value, description, category)
                            VALUES (?, ?, ?, ?)
                        ''', (key, value, description, category))
            except Exception as e:
                log_error(f"Error updating setting {key}: {e}")
        
        conn.commit()
        # Возвращаем иконку/логотип к дефолтной эмодзи
        conn.execute('''
            UPDATE settings 
            SET value = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
            WHERE key IN ('site_icon', 'site_logo')
        ''', ('🎅', session.get('user_id')))
        conn.commit()
        flash('Настройки успешно сохранены', 'success')
        conn.close()
        return redirect(url_for('admin.admin_settings'))
    
    # Получаем все настройки, сгруппированные по категориям (исключаем rating - она в отдельной странице)
    settings = conn.execute('''
        SELECT * FROM settings 
        WHERE category != 'rating' OR category IS NULL
        ORDER BY category, key
    ''').fetchall()
    
    # Группируем по категориям
    settings_by_category = {}
    # Создаем словарь для быстрого доступа к настройкам
    settings_dict = {}
    for setting in settings:
        setting_dict = dict(setting)
        category = setting['category'] or 'general'
        # Пропускаем категорию rating - она в отдельной странице
        if category == 'rating':
            continue
        if category not in settings_by_category:
            settings_by_category[category] = []
        settings_by_category[category].append(setting_dict)
        settings_dict[setting['key']] = setting_dict
    
    # Получаем настройки локализации для вкладки
    default_language = get_setting('default_language', 'ru')
    available_languages = app.config.get('LANGUAGES', {'ru': 'Русский', 'en': 'English'})
    try:
        current_locale = get_locale()
    except Exception:
        current_locale = 'ru'
    
    # Получаем список всех администраторов
    admin_users = []
    try:
        # Получаем всех пользователей с ролью admin
        admin_role = conn.execute('SELECT id FROM roles WHERE name = ?', ('admin',)).fetchone()
        if admin_role:
            admin_user_rows = conn.execute('''
                SELECT DISTINCT u.user_id 
                FROM users u
                INNER JOIN user_roles ur ON u.user_id = ur.user_id
                WHERE ur.role_id = ?
                ORDER BY u.user_id
            ''', (admin_role['id'],)).fetchall()
            admin_users = [row['user_id'] for row in admin_user_rows]
    except Exception as e:
        log_error(f"Error fetching admin users: {e}")
    
    # Получаем список всех системных ролей
    system_roles = []
    try:
        role_rows = conn.execute('''
            SELECT name, display_name 
            FROM roles 
            WHERE is_system = 1 
            ORDER BY name
        ''').fetchall()
        system_roles = [{'name': row['name'], 'display_name': row['display_name']} for row in role_rows]
    except Exception as e:
        log_error(f"Error fetching system roles: {e}")
    
    # Получаем список всех системных званий
    system_titles = []
    try:
        title_rows = conn.execute('''
            SELECT name, display_name, icon 
            FROM titles 
            WHERE is_system = 1 
            ORDER BY name
        ''').fetchall()
        system_titles = [{'name': row['name'], 'display_name': row['display_name'], 'icon': row['icon']} for row in title_rows]
    except Exception as e:
        log_error(f"Error fetching system titles: {e}")
    
    # Получаем список всех званий (системных и кастомных) для управления
    all_titles = []
    try:
        title_rows = conn.execute('''
            SELECT t.*, 
                   (SELECT COUNT(*) FROM user_titles WHERE title_id = t.id) as user_count
            FROM titles t
            ORDER BY t.is_system DESC, t.display_name
        ''').fetchall()
        all_titles = [dict(row) for row in title_rows]
    except Exception as e:
        log_error(f"Error fetching all titles: {e}")
    
    # Получаем меню бота
    bot_menu_items = []
    try:
        bot_menu_rows = conn.execute('''
            SELECT id, button_text, button_type, action, sort_order, is_active
            FROM telegram_bot_menu
            ORDER BY sort_order ASC
        ''').fetchall()
        bot_menu_items = [dict(row) for row in bot_menu_rows]
    except sqlite3.OperationalError as e:
        # Таблица может не существовать, если БД не была инициализирована
        log_error(f"Error fetching bot menu: {e}")
        # Попробуем создать таблицу, если её нет
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS telegram_bot_menu (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    button_text TEXT NOT NULL,
                    button_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    sort_order INTEGER DEFAULT 100,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            log_debug("Created telegram_bot_menu table")
        except Exception as create_error:
            log_error(f"Error creating telegram_bot_menu table: {create_error}")
        bot_menu_items = []
    except Exception as e:
        log_error(f"Unexpected error fetching bot menu: {e}")
        import traceback
        log_error(traceback.format_exc())
        bot_menu_items = []
    
    conn.close()
    
    try:
        return render_template('admin/settings.html', 
                             settings_by_category=settings_by_category,
                             settings_dict=settings_dict,
                             default_language=default_language,
                             available_languages=available_languages,
                             current_locale=current_locale,
                             BABEL_AVAILABLE=BABEL_AVAILABLE,
                             admin_users=admin_users,
                             system_roles=system_roles,
                             system_titles=system_titles,
                             all_titles=all_titles,
                             bot_menu_items=bot_menu_items,
                             gwars_domain_entries=load_gwars_domain_map())
    except Exception as e:
        log_error(f"Error rendering admin/settings.html: {e}")
        import traceback
        log_error(traceback.format_exc())
        raise
