from flask import(
    Flask, render_template, redirect, url_for, request, session,
    flash, jsonify, send_file, Response, abort, has_request_context,
    make_response
)
from urllib.parse import unquote, unquote_plus, unquote_to_bytes, quote
import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone
import os
import logging
from functools import wraps
from version import __version__
import secrets
import json
import random
from collections import defaultdict
import re
try:
    import requests
except ImportError:
    requests = None
from werkzeug.exceptions import HTTPException
from werkzeug.utils import secure_filename
import traceback

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['VERSION'] = __version__

EVENT_TIME_OFFSET_HOURS = 0
try:
    EVENT_TIME_OFFSET_HOURS = int(os.getenv('EVENT_TIME_OFFSET_HOURS', '3'))
except ValueError:
    EVENT_TIME_OFFSET_HOURS = 0


def get_event_now():
    return datetime.utcnow() + timedelta(hours=EVENT_TIME_OFFSET_HOURS)

def parse_event_datetime(value):
    """Безопасно парсит сохранённые в БД даты этапов в объект datetime."""
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    value_str = str(value).strip()
    if not value_str:
        return None

    formats = (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%d %H:%M',
    )

    for fmt in formats:
        try:
            return datetime.strptime(value_str, fmt)
        except ValueError:
            continue

    try:
        result = datetime.fromisoformat(value_str)
        if result.tzinfo is not None:
            result = result.astimezone(timezone.utc).replace(tzinfo=None)
            if EVENT_TIME_OFFSET_HOURS:
                result += timedelta(hours=EVENT_TIME_OFFSET_HOURS)
        return result
    except ValueError:
        return None

@app.template_filter('format_gender')
def format_gender(value):
    """Convert gender codes (0/1) into human-readable labels."""
    if value is None:
        return 'Не указан'

    value_str = str(value).strip()

    if value_str == '0':
        return 'Тычинки'
    if value_str == '1':
        return 'Пестики'

    return value_str or 'Не указан'

LETTER_UPLOAD_RELATIVE = 'uploads/letter_attachments'
LETTER_UPLOAD_FOLDER = os.path.join(app.static_folder, 'uploads', 'letter_attachments')
ASSIGNMENT_RECEIPT_RELATIVE = 'uploads/assignment_receipts'
ASSIGNMENT_RECEIPT_FOLDER = os.path.join(app.static_folder, 'uploads', 'assignment_receipts')
ALLOWED_LETTER_IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp'}
os.makedirs(LETTER_UPLOAD_FOLDER, exist_ok=True)
os.makedirs(ASSIGNMENT_RECEIPT_FOLDER, exist_ok=True)

# Настройка логирования (должно быть перед использованием log_error)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Для PythonAnywhere также используем print (видно в error log)
def log_error(msg):
    """Логирует ошибку через logger и print для PythonAnywhere"""
    logger.error(msg)
    print(msg, flush=True)  # flush=True для немедленного вывода

def log_debug(msg):
    """Логирует отладочную информацию через logger и print"""
    logger.debug(msg)
    print(msg, flush=True)


def log_activity(action, details=None, metadata=None, user_id=None, username=None):
    """Сохраняет информацию о действии пользователя в таблицу activity_logs"""
    if not action:
        return
    
    conn = None
    try:
        meta_dict = {}
        if metadata:
            if isinstance(metadata, dict):
                meta_dict.update(metadata)
            else:
                meta_dict['data'] = metadata
        
        ip_address = None
        if has_request_context():
            if user_id is None:
                user_id = session.get('user_id')
            if username is None:
                username = session.get('username')
            ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
            if ip_address and ',' in str(ip_address):
                ip_address = ip_address.split(',')[0].strip()
            meta_dict.setdefault('endpoint', request.endpoint)
            meta_dict.setdefault('path', request.path)
            meta_dict.setdefault('method', request.method)
            impersonation_original = session.get('impersonation_original')
            if impersonation_original:
                meta_dict.setdefault('impersonator_id', impersonation_original.get('user_id'))
                meta_dict.setdefault('impersonator_username', impersonation_original.get('username'))
        
        metadata_json = json.dumps(meta_dict, ensure_ascii=False) if meta_dict else None
        
        conn = get_db_connection()
        conn.execute('''
            INSERT INTO activity_logs (user_id, username, action, details, metadata, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, username, action, details, metadata_json, ip_address))
        conn.commit()
    except Exception as e:
        log_error(f"Error logging activity '{action}': {e}")
    finally:
        if conn:
            conn.close()

# Настройка локализации
app.config['LANGUAGES'] = {
    'ru': 'Русский',
    'en': 'English'
}
app.config['BABEL_DEFAULT_LOCALE'] = 'ru'
app.config['BABEL_DEFAULT_TIMEZONE'] = 'Europe/Moscow'
app.config['BABEL_TRANSLATION_DIRECTORIES'] = 'translations'

# Словарь русских переводов для fallback (используется всегда)
_russian_translations = {
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
    """Определяет текущую локаль из настроек. Всегда возвращает русский для неавторизованных пользователей."""
    try:
        # Если пользователь авторизован, проверяем его настройку языка
        from flask import session
        if 'user_id' in session:
            try:
                conn = get_db_connection()
                user = conn.execute('SELECT language FROM users WHERE user_id = ?', (session['user_id'],)).fetchone()
                conn.close()
                if user and dict(user).get('language') and user['language'] in app.config['LANGUAGES']:
                    return user['language']
            except Exception as e:
                log_error(f"Error getting user language: {e}")
    except Exception:
        # Если session недоступен (например, вне контекста запроса)
        pass
    
    # Для неавторизованных пользователей всегда используем русский
    return 'ru'

def _(text):
    """Функция перевода - всегда использует русские переводы из словаря"""
    # Всегда используем русские переводы из словаря
    return _russian_translations.get(text, text)

def format_date(date, format=None):
    """Форматирование даты (fallback)"""
    return str(date)

def format_datetime(datetime, format=None):
    """Форматирование даты и времени (fallback)"""
    return str(datetime)

BABEL_AVAILABLE = False
try:
    from flask_babel import Babel
    babel = Babel(app)
    BABEL_AVAILABLE = True
    
    @babel.localeselector
    def babel_get_locale():
        """Определяет локаль для Flask-Babel"""
        try:
            return get_locale()
        except Exception:
            return 'ru'
    
except ImportError:
    # Flask-Babel не установлен - используем fallback функции
    BABEL_AVAILABLE = False
except Exception as e:
    # Любая другая ошибка при инициализации Babel
    log_error(f"Error initializing Babel: {e}")
    BABEL_AVAILABLE = False

# Константы для GWars авторизации
GWARS_PASSWORD = "deadmoroz"
GWARS_HOST = "gwadm.pythonanywhere.com"
GWARS_SITE_ID = 4

# ID администраторов по умолчанию
ADMIN_USER_IDS = [283494, 240139]

# Инициализация базы данных
_db_initialized = False
_db_path = None

def get_db_path():
    """Определяет путь к базе данных"""
    global _db_path
    if _db_path is None:
        # На PythonAnywhere используем абсолютный путь в домашней директории
        if os.path.exists('/home/gwadm'):
            # Мы на PythonAnywhere
            _db_path = '/home/gwadm/gwadm/database.db'
        else:
            # Локально используем относительный путь
            _db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database.db')
    return _db_path

def init_db():
    """Инициализирует базу данных, создавая таблицы если их нет"""
    global _db_initialized
    try:
        db_path = get_db_path()
        log_debug(f"Initializing database at: {db_path}")
        
        # Создаем директорию если её нет
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # Таблица пользователей
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                username TEXT NOT NULL,
                level INTEGER,
                synd INTEGER,
                has_passport INTEGER,
                has_mobile INTEGER,
                old_passport INTEGER,
                usersex TEXT,
                avatar_seed TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        ''')
        
        # Добавляем колонку avatar_seed если её нет (миграция для существующих БД)
        try:
            c.execute('ALTER TABLE users ADD COLUMN avatar_seed TEXT')
        except sqlite3.OperationalError:
            # Колонка уже существует, это нормально
            pass
        
        # Добавляем колонку language если её нет (миграция)
        try:
            c.execute('ALTER TABLE users ADD COLUMN language TEXT')
        except sqlite3.OperationalError:
            # Колонка уже существует, это нормально
            pass
        
        # Добавляем пользовательские поля для редактирования профиля (миграция)
        user_editable_fields = ['bio', 'contact_info', 'avatar_style', 'email', 'phone', 'telegram', 'whatsapp', 'viber',
                                'last_name', 'first_name', 'middle_name',  # Личные данные
                                'postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment']  # Адрес
        for field in user_editable_fields:
            try:
                c.execute(f'ALTER TABLE users ADD COLUMN {field} TEXT')
            except sqlite3.OperationalError:
                # Колонка уже существует, это нормально
                pass
        
        # Добавляем поля блокировки пользователя (миграция)
        try:
            c.execute('ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE users ADD COLUMN blocked_by INTEGER')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE users ADD COLUMN blocked_reason TEXT')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE users ADD COLUMN blocked_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass
        
        # Таблица ролей
        c.execute('''
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT,
                is_system INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Связь пользователей и ролей (многие ко многим)
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                role_id INTEGER NOT NULL,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                assigned_by INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_by) REFERENCES users(user_id),
                UNIQUE(user_id, role_id)
            )
        ''')
        
        # Таблица прав (permissions)
        c.execute('''
            CREATE TABLE IF NOT EXISTS permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT,
                category TEXT DEFAULT 'general',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Связь ролей и прав (многие ко многим)
        c.execute('''
            CREATE TABLE IF NOT EXISTS role_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role_id INTEGER NOT NULL,
                permission_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
                FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE,
                UNIQUE(role_id, permission_id)
            )
        ''')
        
        # Таблица званий (titles)
        c.execute('''
            CREATE TABLE IF NOT EXISTS titles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT,
                color TEXT DEFAULT '#007bff',
                icon TEXT,
                is_system INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Связь пользователей и званий (многие ко многим)
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_titles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title_id INTEGER NOT NULL,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                assigned_by INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (title_id) REFERENCES titles(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_by) REFERENCES users(user_id),
                UNIQUE(user_id, title_id)
            )
        ''')
        
        # Таблица наград
        c.execute('''
            CREATE TABLE IF NOT EXISTS awards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                icon TEXT,
                image TEXT,
                sort_order INTEGER DEFAULT 100,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER,
                FOREIGN KEY (created_by) REFERENCES users(user_id)
            )
        ''')
        
        # Миграция: добавляем поле icon в таблицу awards, если его нет
        try:
            c.execute('ALTER TABLE awards ADD COLUMN icon TEXT')
        except sqlite3.OperationalError:
            # Колонка уже существует, это нормально
            pass
        
        # Таблица связи пользователей и наград
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_awards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                award_id INTEGER NOT NULL,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                assigned_by INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (award_id) REFERENCES awards(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_by) REFERENCES users(user_id),
                UNIQUE(user_id, award_id)
            )
        ''')
        
        # Инициализация стандартных званий
        default_titles = [
            ('author', 'Автор идеи', 'Автор идеи проекта', '#28a745', '💡', 1),
            ('developer', 'Разработчик', 'Разработчик проекта', '#007bff', '💻', 1),
            ('ambassador', 'Амбассадор', 'Амбассадор проекта', '#ffc107', '⭐', 1),
            ('designer', 'Дизайнер', 'Дизайнер проекта', '#e83e8c', '🎨', 1),
        ]
        
        for title_name, title_display, title_desc, title_color, title_icon, is_system in default_titles:
            c.execute('''
                INSERT OR IGNORE INTO titles (name, display_name, description, color, icon, is_system)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (title_name, title_display, title_desc, title_color, title_icon, is_system))
        
        # Инициализация стандартных прав
        default_permissions = [
            # Управление пользователями
            ('users.view', 'Просмотр пользователей', 'Возможность просматривать список пользователей', 'users'),
            ('users.edit', 'Редактирование пользователей', 'Возможность редактировать данные пользователей', 'users'),
            ('users.delete', 'Удаление пользователей', 'Возможность удалять пользователей', 'users'),
            ('users.roles', 'Управление ролями пользователей', 'Возможность назначать роли пользователям', 'users'),
            
            # Управление ролями
            ('roles.view', 'Просмотр ролей', 'Возможность просматривать список ролей', 'roles'),
            ('roles.create', 'Создание ролей', 'Возможность создавать новые роли', 'roles'),
            ('roles.edit', 'Редактирование ролей', 'Возможность редактировать роли', 'roles'),
            ('roles.delete', 'Удаление ролей', 'Возможность удалять роли', 'roles'),
            
            # Управление мероприятиями
            ('events.view', 'Просмотр мероприятий', 'Возможность просматривать мероприятия', 'events'),
            ('events.create', 'Создание мероприятий', 'Возможность создавать мероприятия', 'events'),
            ('events.edit', 'Редактирование мероприятий', 'Возможность редактировать мероприятия', 'events'),
            ('events.delete', 'Удаление мероприятий', 'Возможность удалять мероприятия', 'events'),
            
            # Настройки
            ('settings.view', 'Просмотр настроек', 'Возможность просматривать настройки системы', 'settings'),
            ('settings.edit', 'Редактирование настроек', 'Возможность редактировать настройки системы', 'settings'),
            
            # Модерация
            ('moderate.content', 'Модерация контента', 'Возможность модерировать контент пользователей', 'moderation'),
            ('moderate.users', 'Модерация пользователей', 'Возможность модерировать пользователей', 'moderation'),
        ]
        
        for perm_name, perm_display, perm_desc, perm_category in default_permissions:
            c.execute('''
                INSERT OR IGNORE INTO permissions (name, display_name, description, category)
                VALUES (?, ?, ?, ?)
            ''', (perm_name, perm_display, perm_desc, perm_category))
        
        # Таблица настроек
        c.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                description TEXT,
                category TEXT DEFAULT 'general',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by INTEGER
            )
        ''')
        
        # Таблица логов действий пользователей
        c.execute('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                action TEXT NOT NULL,
                details TEXT,
                metadata TEXT,
                ip_address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица истории рассылок
        c.execute('''
            CREATE TABLE IF NOT EXISTS broadcasts_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_by INTEGER NOT NULL,
                created_by_username TEXT,
                recipient_type TEXT NOT NULL,
                delivery_method TEXT NOT NULL,
                subject TEXT,
                message TEXT NOT NULL,
                total_recipients INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                errors TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица шаблонов рассылок
        c.execute('''
            CREATE TABLE IF NOT EXISTS broadcast_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                delivery_method TEXT NOT NULL,
                subject TEXT,
                message TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                created_by_username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица связи пользователей с Telegram
        c.execute('''
            CREATE TABLE IF NOT EXISTS telegram_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                telegram_chat_id TEXT NOT NULL,
                telegram_username TEXT,
                verification_code TEXT,
                verification_code_expires_at TIMESTAMP,
                verified INTEGER DEFAULT 0,
                verified_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица меню бота
        c.execute('''
            CREATE TABLE IF NOT EXISTS telegram_bot_menu (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                button_text TEXT NOT NULL,
                button_type TEXT NOT NULL,  -- 'command', 'url', 'callback'
                action TEXT NOT NULL,  -- команда или URL или callback_data
                sort_order INTEGER DEFAULT 100,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица мероприятий
        c.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                award_id INTEGER,
                FOREIGN KEY (created_by) REFERENCES users(user_id),
                FOREIGN KEY (award_id) REFERENCES awards(id)
            )
        ''')
        
        # Миграция: добавляем поле award_id если его нет
        try:
            c.execute('ALTER TABLE events ADD COLUMN award_id INTEGER REFERENCES awards(id)')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
        try:
            c.execute('ALTER TABLE events ADD COLUMN deleted_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
        
        # Таблица этапов мероприятий
        c.execute('''
            CREATE TABLE IF NOT EXISTS event_stages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                stage_type TEXT NOT NULL,
                stage_order INTEGER NOT NULL,
                start_datetime TIMESTAMP,
                end_datetime TIMESTAMP,
                is_required INTEGER DEFAULT 0,
                is_optional INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
                UNIQUE(event_id, stage_type)
            )
        ''')
        
        # Таблица регистраций на мероприятия
        c.execute('''
            CREATE TABLE IF NOT EXISTS event_registrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(event_id, user_id)
            )
        ''')

        # Снапшоты данных участника во время регистрации
        c.execute('''
            CREATE TABLE IF NOT EXISTS event_registration_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                last_name TEXT,
                first_name TEXT,
                middle_name TEXT,
                postal_code TEXT,
                country TEXT,
                city TEXT,
                street TEXT,
                house TEXT,
                building TEXT,
                apartment TEXT,
                email TEXT,
                phone TEXT,
                telegram TEXT,
                whatsapp TEXT,
                viber TEXT,
                bio TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(event_id, user_id)
            )
        ''')
        try:
            c.execute('ALTER TABLE event_registration_details ADD COLUMN email TEXT')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_registration_details ADD COLUMN bio TEXT')
        except sqlite3.OperationalError:
            pass
        
        # Таблица начислений «снежинок»
        c.execute('''
            CREATE TABLE IF NOT EXISTS snowflake_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                reason TEXT NOT NULL,
                points INTEGER NOT NULL DEFAULT 1,
                active INTEGER DEFAULT 1,
                manual_revoked INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                revoked_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                UNIQUE(user_id, source)
            )
        ''')
        try:
            c.execute('ALTER TABLE snowflake_events ADD COLUMN points INTEGER NOT NULL DEFAULT 1')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE snowflake_events ADD COLUMN manual_revoked INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE snowflake_events ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE snowflake_events ADD COLUMN revoked_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass
        
        # Таблица утверждений участников (для ревью администратором)
        c.execute('''
            CREATE TABLE IF NOT EXISTS event_participant_approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                approved INTEGER DEFAULT 0,
                approved_at TIMESTAMP,
                approved_by INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (approved_by) REFERENCES users(user_id),
                UNIQUE(event_id, user_id)
            )
        ''')
        
        # Таблица заданий (распределение Деда Мороза и Внучки)
        c.execute('''
            CREATE TABLE IF NOT EXISTS event_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                santa_user_id INTEGER NOT NULL,
                recipient_user_id INTEGER NOT NULL,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                assigned_by INTEGER,
                locked INTEGER DEFAULT 0,
                assignment_locked INTEGER DEFAULT 0,
                santa_sent_at TIMESTAMP,
                santa_send_info TEXT,
                recipient_received_at TIMESTAMP,
                recipient_thanks_message TEXT,
                recipient_receipt_image TEXT,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
                FOREIGN KEY (santa_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (recipient_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_by) REFERENCES users(user_id),
                UNIQUE(event_id, santa_user_id, recipient_user_id)
            )
        ''')

        # Миграция: добавляем поля для статусов отправки/получения подарков
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN santa_sent_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN santa_send_info TEXT')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN recipient_received_at TIMESTAMP')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN locked INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN assignment_locked INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN recipient_thanks_message TEXT')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN recipient_receipt_image TEXT')
        except sqlite3.OperationalError:
            pass

        # Таблица для хранения сообщений переписки
        c.execute('''
            CREATE TABLE IF NOT EXISTS letter_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                assignment_id INTEGER NOT NULL,
                sender TEXT NOT NULL CHECK(sender IN ('santa','grandchild')),
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                attachment_path TEXT,
                FOREIGN KEY (assignment_id) REFERENCES event_assignments(id) ON DELETE CASCADE
            )
        ''')
        try:
            c.execute('ALTER TABLE letter_messages ADD COLUMN attachment_path TEXT')
        except sqlite3.OperationalError:
            pass
        
        # Таблица категорий FAQ
        c.execute('''
            CREATE TABLE IF NOT EXISTS faq_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT,
                sort_order INTEGER DEFAULT 100,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                created_by INTEGER,
                updated_by INTEGER,
                FOREIGN KEY (created_by) REFERENCES users(user_id),
                FOREIGN KEY (updated_by) REFERENCES users(user_id)
            )
        ''')
        
        # Таблица контактов
        c.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                value TEXT NOT NULL,
                icon TEXT,
                description TEXT,
                sort_order INTEGER DEFAULT 100,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                created_by INTEGER,
                updated_by INTEGER,
                FOREIGN KEY (created_by) REFERENCES users(user_id),
                FOREIGN KEY (updated_by) REFERENCES users(user_id)
            )
        ''')
        # Таблица FAQ
        c.execute('''
            CREATE TABLE IF NOT EXISTS faq_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                sort_order INTEGER DEFAULT 100,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                created_by INTEGER,
                updated_by INTEGER,
                FOREIGN KEY (created_by) REFERENCES users(user_id),
                FOREIGN KEY (updated_by) REFERENCES users(user_id)
            )
        ''')
        # Инициализация дефолтных категорий, если их нет
        default_categories = [
            ('general', 'Общие вопросы', 'Общие вопросы о проекте', 10),
            ('events', 'Мероприятия', 'Вопросы о мероприятиях', 20),
            ('profile', 'Профиль и настройки', 'Вопросы о профиле и настройках', 30),
            ('technical', 'Технические вопросы', 'Технические вопросы и помощь', 40),
            ('security', 'Безопасность', 'Безопасность и конфиденциальность', 50),
        ]
        
        for name, display_name, description, sort_order in default_categories:
            c.execute('''
                INSERT OR IGNORE INTO faq_categories (name, display_name, description, sort_order, is_active)
                VALUES (?, ?, ?, ?, 1)
            ''', (name, display_name, description, sort_order))
        
        # Инициализация настроек по умолчанию
        default_settings = [
            ('admin_user_ids', ','.join(map(str, ADMIN_USER_IDS)), 'ID администраторов по умолчанию (через запятую)', 'system'),
            ('project_name', 'Анонимные Деды Морозы', 'Название проекта', 'general'),
            ('site_title', 'Анонимные Деды Морозы', 'Заголовок сайта (title)', 'general'),
            ('site_description', 'Проект для организации анонимных подарков', 'Описание сайта (meta description)', 'general'),
            ('logo_text', 'Анонимные Деды Морозы', 'Надпись рядом с логотипом', 'general'),
            ('default_theme', 'dark', 'Тема по умолчанию (light или dark)', 'general'),
            ('site_icon', '🎅', 'Иконка сайта (favicon)', 'general'),
            ('site_logo', '🎅', 'Логотип сайта', 'general'),
            # Настройки цветов
            ('accent_color', '#007bff', 'Основной цвет интерфейса (светлая тема)', 'design'),
            ('accent_color_hover', '#0056b3', 'Цвет при наведении (светлая тема)', 'design'),
            ('accent_color_dark', '#4a9eff', 'Основной цвет интерфейса (темная тема)', 'design'),
            ('accent_color_hover_dark', '#357abd', 'Цвет при наведении (темная тема)', 'design'),
            # Настройки интеграций
            ('dadata_api_key', '', 'Dadata API ключ', 'integrations'),
            ('dadata_secret_key', '', 'Dadata Secret ключ', 'integrations'),
            ('dadata_enabled', '0', 'Dadata интеграция включена', 'integrations'),
            ('dadata_verified', '0', 'Dadata ключи проверены', 'integrations'),
            ('site_url', '', 'Базовый URL сайта (для Telegram бота и ссылок)', 'integrations'),
        ]
        
        for key, value, description, category in default_settings:
            c.execute('''
                INSERT OR IGNORE INTO settings (key, value, description, category)
                VALUES (?, ?, ?, ?)
            ''', (key, value, description, category))
            if key in ('site_icon', 'site_logo'):
                c.execute('''
                    UPDATE settings 
                    SET value = ? 
                    WHERE key = ? AND (value IS NULL OR value = '' OR value LIKE '/static/uploads/%')
                ''', (value, key))
        
        # Удаляем устаревшие настройки GWars, если они присутствуют
        c.execute('DELETE FROM settings WHERE key IN (?, ?)', ('gwars_host', 'gwars_site_id'))
        
        # Инициализация дефолтного меню бота
        default_menu_items = [
            ('Мероприятия', 'command', 'events', 10, 1),
            ('Задания', 'command', 'assignments', 20, 1),
            ('FAQ', 'url', '/faq', 30, 1),
            ('Правила', 'url', '/rules', 40, 1),
        ]
        
        for button_text, button_type, action, sort_order, is_active in default_menu_items:
            c.execute('''
                INSERT OR IGNORE INTO telegram_bot_menu 
                (button_text, button_type, action, sort_order, is_active)
                VALUES (?, ?, ?, ?, ?)
            ''', (button_text, button_type, action, sort_order, is_active))

        # Обновляем настройки для всех пользователей: темная тема и русский язык по умолчанию
        try:
            # Устанавливаем default_theme на 'dark', если она 'light'
            c.execute('''
                UPDATE settings 
                SET value = 'dark' 
                WHERE key = 'default_theme' AND value = 'light'
            ''')
            # Устанавливаем default_language на 'ru', если не установлен
            c.execute('''
                UPDATE settings 
                SET value = 'ru' 
                WHERE key = 'default_language' AND (value IS NULL OR value = '' OR value != 'ru')
            ''')
            # Устанавливаем русский язык всем пользователям, у которых язык не установлен
            c.execute('''
                UPDATE users 
                SET language = 'ru' 
                WHERE language IS NULL OR language = ''
            ''')
        except sqlite3.OperationalError as e:
            # Игнорируем ошибки миграции
            log_error(f"Migration error (non-critical): {e}")
        
        # Создаем системные роли, если их еще нет
        system_roles = [
            ('admin', 'Администратор', 'Полный доступ ко всем функциям системы', 1),
            ('moderator', 'Модератор', 'Права на модерацию контента', 1),
            ('user', 'Пользователь', 'Обычный пользователь', 1),
            ('guest', 'Гость', 'Неавторизованный пользователь', 1)
        ]
        
        for role_name, display_name, description, is_system in system_roles:
            c.execute('''
                INSERT OR IGNORE INTO roles (name, display_name, description, is_system)
                VALUES (?, ?, ?, ?)
            ''', (role_name, display_name, description, is_system))
        
        conn.commit()
        conn.close()
        _db_initialized = True
        log_debug(f"Database initialized successfully at: {db_path}")
    except Exception as e:
        log_error(f"Error initializing database: {e}")
        raise

def generate_unique_avatar_seed(user_id):
    """Генерирует уникальный seed для аватара пользователя"""
    # Используем комбинацию user_id + случайная строка для уникальности
    random_part = secrets.token_hex(8)
    seed = f"{user_id}_{random_part}"
    return seed

def get_used_avatar_seeds(exclude_user_id=None):
    """Получает список всех используемых avatar_seed в системе"""
    conn = get_db_connection()
    if exclude_user_id:
        used_seeds = conn.execute(
            'SELECT avatar_seed FROM users WHERE avatar_seed IS NOT NULL AND user_id != ?',
            (exclude_user_id,)
        ).fetchall()
    else:
        used_seeds = conn.execute(
            'SELECT avatar_seed FROM users WHERE avatar_seed IS NOT NULL'
        ).fetchall()
    conn.close()
    return set(seed['avatar_seed'] for seed in used_seeds if seed['avatar_seed'])

def generate_unique_avatar_candidates(style, count=20, exclude_user_id=None):
    """Генерирует список уникальных кандидатов аватаров для выбранного стиля"""
    used_seeds = get_used_avatar_seeds(exclude_user_id)
    candidates = []
    attempts = 0
    max_attempts = count * 10  # Лимит попыток
    
    while len(candidates) < count and attempts < max_attempts:
        seed = secrets.token_hex(12)  # Генерируем случайный seed
        if seed not in used_seeds and seed not in candidates:
            candidates.append(seed)
        attempts += 1
    
    return candidates

def get_avatar_url(avatar_seed, style=None, size=128):
    """Генерирует URL аватара DiceBear"""
    if not avatar_seed:
        return None
    if style is None:
        style = 'avataaars'  # Стиль по умолчанию
    return f"https://api.dicebear.com/7.x/{style}/svg?seed={avatar_seed}&size={size}"

def get_user_avatar_url(user, size=128):
    """Получает URL аватара пользователя с учетом его стиля"""
    if not user or not user.get('avatar_seed'):
        return None
    style = user.get('avatar_style') or 'avataaars'
    return get_avatar_url(user['avatar_seed'], style, size)
def ensure_db():
    """Убеждается, что база данных инициализирована"""
    if not _db_initialized:
        init_db()
def get_db_connection():
    """Получает соединение с базой данных"""
    ensure_db()  # Убеждаемся, что БД инициализирована
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# ========== Система ролей и прав доступа ==========

def get_user_roles(user_id):
    """Получает список ролей пользователя"""
    if not user_id:
        return []
    conn = get_db_connection()
    roles = conn.execute('''
        SELECT r.id, r.name, r.display_name, r.description
        FROM roles r
        INNER JOIN user_roles ur ON r.id = ur.role_id
        WHERE ur.user_id = ?
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(role) for role in roles]

def get_user_role_names(user_id):
    """Получает список имен ролей пользователя"""
    if not user_id:
        return ['guest']
    roles = get_user_roles(user_id)
    return [role['name'] for role in roles] if roles else ['user']

def has_role(user_id, role_name):
    """Проверяет, есть ли у пользователя указанная роль"""
    if not user_id:
        return role_name == 'guest'
    role_names = get_user_role_names(user_id)
    return role_name in role_names

def has_any_role(user_id, role_names):
    """Проверяет, есть ли у пользователя хотя бы одна из указанных ролей"""
    if not user_id:
        return 'guest' in role_names
    user_roles = get_user_role_names(user_id)
    return any(role in user_roles for role in role_names)

def assign_role(user_id, role_name, assigned_by=None):
    """Назначает роль пользователю"""
    conn = get_db_connection()
    # Получаем ID роли
    role = conn.execute('SELECT id FROM roles WHERE name = ?', (role_name,)).fetchone()
    if not role:
        conn.close()
        return False
    
    try:
        conn.execute('''
            INSERT OR REPLACE INTO user_roles (user_id, role_id, assigned_by)
            VALUES (?, ?, ?)
        ''', (user_id, role['id'], assigned_by))
        conn.commit()
        log_activity(
            'role_assign',
            details=f'Назначена роль {role_name} пользователю {user_id}',
            metadata={'target_user_id': user_id, 'role': role_name, 'assigned_by': assigned_by},
            user_id=assigned_by
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning role: {e}")
        conn.close()
        return False

def remove_role(user_id, role_name):
    """Удаляет роль у пользователя"""
    conn = get_db_connection()
    role = conn.execute('SELECT id FROM roles WHERE name = ?', (role_name,)).fetchone()
    if not role:
        conn.close()
        return False
    
    try:
        conn.execute('''
            DELETE FROM user_roles
            WHERE user_id = ? AND role_id = ?
        ''', (user_id, role['id']))
        conn.commit()
        log_activity(
            'role_remove',
            details=f'Удалена роль {role_name} у пользователя {user_id}',
            metadata={'target_user_id': user_id, 'role': role_name}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing role: {e}")
        conn.close()
        return False
# ========== Система прав (permissions) ==========
def get_all_permissions():
    """Получает список всех прав"""
    conn = get_db_connection()
    permissions = conn.execute('''
        SELECT * FROM permissions ORDER BY category, display_name
    ''').fetchall()
    conn.close()
    
    # Возвращаем список словарей
    return [dict(perm) for perm in permissions]

def get_role_permissions(role_id):
    """Получает список прав роли"""
    conn = get_db_connection()
    permissions = conn.execute('''
        SELECT p.* FROM permissions p
        INNER JOIN role_permissions rp ON p.id = rp.permission_id
        WHERE rp.role_id = ?
    ''', (role_id,)).fetchall()
    conn.close()
    return [dict(p) for p in permissions]

def assign_permission_to_role(role_id, permission_id):
    """Назначает право роли"""
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR IGNORE INTO role_permissions (role_id, permission_id)
            VALUES (?, ?)
        ''', (role_id, permission_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning permission: {e}")
        conn.close()
        return False

def remove_permission_from_role(role_id, permission_id):
    """Удаляет право у роли"""
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM role_permissions 
            WHERE role_id = ? AND permission_id = ?
        ''', (role_id, permission_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing permission: {e}")
        conn.close()
        return False

def has_permission(user_id, permission_name):
    """Проверяет, есть ли у пользователя указанное право"""
    if not user_id:
        return False
    
    conn = get_db_connection()
    # Получаем роли пользователя
    roles = conn.execute('''
        SELECT r.id FROM roles r
        INNER JOIN user_roles ur ON r.id = ur.role_id
        WHERE ur.user_id = ?
    ''', (user_id,)).fetchall()
    
    if not roles:
        conn.close()
        return False
    
    # Проверяем, есть ли у любой роли пользователя это право
    role_ids = [r['id'] for r in roles]
    placeholders = ','.join(['?'] * len(role_ids))
    
    permission = conn.execute(f'''
        SELECT p.id FROM permissions p
        INNER JOIN role_permissions rp ON p.id = rp.permission_id
        WHERE rp.role_id IN ({placeholders}) AND p.name = ?
    ''', role_ids + [permission_name]).fetchone()
    
    conn.close()
    return permission is not None

# ========== Система званий (titles) ==========

def get_all_titles():
    """Получает список всех званий"""
    conn = get_db_connection()
    titles = conn.execute('''
        SELECT * FROM titles ORDER BY is_system DESC, display_name
    ''').fetchall()
    conn.close()
    return [dict(t) for t in titles]

def get_user_titles(user_id):
    """Получает список званий пользователя"""
    if not user_id:
        return []
    conn = get_db_connection()
    titles = conn.execute('''
        SELECT t.* FROM titles t
        INNER JOIN user_titles ut ON t.id = ut.title_id
        WHERE ut.user_id = ?
        ORDER BY t.display_name
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(t) for t in titles]

def get_users_with_title(title_id):
    """Получает список пользователей, имеющих указанное звание"""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.avatar_seed,
            u.avatar_style,
            u.created_at,
            u.last_login,
            ut.assigned_by,
            ut.assigned_at,
            COALESCE(admin.username, '') AS assigned_by_username
        FROM user_titles ut
        JOIN users u ON ut.user_id = u.user_id
        LEFT JOIN users admin ON ut.assigned_by = admin.user_id
        WHERE ut.title_id = ?
        ORDER BY u.username COLLATE NOCASE
    ''', (title_id,)).fetchall()
    conn.close()

    users = []
    for row in rows:
        record = dict(row)
        users.append(record)
    return users

def get_title_by_name(title_name):
    """Получает звание по имени"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE name = ?', (title_name,)).fetchone()
    conn.close()
    return dict(title) if title else None

def assign_title(user_id, title_id, assigned_by=None):
    """Назначает звание пользователю"""
    if not user_id or not title_id:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR REPLACE INTO user_titles (user_id, title_id, assigned_by)
            VALUES (?, ?, ?)
        ''', (user_id, title_id, assigned_by))
        conn.commit()
        log_activity(
            'title_assign',
            details=f'Назначено звание {title_id} пользователю {user_id}',
            metadata={'target_user_id': user_id, 'title_id': title_id, 'assigned_by': assigned_by},
            user_id=assigned_by
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning title: {e}")
        conn.close()
        return False

def remove_title(user_id, title_id):
    """Удаляет звание у пользователя"""
    if not user_id or not title_id:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM user_titles
            WHERE user_id = ? AND title_id = ?
        ''', (user_id, title_id))
        conn.commit()
        log_activity(
            'title_remove',
            details=f'Удалено звание {title_id} у пользователя {user_id}',
            metadata={'target_user_id': user_id, 'title_id': title_id}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing title: {e}")
        conn.close()
        return False

def get_user_awards(user_id):
    """Получает список наград пользователя"""
    if not user_id:
        return []
    conn = get_db_connection()
    awards = conn.execute('''
        SELECT a.* FROM awards a
        INNER JOIN user_awards ua ON a.id = ua.award_id
        WHERE ua.user_id = ?
        ORDER BY a.sort_order, a.title
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(a) for a in awards]

def get_users_with_award(award_id):
    """Получает список пользователей, имеющих указанную награду"""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.avatar_seed,
            u.avatar_style,
            u.created_at,
            u.last_login,
            ua.assigned_by,
            ua.assigned_at,
            COALESCE(admin.username, '') AS assigned_by_username
        FROM user_awards ua
        JOIN users u ON ua.user_id = u.user_id
        LEFT JOIN users admin ON ua.assigned_by = admin.user_id
        WHERE ua.award_id = ?
        ORDER BY u.username COLLATE NOCASE
    ''', (award_id,)).fetchall()
    conn.close()

    users = []
    for row in rows:
        record = dict(row)
        users.append(record)
    return users

def assign_award(user_id, award_id, assigned_by=None):
    """Назначает награду пользователю"""
    if not user_id or not award_id:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR REPLACE INTO user_awards (user_id, award_id, assigned_by)
            VALUES (?, ?, ?)
        ''', (user_id, award_id, assigned_by))
        conn.commit()
        log_activity(
            'award_assign',
            details=f'Назначена награда {award_id} пользователю {user_id}',
            metadata={'target_user_id': user_id, 'award_id': award_id, 'assigned_by': assigned_by},
            user_id=assigned_by
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning award: {e}")
        conn.close()
        return False

def remove_award(user_id, award_id):
    """Удаляет награду у пользователя"""
    if not user_id or not award_id:
        return False
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM user_awards
            WHERE user_id = ? AND award_id = ?
        ''', (user_id, award_id))
        conn.commit()
        log_activity(
            'award_remove',
            details=f'Удалена награда {award_id} у пользователя {user_id}',
            metadata={'target_user_id': user_id, 'award_id': award_id}
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing award: {e}")
        conn.close()
        return False

# Декораторы для проверки прав доступа
def require_role(role_name):
    """Декоратор для проверки наличия роли у пользователя"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = session.get('user_id')
            if not has_role(user_id, role_name):
                if not user_id:
                    flash('Для доступа к этой странице необходимо авторизоваться', 'error')
                    return redirect(url_for('index'))
                else:
                    flash('У вас нет прав для доступа к этой странице', 'error')
                    return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def require_any_role(*role_names):
    """Декоратор для проверки наличия хотя бы одной из ролей"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = session.get('user_id')
            if not has_any_role(user_id, role_names):
                if not user_id:
                    flash('Для доступа к этой странице необходимо авторизоваться', 'error')
                    return redirect(url_for('index'))
                else:
                    flash('У вас нет прав для доступа к этой странице', 'error')
                    return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator
def require_login(f):
    """Декоратор для проверки авторизации"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Для доступа к этой странице необходимо авторизоваться', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function
# Проверка подписи sign
def verify_sign(username, user_id, sign, encoded_name=None):
    # Формируем подпись: md5(password + username + user_id)
    # В PHP: $sign=md5($pass.$user_name.$user_user_id);
    # ВАЖНО: В PHP подпись вычисляется с оригинальными байтами ДО urlencode!
    # Поэтому нужно использовать unquote_to_bytes для получения оригинальных байтов
    
    variants = []
    
    # Вариант 1: ОРИГИНАЛЬНЫЕ БАЙТЫ из URL (правильный способ!)
    # В PHP подпись вычисляется с оригинальными байтами, а не с декодированной строкой
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
            except:
                pass
    
    # Вариант 2: декодированное имя через UTF-8
    expected_sign_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id)).encode('utf-8')
    ).hexdigest()
    variants.append(('decoded', expected_sign_decoded))
    
    # Вариант 3: закодированное имя (как пришло в URL)
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
    
    # Вариант 4: декодированное через CP1251 (Windows-1251)
    if encoded_name:
        try:
            name_cp1251 = unquote_plus(encoded_name, encoding='cp1251')
            expected_sign_cp1251 = hashlib.md5(
                (GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8')
            ).hexdigest()
            variants.append(('cp1251', expected_sign_cp1251))
        except:
            pass
        
        # Вариант 5: декодированное через latin1, затем байты
        try:
            name_latin1 = unquote_plus(encoded_name, encoding='latin1')
            name_latin1_bytes = name_latin1.encode('latin1')
            expected_sign_latin1_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_latin1_bytes + str(user_id).encode('utf-8')
            ).hexdigest()
            variants.append(('latin1_bytes', expected_sign_latin1_bytes))
        except:
            pass
    
    # Логирование для отладки
    log_error(f"verify_sign: username={username}, user_id={user_id}")
    log_error(f"verify_sign: encoded_name={encoded_name}")
    for variant_name, variant_sign in variants:
        match_status = "MATCH" if variant_sign == sign else "NO MATCH"
        log_error(f"verify_sign: variant {variant_name}={variant_sign}, {match_status}")
    
    # Проверяем все варианты
    for variant_name, variant_sign in variants:
        if variant_sign == sign:
            log_error(f"verify_sign: SUCCESS with variant {variant_name}!")
            return True
    
    log_error(f"verify_sign: ALL VARIANTS FAILED! Received sign={sign}")
    return False

# Проверка подписи sign2
def verify_sign2(level, synd, user_id, sign2):
    expected_sign2 = hashlib.md5(
        (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
    ).hexdigest()
    return expected_sign2 == sign2

# Проверка подписи sign3
def verify_sign3(username, user_id, has_passport, has_mobile, old_passport, sign3, encoded_name=None):
    # В PHP: $sign3=substr(md5($pass.$user_name.$user_id.$has_passport.$has_mobile.$old_passport),0,10);
    # ВАЖНО: Используем оригинальные байты, как и для sign!
    variants = []
    
    # Вариант 1: ОРИГИНАЛЬНЫЕ БАЙТЫ из URL (правильный способ!)
    if encoded_name:
        encoded_variations = [encoded_name]
        if '+' in encoded_name:
            encoded_variations.append(encoded_name.replace('+', '%20'))
        for encoded_variant in encoded_variations:
            try:
                name_bytes = unquote_to_bytes(encoded_variant)
                expected_sign3_bytes = hashlib.md5(
                    GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8') + 
                    str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8')
                ).hexdigest()[:10]
                suffix = '' if encoded_variant == encoded_name else '_space'
                variants.append((f'bytes{suffix}', expected_sign3_bytes))
            except:
                pass
    
    # Вариант 2: декодированное имя
    expected_sign3_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id) + str(has_passport) + str(has_mobile) + str(old_passport)).encode('utf-8')
    ).hexdigest()[:10]
    variants.append(('decoded', expected_sign3_decoded))
    
    # Проверяем все варианты
    for variant_name, variant_sign in variants:
        if variant_sign == sign3:
            log_error(f"verify_sign3: SUCCESS with variant {variant_name}!")
            return True
    
    log_error(f"verify_sign3: ALL VARIANTS FAILED! Received sign3={sign3}")
    return False
# Проверка подписи sign4 (дата)
def verify_sign4(sign3, sign4):
    """
    Проверяет подпись sign4 с учетом возможной разницы в часовых поясах.
    GWars может использовать другую дату из-за часового пояса, поэтому проверяем
    сегодняшнюю, вчерашнюю и завтрашнюю даты.
    """
    from datetime import datetime, timedelta
    
    # Получаем текущую дату и соседние даты (на случай разницы в часовых поясах)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    
    dates_to_check = [
        today.strftime("%Y-%m-%d"),
        yesterday.strftime("%Y-%m-%d"),
        tomorrow.strftime("%Y-%m-%d")
    ]
    
    # Проверяем все возможные варианты дат
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
def inject_common_flags():
    return {
        'is_production': app.config.get('ENV') == 'production',
        'app_config': app.config,
    }

@app.route('/')
def index():
    # Собираем данные для лендинга (доступно всем)
    conn = get_db_connection()
    
    # Статистика участников
    total_users = conn.execute('SELECT COUNT(*) as count FROM users').fetchone()['count']
    online_users = conn.execute('''
        SELECT COUNT(*) as count FROM users 
        WHERE datetime(last_login) > datetime('now', '-1 hour')
    ''').fetchone()['count']
    
    # Последние события (активные или последние 3)
    events_list = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.deleted_at IS NULL
        ORDER BY e.created_at DESC
    ''').fetchall()
    
    # Определяем текущий этап и ближайший будущий этап для каждого мероприятия
    events_with_stages_raw = []
    now = get_event_now()
    stage_info_map = {stage['type']: stage for stage in EVENT_STAGES}

    def parse_dt(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    for event in events_list:
        current_stage = get_current_event_stage(event['id'])
        display_stage_name = None
        next_stage = None
        if current_stage:
            display_stage_name = current_stage['info']['name']
            if current_stage['info']['type'] == 'registration_closed':
                lottery_stage = next((stage for stage in EVENT_STAGES if stage['type'] == 'lottery'), None)
                display_stage_name = lottery_stage['name'] if lottery_stage else 'Жеребьёвка'
        
        # Определяем следующий этап для таймера
        stages = get_event_stages(event['id'])
        stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
        for stage in stages:
            start_dt = parse_dt(stage['start_datetime'])
            if not start_dt or start_dt <= now:
                continue

            stage_info = stage_info_map.get(stage['stage_type'])
            stage_name = stage_info['name'] if stage_info else stage['stage_type']

            if (not next_stage) or start_dt < next_stage['start_dt']:
                next_stage = {
                    'name': stage_name,
                    'start_dt': start_dt,
                    'start_iso': start_dt.isoformat()
                }

        if current_stage and not next_stage:
            current_type = current_stage['info']['type']
            try:
                current_index = next(i for i, s in enumerate(EVENT_STAGES) if s['type'] == current_type)
            except StopIteration:
                current_index = None

            if current_index is not None:
                for idx in range(current_index + 1, len(EVENT_STAGES)):
                    next_info = EVENT_STAGES[idx]
                    next_data = stages_dict.get(next_info['type'])
                    candidate_raw = None
                    candidate_dt = None

                    if next_data and next_data.get('start_datetime'):
                        candidate_raw = next_data['start_datetime']
                    elif next_data and next_data.get('end_datetime'):
                        candidate_raw = next_data['end_datetime']
                    elif next_info['type'] == 'after_party' and current_stage['data'] and current_stage['data'].get('end_datetime'):
                        candidate_raw = current_stage['data']['end_datetime']

                    if candidate_raw:
                        candidate_dt = parse_event_datetime(str(candidate_raw))

                    if candidate_dt and candidate_dt > now:
                        next_stage = {
                            'name': next_info['name'],
                            'start_dt': candidate_dt,
                            'start_iso': candidate_dt.isoformat()
                        }
                        break

        events_with_stages_raw.append({
            'event': event,
            'current_stage': current_stage,
            'display_stage_name': display_stage_name,
            'next_stage': next_stage
        })

    events_with_stages = events_with_stages_raw

    for item in events_with_stages:
        event = item['event']
        item['registrations_count'] = get_event_registrations_count(event['id'])

    # Название проекта
    project_name = get_setting('project_name', 'Анонимные Деды Морозы')
    
    conn.close()
    
    return render_template('index.html', 
                         total_users=total_users,
                         online_users=online_users,
                         events_with_stages=events_with_stages,
                         project_name=project_name)
@app.route('/login/dev')
def login_dev():
    """Тестовый режим авторизации для локальной разработки"""
    # Проверяем, что мы на localhost
    is_local = request.host in ['127.0.0.1:5000', 'localhost:5000', '127.0.0.1', 'localhost']
    
    if not is_local:
        flash('Тестовый режим доступен только на localhost', 'error')
        return redirect(url_for('index'))
    
    # Используем тестовые данные для первого администратора (user_id 283494)
    user_id = ADMIN_USER_IDS[0]
    name = "_Колунт_"
    level = 50
    synd = 5594
    has_passport = 1
    has_mobile = 1
    old_passport = 0
    usersex = "0"
    
    # Генерируем правильные подписи для тестовых данных
    from urllib.parse import quote
    name_encoded = quote(name.encode('cp1251'), safe='')
    
    # Вычисляем подписи
    sign = hashlib.md5((GWARS_PASSWORD.encode('utf-8') + name.encode('cp1251') + str(user_id).encode('utf-8'))).hexdigest()
    sign2 = hashlib.md5((GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')).hexdigest()
    sign3 = hashlib.md5((GWARS_PASSWORD.encode('utf-8') + name.encode('cp1251') + str(user_id).encode('utf-8') + str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8'))).hexdigest()[:10]
    
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    sign4 = hashlib.md5((today + sign3 + GWARS_PASSWORD).encode('utf-8')).hexdigest()[:10]
    
    # Сохраняем пользователя в БД
    conn = get_db_connection()
    try:
        # Проверяем, существует ли пользователь и получаем все его данные
        existing_user = conn.execute('''
            SELECT username, level, synd, has_passport, has_mobile, old_passport, usersex,
                   avatar_seed, avatar_style, bio, contact_info, email, phone, telegram, whatsapp, viber 
            FROM users WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        # Преобразуем данные в правильные типы
        level_int = int(level) if level else 0
        synd_int = int(synd) if synd else 0
        has_passport_int = 1 if has_passport == 1 else 0
        has_mobile_int = 1 if has_mobile == 1 else 0
        old_passport_int = 1 if old_passport == 1 else 0
        
        if not existing_user:
            # Новый пользователь - создаем запись
            avatar_seed = generate_unique_avatar_seed(user_id)
            avatar_style = 'avataaars'  # Стиль по умолчанию
            conn.execute('''
                INSERT INTO users 
                (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, 
                 avatar_seed, avatar_style, last_login)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, name, level_int, synd_int, has_passport_int, has_mobile_int, 
                  old_passport_int, usersex, avatar_seed, avatar_style, datetime.now()))
            log_debug(f"New dev user created: user_id={user_id}, username={name}")
        else:
            # Существующий пользователь - проверяем, изменились ли данные
            needs_update = False
            update_fields = []
            update_values = []
            
            # Проверяем каждое поле
            if existing_user['username'] != name:
                needs_update = True
                update_fields.append('username = ?')
                update_values.append(name)
                log_debug(f"Dev username changed for user {user_id}: '{existing_user['username']}' -> '{name}'")
            
            if existing_user['level'] != level_int:
                needs_update = True
                update_fields.append('level = ?')
                update_values.append(level_int)
                log_debug(f"Dev level changed for user {user_id}: {existing_user['level']} -> {level_int}")
            
            if existing_user['synd'] != synd_int:
                needs_update = True
                update_fields.append('synd = ?')
                update_values.append(synd_int)
                log_debug(f"Dev synd changed for user {user_id}: {existing_user['synd']} -> {synd_int}")
            
            if existing_user['has_passport'] != has_passport_int:
                needs_update = True
                update_fields.append('has_passport = ?')
                update_values.append(has_passport_int)
            
            if existing_user['has_mobile'] != has_mobile_int:
                needs_update = True
                update_fields.append('has_mobile = ?')
                update_values.append(has_mobile_int)
            
            if existing_user['old_passport'] != old_passport_int:
                needs_update = True
                update_fields.append('old_passport = ?')
                update_values.append(old_passport_int)
            
            if existing_user['usersex'] != usersex:
                needs_update = True
                update_fields.append('usersex = ?')
                update_values.append(usersex)
            
            # Всегда обновляем last_login
            update_fields.append('last_login = ?')
            update_values.append(datetime.now())
            
            # Если есть изменения, обновляем только измененные поля
            if needs_update:
                update_values.append(user_id)
                update_query = f'''
                    UPDATE users 
                    SET {', '.join(update_fields)}
                    WHERE user_id = ?
                '''
                conn.execute(update_query, update_values)
                log_debug(f"Dev user data updated: user_id={user_id}, fields: {', '.join([f.split('=')[0].strip() for f in update_fields])}")
            else:
                # Если данных не изменилось, обновляем только last_login
                conn.execute('''
                    UPDATE users 
                    SET last_login = ?
                    WHERE user_id = ?
                ''', (datetime.now(), user_id))
                log_debug(f"Dev user data unchanged, only last_login updated: user_id={user_id}")
            
            # Если у пользователя нет avatar_seed, генерируем его
            if not existing_user['avatar_seed']:
                avatar_seed = generate_unique_avatar_seed(user_id)
                avatar_style = existing_user['avatar_style'] or 'avataaars'
                conn.execute('''
                    UPDATE users 
                    SET avatar_seed = ?, avatar_style = ?
                    WHERE user_id = ?
                ''', (avatar_seed, avatar_style, user_id))
                log_debug(f"Generated avatar_seed for dev user {user_id}")
        
        conn.commit()
        log_debug(f"Dev user saved successfully: user_id={user_id}, username={name}")
    except Exception as e:
        log_error(f"Error saving dev user: {e}")
        flash(f'Ошибка сохранения пользователя: {str(e)}', 'error')
        return redirect(url_for('index'))
    finally:
        conn.close()
    
    # Автоматически назначаем роль админа для администраторов по умолчанию
    if user_id in ADMIN_USER_IDS:
        if not has_role(user_id, 'admin'):
            assign_role(user_id, 'admin', assigned_by=user_id)
            log_debug(f"Admin role automatically assigned to user_id {user_id}")
    
    # Если у пользователя нет ролей, назначаем роль 'user' по умолчанию
    if not get_user_roles(user_id):
        assign_role(user_id, 'user', assigned_by=user_id)
        log_debug(f"Default 'user' role assigned to user_id {user_id}")
    
    # Сохраняем в сессию
    session['user_id'] = user_id
    session['username'] = name
    session['level'] = level
    session['synd'] = synd
    session['roles'] = get_user_role_names(user_id)
    # Очищаем флаг попытки авторизации через GWars (если был установлен)
    session.pop('gwars_auth_attempt', None)
    
    log_activity(
        'login',
        details='Тестовый вход через login_dev',
        metadata={'source': 'dev', 'user_id': user_id, 'username': name}
    )
    
    flash('Тестовая авторизация выполнена успешно!', 'success')
    return redirect(url_for('dashboard'))
@app.route('/login')
def login():
    try:
        # Получаем параметры от GWars
        sign = request.args.get('sign', '')
        user_id = request.args.get('user_id', '')
        
        # ВАЖНО: Flask автоматически декодирует URL параметры, но нам нужен оригинальный закодированный вариант
        # Получаем оригинальное значение из query string напрямую
        try:
            query_string_raw = request.query_string
            query_string = query_string_raw.decode('utf-8', errors='replace')
        except:
            query_string = request.query_string.decode('utf-8') if request.query_string else ''
        
        name_encoded = None
        # Пробуем извлечь name из query string
        if query_string:
            for param in query_string.split('&'):
                if param.startswith('name='):
                    name_encoded = param.split('=', 1)[1]  # Берем все после первого =
                    break
        
        # Если не получилось получить из query_string, пробуем через request.args (но это уже декодированное)
        if not name_encoded or name_encoded == '':
            name_encoded = request.args.get('name', '')
            # Если получили через args, значит оно уже декодировано, нужно закодировать обратно для проверки
            if name_encoded:
                name_encoded_for_comparison = quote(name_encoded, safe='')
            else:
                name_encoded_for_comparison = ''
        else:
            name_encoded_for_comparison = name_encoded
        
        # Пробуем декодировать разными способами
        # ВАЖНО: GWars использует CP1251 (Windows-1251) для кодирования русских символов!
        name = name_encoded if name_encoded else ''
        name_latin1 = None
        name_cp1251 = None
        if name_encoded:
            try:
                # Сначала пробуем CP1251 (Windows-1251) - это основная кодировка для русских символов
                name_cp1251 = unquote_plus(name_encoded, encoding='cp1251')
                name = name_cp1251  # Используем CP1251 как основной вариант
            except:
                try:
                    name = unquote_plus(name_encoded, encoding='utf-8')
                except:
                    try:
                        name = unquote_plus(name_encoded, encoding='latin1')
                        name_latin1 = name
                    except:
                        name = name_encoded
                        name_latin1 = name_encoded
            
            # Если CP1251 декодирование не сработало, пробуем еще раз
            if not name_cp1251:
                try:
                    name_cp1251 = unquote_plus(name_encoded, encoding='cp1251')
                except:
                    name_cp1251 = None
        
        level = request.args.get('level', '0')
        synd = request.args.get('synd', '0')
        sign2 = request.args.get('sign2', '')
        has_passport = request.args.get('has_passport', '0')
        has_mobile = request.args.get('has_mobile', '0')
        old_passport = request.args.get('old_passport', '0')
        sign3 = request.args.get('sign3', '')
        usersex = request.args.get('usersex', '')
        sign4 = request.args.get('sign4', '')
        
        # Если name пустое, пробуем получить из request.args напрямую
        if not name or name == '':
            name = request.args.get('name', '')
            if name:
                name_encoded = name  # Если получили через args, значит оно уже декодировано
        
        # Если нет параметров, проверяем, вернулся ли пользователь с GWars без авторизации
        if not sign or not user_id:
            # Проверяем, есть ли в сессии флаг о попытке авторизации через GWars
            gwars_auth_attempt = session.get('gwars_auth_attempt', False)
            
            # Если пользователь уже пытался авторизоваться через GWars (флаг в сессии),
            # но параметров авторизации нет, значит он не авторизован в GWars
            if gwars_auth_attempt:
                # Очищаем флаг
                session.pop('gwars_auth_attempt', None)
                # Показываем страницу с сообщением о необходимости авторизации
                return redirect(url_for('gwars_required'))
            
            # Если параметров нет и пользователь еще не пытался авторизоваться,
            # устанавливаем флаг и редиректим на GWars для авторизации
            session['gwars_auth_attempt'] = True
            
            # ВАЖНО: GWars проверяет домен callback URL
            # Для локальной разработки используем production URL, чтобы GWars принял запрос
            # После авторизации пользователь будет редиректиться на production, 
            # где можно будет протестировать функционал
            
            # Определяем, работаем ли мы локально
            is_local = request.host in ['127.0.0.1:5000', 'localhost:5000', '127.0.0.1', 'localhost']
            
            if is_local:
                # При локальной разработке используем production URL для callback
                # Это необходимо, так как GWars не принимает localhost
                callback_url = f"https://{GWARS_HOST}/login"
                log_debug(f"Local development detected. Using production callback URL: {callback_url}")
                log_debug("After GWars authorization, you'll be redirected to production server.")
                log_debug("You can then manually navigate to localhost:5000 for local testing.")
            else:
                # На production используем текущий домен
                if 'pythonanywhere.com' in request.host:
                    callback_url = f"https://{request.host}/login"
                else:
                    callback_url = f"{request.scheme}://{request.host}/login"
            
            # Редиректим на GWars для авторизации
            # Если пользователь авторизован в GWars, он получит параметры (sign, user_id и т.д.)
            # и будет авторизован в нашей системе (флаг gwars_auth_attempt будет очищен при успешной авторизации)
            # Если не авторизован, вернется без параметров, и мы покажем страницу /gwars-required
            gwars_login_url = f"https://www.gwars.io/cross-server-login.php?site_id={GWARS_SITE_ID}&url={quote(callback_url)}"
            return redirect(gwars_login_url)
        
        # Логируем все полученные параметры для отладки
        log_error("=== LOGIN DEBUG ===")
        log_error(f"Received parameters:")
        log_error(f"  sign={sign}")
        log_error(f"  name (from args)={request.args.get('name', '')}")
        log_error(f"  name (encoded/raw from query_string)={name_encoded}")
        log_error(f"  name (decoded)={name}")
        log_error(f"  name (repr)={repr(name)}")
        log_error(f"  name_encoded (repr)={repr(name_encoded)}")
        log_error(f"  user_id={user_id}")
        log_error(f"  level={level}")
        log_error(f"  synd={synd}")
        log_error(f"  sign2={sign2}")
        log_error(f"Full URL: {request.url}")
        log_error(f"Query string (raw bytes): {request.query_string}")
        log_error(f"Query string (decoded): {query_string}")
        log_error(f"All args: {dict(request.args)}")
        
        # Проверяем подписи (пробуем оба варианта - с декодированным и закодированным именем)
        if not verify_sign(name, user_id, sign, name_encoded):
            # Вместо редиректа, сразу показываем страницу отладки
            # Это позволит увидеть информацию даже если логи не работают
            flash('Ошибка проверки подписи sign. Смотрите информацию ниже.', 'error')
            
            # Вычисляем все варианты для отображения
            # ВАЖНО: Правильный способ - использовать оригинальные байты из URL!
            variant_bytes = None
            if name_encoded:
                try:
                    name_bytes = unquote_to_bytes(name_encoded)
                    variant_bytes = hashlib.md5(
                        GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8')
                    ).hexdigest()
                except:
                    pass
            
            variant1 = hashlib.md5((GWARS_PASSWORD + name + str(user_id)).encode('utf-8')).hexdigest()
            variant2 = hashlib.md5((GWARS_PASSWORD + name_encoded + str(user_id)).encode('utf-8')).hexdigest()
            variant3 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name).encode('utf-8')).hexdigest()
            variant4 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name_encoded).encode('utf-8')).hexdigest()
            
            # Пробуем CP1251
            try:
                if not name_cp1251:
                    name_cp1251 = unquote(name_encoded, encoding='cp1251') if name_encoded else None
                if name_cp1251:
                    variant5 = hashlib.md5((GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8')).hexdigest()
                else:
                    variant5 = None
            except:
                name_cp1251 = None
                variant5 = None
            
            # Пробуем latin1 с байтами (правильный способ!)
            variant_latin1_bytes = None
            try:
                if not name_latin1:
                    name_latin1 = unquote(name_encoded, encoding='latin1') if name_encoded else None
                if name_latin1:
                    name_latin1_bytes = name_latin1.encode('latin1')
                    variant_latin1_bytes = hashlib.md5(
                        GWARS_PASSWORD.encode('utf-8') + name_latin1_bytes + str(user_id).encode('utf-8')
                    ).hexdigest()
            except:
                name_latin1 = None
                variant_latin1_bytes = None
            
            # Пробуем с именем как оно пришло через request.args (уже декодированное)
            name_from_args = request.args.get('name', '')
            variant7 = None
            if name_from_args and name_from_args != name:
                variant7 = hashlib.md5((GWARS_PASSWORD + name_from_args + str(user_id)).encode('utf-8')).hexdigest()
            
            # Пробуем с пустым именем (если имя пустое)
            variant8 = None
            variant9 = None
            if not name or name == '':
                variant8 = hashlib.md5((GWARS_PASSWORD + '' + str(user_id)).encode('utf-8')).hexdigest()
                variant9 = hashlib.md5((GWARS_PASSWORD + str(user_id) + '').encode('utf-8')).hexdigest()
            
            expected_sign2 = hashlib.md5(
                (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
            ).hexdigest()
            
            debug_info = {
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
                'received_sign': sign,
                'sign_match_bytes': variant_bytes == sign if variant_bytes else False,
                'sign_match_v1': variant1 == sign,
                'sign_match_v2': variant2 == sign,
                'sign_match_v3': variant3 == sign,
                'sign_match_v4': variant4 == sign,
                'sign_match_v5': variant5 == sign if variant5 else False,
                'sign_match_latin1_bytes': variant_latin1_bytes == sign if variant_latin1_bytes else False,
                'expected_sign2': expected_sign2,
                'received_sign2': sign2,
                'sign2_match': expected_sign2 == sign2,
            }
            
            return render_template('debug.html', debug_info=debug_info)
        
        if not verify_sign2(level, synd, user_id, sign2):
            flash('Ошибка проверки подписи sign2', 'error')
            return redirect(url_for('index'))
        
        if not verify_sign3(name, user_id, has_passport, has_mobile, old_passport, sign3, name_encoded):
            # Показываем страницу отладки для sign3
            flash('Ошибка проверки подписи sign3. Смотрите информацию ниже.', 'error')
            
            # Вычисляем варианты sign3 для отладки
            sign3_variant_bytes = None
            if name_encoded:
                try:
                    name_bytes = unquote_to_bytes(name_encoded)
                    sign3_variant_bytes = hashlib.md5(
                        GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8') + 
                        str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8')
                    ).hexdigest()[:10]
                except:
                    pass
            
            sign3_variant_decoded = hashlib.md5(
                (GWARS_PASSWORD + name + str(user_id) + str(has_passport) + str(has_mobile) + str(old_passport)).encode('utf-8')
            ).hexdigest()[:10]
            
            # Вычисляем sign4 варианты
            today = datetime.now().strftime("%Y-%m-%d")
            sign4_variant1 = hashlib.md5((today + sign3 + GWARS_PASSWORD).encode('utf-8')).hexdigest()[:10]
            
            debug_info = {
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
            
            return render_template('debug_sign3.html', debug_info=debug_info)
        
        if not verify_sign4(sign3, sign4):
            # Логируем детали для отладки
            today = datetime.now().strftime("%Y-%m-%d")
            log_error(f"sign4 verification failed: sign3={sign3}, sign4={sign4}, today={today}")
            log_error(f"sign4 verification failed: user_id={user_id}, name={name}")
            
            # Показываем более информативное сообщение
            flash('Ошибка проверки подписи sign4. Возможно, разница в часовых поясах. Попробуйте войти еще раз.', 'error')
            return redirect(url_for('index'))
        
        # Сохраняем пользователя в БД
        conn = get_db_connection()
        try:
            # Проверяем, существует ли пользователь и получаем все его данные
            existing_user = conn.execute('''
                SELECT username, level, synd, has_passport, has_mobile, old_passport, usersex,
                       avatar_seed, avatar_style, bio, contact_info, email, phone, telegram, whatsapp, viber 
                FROM users WHERE user_id = ?
            ''', (user_id,)).fetchone()
            
            # Преобразуем данные из GWars в правильные типы
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = 1 if has_passport == '1' else 0
            has_mobile_int = 1 if has_mobile == '1' else 0
            old_passport_int = 1 if old_passport == '1' else 0
            
            if not existing_user:
                # Новый пользователь - создаем запись
                avatar_seed = generate_unique_avatar_seed(user_id)
                avatar_style = 'avataaars'  # Стиль по умолчанию
                # Явно устанавливаем все поля контактов в NULL для нового пользователя
                conn.execute('''
                    INSERT INTO users 
                    (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, 
                     avatar_seed, avatar_style, last_login,
                     email, phone, telegram, whatsapp, viber,
                     last_name, first_name, middle_name,
                     postal_code, country, city, street, house, building, apartment,
                     bio, contact_info)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL)
                ''', (user_id, name, level_int, synd_int, has_passport_int, has_mobile_int, 
                      old_passport_int, usersex, avatar_seed, avatar_style, datetime.now()))
                log_debug(f"New user created: user_id={user_id}, username={name}")
            else:
                # Существующий пользователь - проверяем, изменились ли данные из GWars
                needs_update = False
                update_fields = []
                update_values = []
                
                # Проверяем каждое поле из GWars
                if existing_user['username'] != name:
                    needs_update = True
                    update_fields.append('username = ?')
                    update_values.append(name)
                    log_debug(f"Username changed for user {user_id}: '{existing_user['username']}' -> '{name}'")
                
                if existing_user['level'] != level_int:
                    needs_update = True
                    update_fields.append('level = ?')
                    update_values.append(level_int)
                    log_debug(f"Level changed for user {user_id}: {existing_user['level']} -> {level_int}")
                
                if existing_user['synd'] != synd_int:
                    needs_update = True
                    update_fields.append('synd = ?')
                    update_values.append(synd_int)
                    log_debug(f"Synd changed for user {user_id}: {existing_user['synd']} -> {synd_int}")
                
                if existing_user['has_passport'] != has_passport_int:
                    needs_update = True
                    update_fields.append('has_passport = ?')
                    update_values.append(has_passport_int)
                
                if existing_user['has_mobile'] != has_mobile_int:
                    needs_update = True
                    update_fields.append('has_mobile = ?')
                    update_values.append(has_mobile_int)
                
                if existing_user['old_passport'] != old_passport_int:
                    needs_update = True
                    update_fields.append('old_passport = ?')
                    update_values.append(old_passport_int)
                
                if existing_user['usersex'] != usersex:
                    needs_update = True
                    update_fields.append('usersex = ?')
                    update_values.append(usersex)
                
                # Всегда обновляем last_login
                update_fields.append('last_login = ?')
                update_values.append(datetime.now())
                
                # Если есть изменения, обновляем только измененные поля
                if needs_update:
                    update_values.append(user_id)
                    update_query = f'''
                        UPDATE users 
                        SET {', '.join(update_fields)}
                        WHERE user_id = ?
                    '''
                    conn.execute(update_query, update_values)
                    log_debug(f"User data updated: user_id={user_id}, fields: {', '.join([f.split('=')[0].strip() for f in update_fields])}")
                else:
                    # Если данных не изменилось, обновляем только last_login
                    conn.execute('''
                        UPDATE users 
                        SET last_login = ?
                        WHERE user_id = ?
                    ''', (datetime.now(), user_id))
                    log_debug(f"User data unchanged, only last_login updated: user_id={user_id}")
                
                # Если у пользователя нет avatar_seed, генерируем его
                if not existing_user['avatar_seed']:
                    avatar_seed = generate_unique_avatar_seed(user_id)
                    avatar_style = existing_user['avatar_style'] or 'avataaars'
                    conn.execute('''
                        UPDATE users 
                        SET avatar_seed = ?, avatar_style = ?
                        WHERE user_id = ?
                    ''', (avatar_seed, avatar_style, user_id))
                    log_debug(f"Generated avatar_seed for user {user_id}")
            
            conn.commit()
            log_debug(f"User saved successfully: user_id={user_id}, username={name}")
        except Exception as e:
            log_error(f"Error saving user: {e}")
            # Если ошибка из-за отсутствия таблицы, пробуем инициализировать БД заново
            if "no such table" in str(e).lower():
                log_error("Table not found, reinitializing database...")
                init_db()
                # Пробуем еще раз
                try:
                    # Проверяем существующего пользователя еще раз
                    existing_user = conn.execute('SELECT avatar_seed, avatar_style, bio, contact_info FROM users WHERE user_id = ?', (user_id,)).fetchone()
                    
                    # Генерируем seed для нового пользователя или используем существующий
                    if not existing_user or not existing_user['avatar_seed']:
                        avatar_seed = generate_unique_avatar_seed(user_id)
                        avatar_style = 'avataaars'
                        bio = None
                        contact_info = None
                    else:
                        avatar_seed = existing_user['avatar_seed']
                        avatar_style = existing_user['avatar_style']
                        bio = existing_user['bio']
                        contact_info = existing_user['contact_info']
                    conn.execute('''
                        INSERT OR REPLACE INTO users 
                        (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, last_login)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (user_id, name, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, datetime.now()))
                    conn.commit()
                    log_debug(f"User saved successfully after reinitialization: user_id={user_id}")
                except Exception as e2:
                    log_error(f"Error saving user after reinitialization: {e2}")
                    flash(f'Ошибка сохранения пользователя: {str(e2)}', 'error')
                    conn.close()
                    return redirect(url_for('index'))
            else:
                flash(f'Ошибка сохранения пользователя: {str(e)}', 'error')
                conn.close()
                return redirect(url_for('index'))
        finally:
            conn.close()
        
        # Автоматически назначаем роль админа для администраторов по умолчанию
        if int(user_id) in ADMIN_USER_IDS:
            if not has_role(user_id, 'admin'):
                assign_role(user_id, 'admin', assigned_by=user_id)
                log_debug(f"Admin role automatically assigned to user_id {user_id}")
        
        # Если у пользователя нет ролей, назначаем роль 'user' по умолчанию
        if not get_user_roles(user_id):
            assign_role(user_id, 'user', assigned_by=user_id)
            log_debug(f"Default 'user' role assigned to user_id {user_id}")
        
        # Сохраняем в сессию
        session['user_id'] = user_id
        session['username'] = name
        session['level'] = level
        session['synd'] = synd
        session['roles'] = get_user_role_names(user_id)  # Сохраняем роли в сессию
        # Очищаем флаг попытки авторизации через GWars (если был установлен)
        session.pop('gwars_auth_attempt', None)
        
        log_activity(
            'login',
            details='Вход через GWars',
            metadata={'source': 'gwars', 'user_id': user_id, 'username': name}
        )
        
        return redirect(url_for('dashboard'))
    except Exception as e:
        log_error(f"Error in login route: {e}")
        log_error(f"Traceback: {traceback.format_exc()}")
        flash(f'Ошибка при входе: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/telegram/verify/generate', methods=['POST'])
@require_login
def telegram_verify_generate():
    """Генерирует код верификации для пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'message': 'Необходима авторизация'}), 401
    
    code = generate_telegram_verification_code(user_id)
    if code:
        # Получаем имя бота для ссылки
        token = get_setting('telegram_bot_token', '')
        bot_username = None
        if token and requests:
            try:
                api_url = f'https://api.telegram.org/bot{token}/getMe'
                response = requests.get(api_url, timeout=5)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('ok'):
                        bot_username = result.get('result', {}).get('username')
            except:
                pass
        
        return jsonify({
            'success': True,
            'code': code,
            'user_id': user_id,
            'bot_username': bot_username,
            'message': f'Код верификации: {code}\n\nОткройте бота в Telegram и отправьте ему этот код.'
        })
    else:
        return jsonify({'success': False, 'message': 'Ошибка при генерации кода'}), 500

@app.route('/telegram/verify/status', methods=['GET'])
@require_login
def telegram_verify_status():
    """Проверяет статус верификации Telegram"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'verified': False}), 401
    
    conn = get_db_connection()
    telegram_user = conn.execute('''
        SELECT verified, telegram_chat_id, telegram_username, verified_at
        FROM telegram_users
        WHERE user_id = ?
    ''', (user_id,)).fetchone()
    conn.close()
    
    if telegram_user:
        return jsonify({
            'success': True,
            'verified': bool(telegram_user['verified']),
            'telegram_chat_id': telegram_user['telegram_chat_id'],
            'telegram_username': telegram_user['telegram_username'],
            'verified_at': telegram_user['verified_at']
        })
    else:
        return jsonify({'success': True, 'verified': False})

@app.route('/telegram/verify/unlink', methods=['POST'])
@require_login
def telegram_verify_unlink():
    """Отвязывает Telegram аккаунт от пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'message': 'Необходима авторизация'}), 401
    
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM telegram_users
            WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        return jsonify({'success': True, 'message': 'Telegram аккаунт успешно отвязан'})
    except Exception as e:
        log_error(f"Error unlinking Telegram: {e}")
        return jsonify({'success': False, 'message': f'Ошибка при отвязке: {str(e)}'}), 500
    finally:
        conn.close()

@app.route('/dashboard')
@require_login
def dashboard():
    # Получаем данные пользователя из БД
    conn = get_db_connection()
    try:
        user = conn.execute(
            'SELECT * FROM users WHERE user_id = ?', (session['user_id'],)
        ).fetchone()
        
        # Получаем роли пользователя
        user_roles = get_user_roles(session['user_id'])
        
        # Получаем статус верификации Telegram
        telegram_verified = False
        telegram_info = None
        try:
            telegram_user = conn.execute('''
                SELECT verified, telegram_chat_id, telegram_username, verified_at
                FROM telegram_users
                WHERE user_id = ?
            ''', (session['user_id'],)).fetchone()
            
            if telegram_user:
                telegram_verified = bool(telegram_user['verified'])
                telegram_info = dict(telegram_user)
        except sqlite3.OperationalError as e:
            # Таблица может не существовать, если БД не была инициализирована
            log_error(f"Error fetching telegram user: {e}")
    finally:
        conn.close()
    
    return render_template('dashboard.html', 
                         user=user, 
                         user_roles=user_roles,
                         telegram_verified=telegram_verified,
                         telegram_info=telegram_info)


@app.route('/api/avatar/generate-options', methods=['POST'])
@require_login
def api_generate_avatar_options():
    """API endpoint для генерации вариантов аватаров по стилю"""
    data = request.get_json()
    style = data.get('style', 'avataaars')
    count = data.get('count', 20)  # Количество вариантов для генерации
    
    if not style:
        return jsonify({'error': 'Style is required'}), 400
    
    conn = get_db_connection()
    try:
        # Получаем все использованные seeds
        used_seeds = set(row[0] for row in conn.execute(
            'SELECT avatar_seed FROM users WHERE avatar_seed IS NOT NULL'
        ).fetchall())
        conn.close()
    except Exception as e:
        log_error(f"Error fetching used seeds: {e}")
        conn.close()
        used_seeds = set()
    
    # Генерируем варианты аватаров
    options = []
    attempts = 0
    max_attempts = count * 10  # Максимальное количество попыток
    
    while len(options) < count and attempts < max_attempts:
        # Генерируем случайный seed
        random_part = secrets.token_hex(8)
        seed = f"option_{random_part}"
        
        # Проверяем уникальность
        if seed not in used_seeds:
            options.append({
                'seed': seed,
                'url': get_avatar_url(seed, style, 128),
                'unique': True
            })
            used_seeds.add(seed)  # Добавляем в список использованных для этой сессии
        
        attempts += 1
    
    return jsonify({
        'style': style,
        'options': options,
        'count': len(options)
    })
@app.route('/profile/edit', methods=['GET', 'POST'])
@require_login
def edit_profile():
    """Редактирование профиля пользователя"""
    conn = get_db_connection()
    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (session['user_id'],)
    ).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('dashboard'))
    
    if request.method == 'POST':
        # Получаем редактируемые поля (не из GWars)
        bio = request.form.get('bio', '').strip()
        contact_info = request.form.get('contact_info', '').strip()
        avatar_seed = request.form.get('avatar_seed', '').strip()
        avatar_style = request.form.get('avatar_style', 'avataaars').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        telegram = request.form.get('telegram', '').strip()
        whatsapp = request.form.get('whatsapp', '').strip()
        viber = request.form.get('viber', '').strip()
        
        # Личные данные
        last_name = request.form.get('last_name', '').strip()
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        
        # Адрес
        postal_code = request.form.get('postal_code', '').strip()
        country = request.form.get('country', '').strip()
        city = request.form.get('city', '').strip()
        street = request.form.get('street', '').strip()
        house = request.form.get('house', '').strip()
        building = request.form.get('building', '').strip()
        apartment = request.form.get('apartment', '').strip()
        
        try:
            # Если передан новый avatar_seed и avatar_style, проверяем уникальность
            if avatar_seed and avatar_style:
                # Проверяем, что выбранный seed уникален (не используется другими пользователями)
                used_seeds = get_used_avatar_seeds(exclude_user_id=session['user_id'])
                if avatar_seed in used_seeds:
                    flash('Выбранный аватар уже используется другим пользователем. Пожалуйста, выберите другой.', 'error')
                    conn.close()
                    return render_template('edit_profile.html', user=user)
            
            # Обновляем профиль
            if avatar_seed and avatar_style:
                # Обновляем с новым аватаром и всеми полями
                conn.execute('''
                    UPDATE users 
                    SET bio = ?, contact_info = ?, avatar_style = ?, avatar_seed = ?, 
                        email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?,
                        last_name = ?, first_name = ?, middle_name = ?,
                        postal_code = ?, country = ?, city = ?, street = ?, house = ?, building = ?, apartment = ?
                    WHERE user_id = ?
                ''', (bio, contact_info, avatar_style, avatar_seed, email, phone, telegram, whatsapp, viber,
                      last_name, first_name, middle_name,
                      postal_code, country, city, street, house, building, apartment, session['user_id']))
            else:
                # Обновляем без изменения аватара, но со всеми остальными полями
                conn.execute('''
                    UPDATE users 
                    SET bio = ?, contact_info = ?, 
                        email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?,
                        last_name = ?, first_name = ?, middle_name = ?,
                        postal_code = ?, country = ?, city = ?, street = ?, house = ?, building = ?, apartment = ?
                    WHERE user_id = ?
                ''', (bio, contact_info, email, phone, telegram, whatsapp, viber,
                      last_name, first_name, middle_name,
                      postal_code, country, city, street, house, building, apartment, session['user_id']))
            
            conn.commit()
            flash('Профиль успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('dashboard'))
        except Exception as e:
            log_error(f"Error updating profile: {e}")
            flash(f'Ошибка обновления профиля: {str(e)}', 'error')
            conn.close()
    
    conn.close()
    return render_template('edit_profile.html', user=user)

@app.route('/profile/clear', methods=['POST'])
@require_login
@require_role('admin')
def clear_profile():
    """Очистка всех редактируемых полей профиля (только для администратора)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходимо авторизоваться'}), 401
    
    conn = get_db_connection()
    try:
        # Очищаем все редактируемые поля, но сохраняем системные (user_id, username, level, synd и т.д.)
        # Также сохраняем avatar_seed и avatar_style, так как они могут быть системными
        conn.execute('''
            UPDATE users 
            SET bio = NULL, contact_info = NULL,
                email = NULL, phone = NULL, telegram = NULL, whatsapp = NULL, viber = NULL,
                last_name = NULL, first_name = NULL, middle_name = NULL,
                postal_code = NULL, country = NULL, city = NULL, street = NULL, 
                house = NULL, building = NULL, apartment = NULL
            WHERE user_id = ?
        ''', (user_id,))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Все редактируемые поля профиля очищены'})
    except Exception as e:
        log_error(f"Error clearing profile: {e}")
        conn.close()
        return jsonify({'success': False, 'error': f'Ошибка очистки профиля: {str(e)}'}), 500

@app.route('/api/avatar/candidates', methods=['GET'])
@require_login
def get_avatar_candidates():
    """API endpoint для получения уникальных кандидатов аватаров выбранного стиля"""
    style = request.args.get('style', 'avataaars')
    count = int(request.args.get('count', 20))
    
    valid_styles = [
        'adventurer', 'adventurer-neutral', 'avataaars', 'avataaars-neutral',
        'big-ears', 'big-ears-neutral', 'big-smile', 'bottts', 'bottts-neutral',
        'croodles', 'croodles-neutral', 'fun-emoji', 'icons', 'identicon', 'initials',
        'lorelei', 'lorelei-neutral', 'micah', 'miniavs', 'open-peeps', 'personas',
        'pixel-art', 'pixel-art-neutral', 'rings', 'shapes', 'thumbs'
    ]
    
    if style not in valid_styles:
        return jsonify({'error': 'Invalid style'}), 400
    
    candidates = generate_unique_avatar_candidates(style, count, exclude_user_id=session['user_id'])
    
    return jsonify({
        'candidates': [
            {
                'seed': seed,
                'url': get_avatar_url(seed, style, size=128)
            }
            for seed in candidates
        ]
    })

@app.route('/profile/<int:user_id>')
def view_profile(user_id):
    """Просмотр профиля другого пользователя (доступно всем)"""
    conn = get_db_connection()
    
    # Получаем данные пользователя
    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (user_id,)
    ).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('participants'))
    
    # Получаем роли и звания пользователя
    user_roles = get_user_roles(user_id)
    user_titles = get_user_titles(user_id)
    user_awards = get_user_awards(user_id)
    
    # Получаем информацию о заблокировавшем пользователе
    blocker_info = None
    user_keys = user.keys()
    if 'is_blocked' in user_keys and user['is_blocked'] and 'blocked_by' in user_keys and user['blocked_by']:
        blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
        if blocker:
            blocker_info = dict(blocker)
    
    conn.close()
    
    # Проверяем, является ли это профилем текущего пользователя (если авторизован)
    session_user_id = session.get('user_id')
    try:
        session_user_id_int = int(session_user_id) if session_user_id is not None else None
    except (TypeError, ValueError):
        session_user_id_int = None
    is_own_profile = session_user_id_int == user_id
    is_admin = 'admin' in session.get('roles', []) if 'roles' in session else False
    impersonation_active = bool(session.get('impersonation_original'))
    can_impersonate = is_admin and not is_own_profile and not impersonation_active

    user_keys = user.keys()
    user_bio = user['bio'] if 'bio' in user_keys else None
    user_contact_info = user['contact_info'] if 'contact_info' in user_keys else None

    show_about = bool(user_bio or user_contact_info) and (is_admin or is_own_profile)
    bio_to_display = user_bio if show_about else None
    contact_info_to_display = user_contact_info if show_about and is_admin else None
    
    return render_template(
        'view_profile.html',
        user=dict(user),
        user_roles=user_roles,
        user_titles=user_titles,
        user_awards=user_awards,
        is_own_profile=is_own_profile,
        is_admin=is_admin,
        can_impersonate=can_impersonate,
        impersonation_active=impersonation_active,
        show_about=show_about,
        bio_to_display=bio_to_display,
        contact_info_to_display=contact_info_to_display,
        blocker_info=blocker_info
    )
@app.route('/participants')
def participants():
    """Страница со списком участников"""
    try:
        # Параметры пагинации и поиска
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        search_query = request.args.get('search', '').strip()
        
        # Логирование для отладки поиска
        if search_query:
            log_debug(f"Participants search: query='{search_query}', encoded={search_query.encode('utf-8')}")
        
        # Ограничиваем per_page разумными значениями
        per_page = min(max(per_page, 10), 100)
        
        conn = get_db_connection()
        
        # Формируем условия поиска
        # SQLite LOWER() не работает с кириллицей, поэтому используем другой подход:
        # Загружаем всех пользователей и фильтруем в Python
        search_params = []
        if search_query:
            search_lower = search_query.lower()
            # Используем оригинальный запрос и нижний регистр для SQL
            # Но основной фильтр будет в Python
            search_pattern = f'%{search_query}%'
            search_params = [search_pattern, search_pattern, search_pattern]
        
        # Для поиска загружаем всех пользователей и фильтруем в Python
        # (так как SQLite LOWER() не работает с кириллицей)
        if search_query:
            # Загружаем всех пользователей для фильтрации в Python
            all_users = conn.execute('''
                SELECT 
                    u.user_id,
                    u.username,
                    u.avatar_seed,
                    u.avatar_style,
                    u.created_at,
                    u.last_login,
                    GROUP_CONCAT(r.display_name, ', ') as roles
                FROM users u
                LEFT JOIN user_roles ur ON u.user_id = ur.user_id
                LEFT JOIN roles r ON ur.role_id = r.id
                GROUP BY u.user_id
                ORDER BY u.created_at ASC
            ''').fetchall()
            
            # Фильтруем в Python с учетом регистра
            search_lower = search_query.lower()
            filtered_users = []
            for user in all_users:
                user_keys = user.keys()
                username = user['username'] if 'username' in user_keys else ''
                user_id_str = str(user['user_id']) if 'user_id' in user_keys else ''
                roles_str = user['roles'] if ('roles' in user_keys and user['roles']) else ''
                
                # Проверяем совпадение (регистронезависимо)
                if (search_query.lower() in username.lower() or 
                    search_query.lower() in user_id_str.lower() or
                    search_query.lower() in roles_str.lower()):
                    filtered_users.append(user)
            
            # Применяем пагинацию к отфильтрованным результатам
            total_count = len(filtered_users)
            offset = (page - 1) * per_page
            users = filtered_users[offset:offset + per_page]
        else:
            # Без поиска - обычная пагинация
            total_count = conn.execute('''
                SELECT COUNT(DISTINCT u.user_id)
                FROM users u
            ''').fetchone()[0]
            
            offset = (page - 1) * per_page
            users_query = '''
                SELECT 
                    u.user_id,
                    u.username,
                    u.avatar_seed,
                    u.avatar_style,
                    u.created_at,
                    u.last_login,
                    GROUP_CONCAT(r.display_name, ', ') as roles
                FROM users u
                LEFT JOIN user_roles ur ON u.user_id = ur.user_id
                LEFT JOIN roles r ON ur.role_id = r.id
                GROUP BY u.user_id
                ORDER BY u.created_at ASC
                LIMIT ? OFFSET ?
            '''
            users = conn.execute(users_query, [per_page, offset]).fetchall()
        
        # Для каждого пользователя определяем статус
        participants_data = []
        for user in users:
            # sqlite3.Row работает как словарь, но не имеет метода .get()
            # Используем прямой доступ с проверкой наличия ключей
            user_keys = user.keys()
            
            last_login = user['last_login'] if 'last_login' in user_keys else None
            
            status = 'Оффлайн'
            if last_login:
                try:
                    # Обрабатываем разные форматы даты
                    last_login_str = str(last_login).split('.')[0] if '.' in str(last_login) else str(last_login)
                    last_login_date = datetime.strptime(last_login_str, '%Y-%m-%d %H:%M:%S')
                    now = datetime.now()
                    if (now - last_login_date).total_seconds() < 3600:  # Меньше часа
                        status = 'Онлайн'
                    elif (now - last_login_date).days == 0:  # Сегодня
                        status = 'Был сегодня'
                except Exception as e:
                    user_id = user['user_id'] if 'user_id' in user_keys else 'unknown'
                    log_debug(f"Error parsing last_login for user {user_id}: {e}")
            
            # Обрабатываем роли - если их нет, используем 'Пользователь'
            roles_str = user['roles'] if ('roles' in user_keys and user['roles']) else 'Пользователь'
            
            # Получаем значения с обработкой отсутствующих ключей
            user_id = user['user_id'] if 'user_id' in user_keys else None
            username = user['username'] if ('username' in user_keys and user['username']) else 'Неизвестно'
            avatar_seed = user['avatar_seed'] if 'avatar_seed' in user_keys else None
            avatar_style = user['avatar_style'] if 'avatar_style' in user_keys else None
            created_at = user['created_at'] if ('created_at' in user_keys and user['created_at']) else 'N/A'
            
            participants_data.append({
                'user_id': user_id,
                'username': username,
                'avatar_seed': avatar_seed,
                'avatar_style': avatar_style,
                'status': status,
                'roles': roles_str,
                'created_at': created_at
            })
        
        conn.close()
        
        # Вычисляем данные для пагинации
        total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
        has_prev = page > 1
        has_next = page < total_pages
        
        # Логирование для отладки
        log_debug(f"Participants pagination: page={page}, per_page={per_page}, total_count={total_count}, total_pages={total_pages}, participants_count={len(participants_data)}")
        
        return render_template('participants.html', 
                             participants=participants_data,
                             get_avatar_url=get_avatar_url,
                             page=page,
                             per_page=per_page,
                             total_count=total_count,
                             total_pages=total_pages,
                             has_prev=has_prev,
                             has_next=has_next,
                             search_query=search_query)
    except Exception as e:
        log_error(f"Error in participants route: {e}")
        log_error(traceback.format_exc())
        try:
            conn.close()
        except:
            pass
        return f"Ошибка при загрузке участников: {str(e)}", 500

@app.route('/logout')
def logout():
    if session.get('user_id'):
        log_activity(
            'logout',
            details='Пользователь вышел из системы',
            metadata={'username': session.get('username')}
        )
    session.clear()
    flash('Вы успешно вышли из системы', 'success')
    return redirect(url_for('index'))

@app.route('/debug')
def debug():
    """Страница для отладки - показывает все параметры от GWars"""
    if request.args:
        # Получаем параметры
        sign = request.args.get('sign', '')
        user_id = request.args.get('user_id', '')
        
        # Получаем оригинальное закодированное имя из query_string
        query_string = request.query_string.decode('utf-8')
        name_encoded = None
        for param in query_string.split('&'):
            if param.startswith('name='):
                name_encoded = param.split('=', 1)[1]
                break
        
        if not name_encoded:
            name_encoded = request.args.get('name', '')
        
        # Пробуем декодировать разными способами
        try:
            name = unquote(name_encoded, encoding='utf-8')
        except:
            try:
                name = unquote(name_encoded, encoding='cp1251')
            except:
                try:
                    name = unquote(name_encoded, encoding='latin1')
                except:
                    name = name_encoded
        level = request.args.get('level', '0')
        synd = request.args.get('synd', '0')
        sign2 = request.args.get('sign2', '')
        has_passport = request.args.get('has_passport', '0')
        has_mobile = request.args.get('has_mobile', '0')
        old_passport = request.args.get('old_passport', '0')
        sign3 = request.args.get('sign3', '')
        sign4 = request.args.get('sign4', '')
        
        # Вычисляем все возможные варианты подписи
        variant1 = hashlib.md5((GWARS_PASSWORD + name + str(user_id)).encode('utf-8')).hexdigest()
        variant2 = hashlib.md5((GWARS_PASSWORD + name_encoded + str(user_id)).encode('utf-8')).hexdigest()
        variant3 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name).encode('utf-8')).hexdigest()
        variant4 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name_encoded).encode('utf-8')).hexdigest()
        
        # Пробуем latin1 декодирование
        try:
            name_latin1 = unquote(name_encoded, encoding='latin1')
            variant5 = hashlib.md5((GWARS_PASSWORD + name_latin1 + str(user_id)).encode('utf-8')).hexdigest()
        except:
            name_latin1 = None
            variant5 = None
        
        expected_sign2 = hashlib.md5(
            (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
        ).hexdigest()
        
        debug_info = {
            'received_params': dict(request.args),
            'password': GWARS_PASSWORD,
            'encoded_name': name_encoded,
            'decoded_name': name,
            'decoded_name_latin1': name_latin1 if name_latin1 else 'N/A',
            'user_id': user_id,
            'variant1': variant1,
            'variant2': variant2,
            'variant3': variant3,
            'variant4': variant4,
            'variant5': variant5 if variant5 else 'N/A',
            'received_sign': sign,
            'sign_match_v1': variant1 == sign,
            'sign_match_v2': variant2 == sign,
            'sign_match_v3': variant3 == sign,
            'sign_match_v4': variant4 == sign,
            'sign_match_v5': variant5 == sign if variant5 else False,
            'expected_sign2': expected_sign2,
            'received_sign2': sign2,
            'sign2_match': expected_sign2 == sign2,
        }
        
        return render_template('debug.html', debug_info=debug_info)
    return render_template('debug.html', debug_info=None)

# ========== Админ-панель ==========

@app.route('/admin')
@require_role('admin')
def admin_panel():
    """Главная страница админ-панели"""
    return render_template('admin/index.html')

@app.route('/admin/broadcasts')
@require_role('admin')
def admin_broadcasts():
    """Страница рассылок"""
    conn = get_db_connection()
    # Получаем всех пользователей с email и telegram
    users = conn.execute('''
        SELECT user_id, username, email, telegram, is_blocked
        FROM users
        ORDER BY username COLLATE NOCASE
    ''').fetchall()
    
    # Получаем историю рассылок
    broadcasts_history_raw = conn.execute('''
        SELECT id, created_by, created_by_username, recipient_type, delivery_method,
               subject, message, total_recipients, success_count, error_count,
               errors, created_at
        FROM broadcasts_history
        ORDER BY created_at DESC
        LIMIT 50
    ''').fetchall()
    
    # Получаем шаблоны рассылок
    templates = conn.execute('''
        SELECT id, name, description, delivery_method, subject, message, 
               created_by_username, created_at, updated_at
        FROM broadcast_templates
        ORDER BY updated_at DESC
    ''').fetchall()
    
    conn.close()
    
    # Парсим JSON ошибок для каждого элемента истории
    broadcasts_history = []
    for item in broadcasts_history_raw:
        item_dict = dict(item)
        # Парсим JSON ошибок, если они есть
        if item_dict.get('errors'):
            try:
                item_dict['errors_parsed'] = json.loads(item_dict['errors'])
            except (json.JSONDecodeError, TypeError):
                item_dict['errors_parsed'] = [item_dict['errors']] if item_dict['errors'] else []
        else:
            item_dict['errors_parsed'] = []
        broadcasts_history.append(item_dict)
    
    # Проверяем доступность интеграций
    smtp_enabled = get_setting('smtp_enabled', '0') == '1'
    smtp_verified = get_setting('smtp_verified', '0') == '1'
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    telegram_verified = get_setting('telegram_verified', '0') == '1'
    
    smtp_available = smtp_enabled and smtp_verified
    telegram_available = telegram_enabled and telegram_verified
    
    return render_template('admin/broadcasts.html', 
                         users=users,
                         smtp_available=smtp_available,
                         telegram_available=telegram_available,
                         broadcasts_history=broadcasts_history,
                         templates=templates)

@app.route('/admin/broadcasts/send', methods=['POST'])
@require_role('admin')
def admin_broadcasts_send():
    """Обработка отправки рассылки"""
    recipient_type = request.form.get('recipient_type', 'all')  # 'all' или 'selected'
    selected_users = request.form.getlist('selected_users')  # Список user_id
    delivery_method = request.form.get('delivery_method', 'email')  # 'email' или 'telegram'
    subject = request.form.get('subject', '').strip()
    message = request.form.get('message', '').strip()
    
    if not message:
        flash('Текст сообщения обязателен', 'error')
        return redirect(url_for('admin_broadcasts'))
    
    if delivery_method == 'email' and not subject:
        flash('Тема письма обязательна для email рассылки', 'error')
        return redirect(url_for('admin_broadcasts'))
    
    # Получаем список получателей с расширенными данными для плейсхолдеров
    conn = get_db_connection()
    if recipient_type == 'all':
        if delivery_method == 'email':
            recipients = conn.execute('''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE email IS NOT NULL AND email != '' AND is_blocked = 0
            ''').fetchall()
        else:  # telegram
            recipients = conn.execute('''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
            ''').fetchall()
    else:
        # Выбранные пользователи
        if not selected_users:
            conn.close()
            flash('Выберите хотя бы одного получателя', 'error')
            return redirect(url_for('admin_broadcasts'))
        
        placeholders = ','.join(['?'] * len(selected_users))
        if delivery_method == 'email':
            recipients = conn.execute(f'''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE user_id IN ({placeholders}) 
                  AND email IS NOT NULL AND email != '' AND is_blocked = 0
            ''', selected_users).fetchall()
        else:  # telegram
            recipients = conn.execute(f'''
                SELECT user_id, username, email, level, synd, phone, telegram,
                       first_name, last_name, city, country
                FROM users
                WHERE user_id IN ({placeholders}) 
                  AND telegram IS NOT NULL AND telegram != '' AND is_blocked = 0
            ''', selected_users).fetchall()
    
    conn.close()
    
    if not recipients:
        flash('Не найдено получателей с указанным способом доставки', 'error')
        return redirect(url_for('admin_broadcasts'))
    
    # Функция замены плейсхолдеров
    def replace_placeholders(text, recipient):
        """Заменяет плейсхолдеры в тексте на данные получателя"""
        # Формируем имя (приоритет: first_name + last_name, затем username)
        name = ''
        if recipient.get('first_name') or recipient.get('last_name'):
            name_parts = []
            if recipient.get('first_name'):
                name_parts.append(recipient['first_name'])
            if recipient.get('last_name'):
                name_parts.append(recipient['last_name'])
            name = ' '.join(name_parts).strip()
        if not name:
            name = recipient.get('username', '')
        
        replacements = {
            '[name]': name,
            '[username]': recipient.get('username', ''),
            '[email]': recipient.get('email', ''),
            '[telegram]': recipient.get('telegram', ''),
            '[phone]': recipient.get('phone', ''),
            '[id]': str(recipient.get('user_id', '')),
            '[level]': str(recipient.get('level', '')) if recipient.get('level') else '',
            '[syndicate]': str(recipient.get('synd', '')) if recipient.get('synd') else '',
            '[first_name]': recipient.get('first_name', ''),
            '[last_name]': recipient.get('last_name', ''),
            '[city]': recipient.get('city', ''),
            '[country]': recipient.get('country', ''),
        }
        
        result = text
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, value)
        
        return result
    
    # Отправляем сообщения
    success_count = 0
    error_count = 0
    errors = []
    
    for recipient in recipients:
        try:
            # Заменяем плейсхолдеры в сообщении и теме
            personalized_message = replace_placeholders(message, recipient)
            personalized_subject = replace_placeholders(subject, recipient) if subject else ''
            
            if delivery_method == 'email':
                email = recipient['email']
                success, result_message = send_email_via_smtp(
                    to_email=email,
                    subject=personalized_subject,
                    body=personalized_message
                )
            else:  # telegram
                telegram = recipient['telegram']
                # Если telegram начинается с @, используем как username, иначе как chat_id
                success, result_message = send_telegram_message(
                    message=personalized_message,
                    chat_id=telegram
                )
            
            if success:
                success_count += 1
                log_activity(
                    'broadcast_sent',
                    details=f'Рассылка отправлена пользователю {recipient["username"]} (ID: {recipient["user_id"]}) через {delivery_method}',
                    metadata={
                        'recipient_id': recipient['user_id'],
                        'recipient_username': recipient['username'],
                        'delivery_method': delivery_method,
                        'subject': subject if delivery_method == 'email' else None
                    }
                )
            else:
                error_count += 1
                errors.append(f"{recipient['username']}: {result_message}")
        except Exception as e:
            error_count += 1
            errors.append(f"{recipient['username']}: {str(e)}")
            log_error(f"Error sending broadcast to {recipient['username']}: {e}")
    
    # Сохраняем историю рассылки
    conn = get_db_connection()
    try:
        errors_json = json.dumps(errors, ensure_ascii=False) if errors else None
        conn.execute('''
            INSERT INTO broadcasts_history 
            (created_by, created_by_username, recipient_type, delivery_method, subject, 
             message, total_recipients, success_count, error_count, errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            session.get('user_id'),
            session.get('username'),
            recipient_type,
            delivery_method,
            subject if delivery_method == 'email' else None,
            message,
            len(recipients),
            success_count,
            error_count,
            errors_json
        ))
        conn.commit()
    except Exception as e:
        log_error(f"Error saving broadcast history: {e}")
    finally:
        conn.close()
    
    # Формируем сообщение о результате
    if success_count > 0 and error_count == 0:
        flash(f'Рассылка успешно отправлена {success_count} получателям', 'success')
    elif success_count > 0:
        flash(f'Рассылка отправлена {success_count} получателям. Ошибок: {error_count}. Детали: {"; ".join(errors[:5])}', 'warning')
    else:
        flash(f'Не удалось отправить рассылку. Ошибки: {"; ".join(errors[:5])}', 'error')
    
    log_activity(
        'broadcast_completed',
        details=f'Рассылка завершена: успешно {success_count}, ошибок {error_count}',
        metadata={
            'recipient_type': recipient_type,
            'delivery_method': delivery_method,
            'success_count': success_count,
            'error_count': error_count,
            'total_recipients': len(recipients)
        }
    )
    
    return redirect(url_for('admin_broadcasts'))

@app.route('/admin/broadcasts/templates', methods=['GET', 'POST'])
@require_role('admin')
def admin_broadcasts_templates():
    """Управление шаблонами рассылок"""
    if request.method == 'GET':
        # Редирект на главную страницу рассылок с табом шаблонов
        return redirect(url_for('admin_broadcasts') + '#templates')
    
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            delivery_method = request.form.get('delivery_method', 'email')
            subject = request.form.get('subject', '').strip()
            message = request.form.get('message', '').strip()
            
            if not name or not message:
                flash('Название и текст сообщения обязательны', 'error')
                conn.close()
                return redirect(url_for('admin_broadcasts_templates'))
            
            if delivery_method == 'email' and not subject:
                flash('Тема обязательна для email шаблона', 'error')
                conn.close()
                return redirect(url_for('admin_broadcasts_templates'))
            
            try:
                conn.execute('''
                    INSERT INTO broadcast_templates 
                    (name, description, delivery_method, subject, message, created_by, created_by_username)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    name,
                    description,
                    delivery_method,
                    subject if delivery_method == 'email' else None,
                    message,
                    session.get('user_id'),
                    session.get('username')
                ))
                conn.commit()
                flash('Шаблон успешно создан', 'success')
                log_activity(
                    'broadcast_template_created',
                    details=f'Создан шаблон рассылки "{name}"',
                    metadata={'template_name': name, 'delivery_method': delivery_method}
                )
            except Exception as e:
                log_error(f"Error creating broadcast template: {e}")
                flash('Ошибка при создании шаблона', 'error')
        
        elif action == 'update':
            template_id = request.form.get('template_id')
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            delivery_method = request.form.get('delivery_method', 'email')
            subject = request.form.get('subject', '').strip()
            message = request.form.get('message', '').strip()
            
            if not template_id or not name or not message:
                flash('Название и текст сообщения обязательны', 'error')
                conn.close()
                return redirect(url_for('admin_broadcasts_templates'))
            
            if delivery_method == 'email' and not subject:
                flash('Тема обязательна для email шаблона', 'error')
                conn.close()
                return redirect(url_for('admin_broadcasts_templates'))
            
            try:
                conn.execute('''
                    UPDATE broadcast_templates
                    SET name = ?, description = ?, delivery_method = ?, 
                        subject = ?, message = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    name,
                    description,
                    delivery_method,
                    subject if delivery_method == 'email' else None,
                    message,
                    template_id
                ))
                conn.commit()
                flash('Шаблон успешно обновлен', 'success')
                log_activity(
                    'broadcast_template_updated',
                    details=f'Обновлен шаблон рассылки "{name}"',
                    metadata={'template_id': template_id, 'template_name': name}
                )
            except Exception as e:
                log_error(f"Error updating broadcast template: {e}")
                flash('Ошибка при обновлении шаблона', 'error')
        
        elif action == 'delete':
            template_id = request.form.get('template_id')
            if template_id:
                try:
                    template = conn.execute('SELECT name FROM broadcast_templates WHERE id = ?', (template_id,)).fetchone()
                    conn.execute('DELETE FROM broadcast_templates WHERE id = ?', (template_id,))
                    conn.commit()
                    flash('Шаблон успешно удален', 'success')
                    if template:
                        log_activity(
                            'broadcast_template_deleted',
                            details=f'Удален шаблон рассылки "{template["name"]}"',
                            metadata={'template_id': template_id}
                        )
                except Exception as e:
                    log_error(f"Error deleting broadcast template: {e}")
                    flash('Ошибка при удалении шаблона', 'error')
    
    conn.close()
    
    # После POST запроса редиректим обратно на страницу рассылок с табом шаблонов
    return redirect(url_for('admin_broadcasts') + '#templates')

@app.route('/admin/broadcasts/templates/<int:template_id>')
@require_role('admin')
def admin_broadcasts_template_get(template_id):
    """Получение шаблона по ID (для AJAX)"""
    conn = get_db_connection()
    template = conn.execute('''
        SELECT id, name, description, delivery_method, subject, message
        FROM broadcast_templates
        WHERE id = ?
    ''', (template_id,)).fetchone()
    conn.close()
    
    if template:
        return jsonify(dict(template))
    return jsonify({'error': 'Template not found'}), 404

@app.route('/admin/telegram/menu', methods=['GET', 'POST'])
@require_role('admin')
def admin_telegram_menu():
    """Управление меню Telegram бота"""
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            button_text = request.form.get('button_text', '').strip()
            button_type = request.form.get('button_type', 'command')
            action_value = request.form.get('action_value', '').strip()
            sort_order = int(request.form.get('sort_order', 100))
            
            if not button_text or not action_value:
                conn.close()
                flash('Название кнопки и действие обязательны', 'error')
                return redirect(url_for('admin_settings') + '#integrations')
            
            try:
                conn.execute('''
                    INSERT INTO telegram_bot_menu (button_text, button_type, action, sort_order, is_active)
                    VALUES (?, ?, ?, ?, 1)
                ''', (button_text, button_type, action_value, sort_order))
                conn.commit()
                flash('Пункт меню успешно добавлен', 'success')
            except Exception as e:
                log_error(f"Error creating menu item: {e}")
                flash('Ошибка при добавлении пункта меню', 'error')
        
        elif action == 'update':
            menu_id = request.form.get('menu_id')
            button_text = request.form.get('button_text', '').strip()
            button_type = request.form.get('button_type', 'command')
            action_value = request.form.get('action_value', '').strip()
            sort_order = int(request.form.get('sort_order', 100))
            is_active = 1 if request.form.get('is_active') == '1' else 0
            
            if not menu_id or not button_text or not action_value:
                conn.close()
                flash('Все поля обязательны', 'error')
                return redirect(url_for('admin_settings') + '#integrations')
            
            try:
                conn.execute('''
                    UPDATE telegram_bot_menu
                    SET button_text = ?, button_type = ?, action = ?, sort_order = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (button_text, button_type, action_value, sort_order, is_active, menu_id))
                conn.commit()
                flash('Пункт меню успешно обновлен', 'success')
            except Exception as e:
                log_error(f"Error updating menu item: {e}")
                flash('Ошибка при обновлении пункта меню', 'error')
        
        elif action == 'delete':
            menu_id = request.form.get('menu_id')
            if menu_id:
                try:
                    conn.execute('DELETE FROM telegram_bot_menu WHERE id = ?', (menu_id,))
                    conn.commit()
                    flash('Пункт меню успешно удален', 'success')
                except Exception as e:
                    log_error(f"Error deleting menu item: {e}")
                    flash('Ошибка при удалении пункта меню', 'error')
        
        conn.close()
        return redirect(url_for('admin_settings') + '#integrations')
    
    # GET - возвращаем список меню
    menu_items = conn.execute('''
        SELECT id, button_text, button_type, action, sort_order, is_active, created_at, updated_at
        FROM telegram_bot_menu
        ORDER BY sort_order ASC
    ''').fetchall()
    conn.close()
    
    return jsonify([dict(item) for item in menu_items])

@app.route('/admin/telegram/menu/<int:menu_id>', methods=['GET'])
@require_role('admin')
def admin_telegram_menu_get(menu_id):
    """Получение пункта меню по ID"""
    conn = get_db_connection()
    menu_item = conn.execute('''
        SELECT id, button_text, button_type, action, sort_order, is_active
        FROM telegram_bot_menu
        WHERE id = ?
    ''', (menu_id,)).fetchone()
    conn.close()
    
    if menu_item:
        return jsonify(dict(menu_item))
    return jsonify({'error': 'Menu item not found'}), 404

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """Вебхук для обработки сообщений от Telegram бота"""
    if not requests:
        return jsonify({'ok': False, 'error': 'requests library not available'}), 500
    
    # Проверяем, что бот включен
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    telegram_verified = get_setting('telegram_verified', '0') == '1'
    if not telegram_enabled or not telegram_verified:
        return jsonify({'ok': False, 'error': 'Telegram bot not enabled or verified'}), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'ok': False}), 400
        
        message = data.get('message')
        callback_query = data.get('callback_query')
        
        if callback_query:
            # Обработка нажатий на кнопки
            try:
                return handle_telegram_callback(callback_query)
            except Exception as e:
                log_error(f"Error handling callback: {e}")
                return jsonify({'ok': True})  # Возвращаем ok, чтобы Telegram не повторял запрос
        elif message:
            # Обработка текстовых сообщений и команд
            try:
                return handle_telegram_message(message)
            except Exception as e:
                log_error(f"Error handling message: {e}")
                return jsonify({'ok': True})  # Возвращаем ok, чтобы Telegram не повторял запрос
        
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error processing Telegram webhook: {e}")
        import traceback
        log_error(traceback.format_exc())
        return jsonify({'ok': True})  # Возвращаем ok, чтобы Telegram не повторял запрос

def handle_telegram_message(message):
    """Обрабатывает сообщения от пользователей в Telegram"""
    chat_id = message.get('chat', {}).get('id')
    text = message.get('text', '').strip()
    username = message.get('from', {}).get('username')
    
    if not chat_id:
        return jsonify({'ok': False, 'error': 'No chat_id'}), 400
    
    # Обработка команд
    if text.startswith('/'):
        command = text.split()[0].lower()
        
        if command == '/start':
            return handle_start_command(chat_id, username, text)
        elif command == '/menu':
            return handle_menu_command(chat_id)
        elif command == '/verify':
            return handle_verify_command(chat_id, text)
        elif command == '/events':
            return handle_events_command(chat_id)
        elif command == '/assignments':
            return handle_assignments_command(chat_id)
        else:
            send_telegram_message_with_keyboard(
                "Неизвестная команда. Используйте /menu для просмотра меню.",
                chat_id
            )
            return jsonify({'ok': True})
    
    # Обработка кода верификации (6 цифр)
    elif text.isdigit() and len(text) == 6:
        return handle_verification_code(chat_id, text, username)
    
    # Обработка обычных сообщений
    else:
        send_telegram_message_with_keyboard(
            "Используйте /menu для просмотра доступных команд.",
            chat_id
        )
        return jsonify({'ok': True})

def handle_telegram_callback(callback_query):
    """Обрабатывает нажатия на inline кнопки"""
    chat_id = callback_query.get('message', {}).get('chat', {}).get('id')
    data = callback_query.get('data', '')
    
    if not chat_id:
        return jsonify({'ok': False, 'error': 'No chat_id'}), 400
    
    # Обработка callback_data
    if data.startswith('cmd_'):
        command = data.replace('cmd_', '')
        if command == 'events':
            return handle_events_command(chat_id)
        elif command == 'assignments':
            return handle_assignments_command(chat_id)
        elif command == 'faq':
            return handle_faq_command(chat_id)
        elif command == 'rules':
            return handle_rules_command(chat_id)
    
    # Отправляем подтверждение нажатия кнопки
    token = get_setting('telegram_bot_token', '')
    if token:
        try:
            api_url = f'https://api.telegram.org/bot{token}/answerCallbackQuery'
            requests.post(api_url, json={'callback_query_id': callback_query.get('id')}, timeout=5)
        except:
            pass
    
    return jsonify({'ok': True})

def handle_start_command(chat_id, username, full_text):
    """Обрабатывает команду /start"""
    # Проверяем, есть ли код верификации в команде
    parts = full_text.split()
    if len(parts) > 1:
        verification_code = parts[1]
        # Это может быть код верификации или user_id
        return handle_start_with_code(chat_id, username, verification_code)
    
    # Обычное приветствие
    welcome_text = (
        "👋 Добро пожаловать в бота Анонимных Дедов Морозов!\n\n"
        "Для использования бота необходимо привязать ваш аккаунт.\n"
        "Перейдите в свой профиль на сайте и запросите код верификации."
    )
    
    send_telegram_message_with_keyboard(welcome_text, chat_id)
    return jsonify({'ok': True})

def handle_start_with_code(chat_id, username, code):
    """Обрабатывает /start с кодом верификации"""
    # Если код - это user_id (обычно 6+ цифр), генерируем код верификации
    if code.isdigit():
        try:
            user_id = int(code)
            # Проверяем, что это действительно user_id (обычно больше 100000)
            # Или это может быть код верификации (ровно 6 цифр)
            if len(code) == 6:
                # Это код верификации
                handle_verification_code(chat_id, code, username)
            elif user_id > 100000:
                # Это user_id, генерируем код верификации
                verification_code = generate_telegram_verification_code(user_id)
                if verification_code:
                    send_telegram_message_with_keyboard(
                        f"Ваш код верификации: {verification_code}\n\n"
                        "Введите этот код в бота для завершения привязки аккаунта.\n"
                        f"Или используйте команду: /verify {verification_code}",
                        chat_id
                    )
                else:
                    send_telegram_message_with_keyboard(
                        "Ошибка при генерации кода. Попробуйте позже.",
                        chat_id
                    )
            else:
                # Небольшое число, возможно код верификации
                handle_verification_code(chat_id, code, username)
        except ValueError:
            # Не число, игнорируем
            send_telegram_message_with_keyboard(
                "Используйте /menu для просмотра доступных команд.",
                chat_id
            )
    else:
        # Не число, игнорируем
        send_telegram_message_with_keyboard(
            "Используйте /menu для просмотра доступных команд.",
            chat_id
        )
    
    return jsonify({'ok': True})

def handle_menu_command(chat_id):
    """Показывает меню бота"""
    menu_items = get_telegram_bot_menu()
    if not menu_items:
        send_telegram_message_with_keyboard("Меню пока не настроено.", chat_id)
        return jsonify({'ok': True})
    
    # Формируем inline клавиатуру
    keyboard = {'inline_keyboard': []}
    row = []
    
    for item in menu_items:
        button_text = item['button_text']
        button_type = item['button_type']
        action = item['action']
        
        if button_type == 'command':
            row.append({'text': button_text, 'callback_data': f'cmd_{action}'})
        elif button_type == 'url':
            # Получаем базовый URL сайта
            base_url = get_base_url()
            full_url = action if action.startswith('http') else f"{base_url}{action}"
            row.append({'text': button_text, 'url': full_url})
        
        # Добавляем кнопки по 2 в ряд
        if len(row) >= 2:
            keyboard['inline_keyboard'].append(row)
            row = []
    
    if row:
        keyboard['inline_keyboard'].append(row)
    
    menu_text = "📋 Главное меню:\n\nВыберите раздел:"
    send_telegram_message_with_keyboard(menu_text, chat_id, keyboard)
    return jsonify({'ok': True})

def handle_verify_command(chat_id, full_text):
    """Обрабатывает команду /verify"""
    parts = full_text.split()
    if len(parts) > 1:
        code = parts[1]
        username = None
        return handle_verification_code(chat_id, code, username)
    else:
        send_telegram_message_with_keyboard(
            "Для верификации введите команду:\n/verify <код>\n\n"
            "Код можно получить в вашем профиле на сайте.",
            chat_id
        )
    return jsonify({'ok': True})

def handle_verification_code(chat_id, code, username):
    """Обрабатывает код верификации"""
    conn = get_db_connection()
    try:
        # Ищем пользователя с этим кодом
        telegram_user = conn.execute('''
            SELECT user_id, verification_code, verification_code_expires_at
            FROM telegram_users
            WHERE verification_code = ? AND verified = 0
        ''', (code,)).fetchone()
        
        if not telegram_user:
            send_telegram_message_with_keyboard(
                "Код верификации не найден или уже использован.\n"
                "Запросите новый код в вашем профиле на сайте.",
                chat_id
            )
            return jsonify({'ok': True})
        
        user_id = telegram_user['user_id']
        expires_at_str = telegram_user['verification_code_expires_at']
        
        # Проверяем срок действия
        if expires_at_str:
            expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
            if expires_at.tzinfo:
                expires_at = expires_at.replace(tzinfo=None)
            if datetime.utcnow() > expires_at:
                send_telegram_message_with_keyboard(
                    "Код верификации истёк. Запросите новый код в вашем профиле.",
                    chat_id
                )
                return jsonify({'ok': True})
        
        # Связываем пользователя с Telegram
        success, message = verify_telegram_code(user_id, code, str(chat_id), username)
        
        if success:
            send_telegram_message_with_keyboard(
                f"✅ {message}\n\nИспользуйте /menu для просмотра доступных команд.",
                chat_id
            )
        else:
            send_telegram_message_with_keyboard(f"❌ {message}", chat_id)
        
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error handling verification code: {e}")
        send_telegram_message_with_keyboard(
            "Произошла ошибка при верификации. Попробуйте позже.",
            chat_id
        )
        return jsonify({'ok': True})
    finally:
        conn.close()

def get_base_url():
    """Получает базовый URL сайта"""
    # Сначала проверяем настройку из БД
    site_url = get_setting('site_url', '')
    if site_url:
        return site_url.rstrip('/')
    
    # Затем пытаемся получить из request
    try:
        if has_request_context():
            return request.host_url.rstrip('/')
    except:
        pass
    
    # Fallback: дефолтное значение для разработки
    return 'http://localhost:5000'

def handle_events_command(chat_id):
    """Показывает список мероприятий"""
    conn = get_db_connection()
    try:
        events = conn.execute('''
            SELECT id, name, description
            FROM events
            WHERE deleted_at IS NULL
            ORDER BY created_at DESC
            LIMIT 10
        ''').fetchall()
        conn.close()
        
        if not events:
            send_telegram_message_with_keyboard("Мероприятия не найдены.", chat_id)
            return jsonify({'ok': True})
        
        base_url = get_base_url()
        text = "🎉 Мероприятия:\n\n"
        keyboard = {'inline_keyboard': []}
        
        for event in events:
            event_id = event['id']
            event_name = event['name']
            event_url = f"{base_url}/events#{event_id}"
            text += f"• {event_name}\n"
            keyboard['inline_keyboard'].append([{
                'text': f"📋 {event_name}",
                'url': event_url
            }])
        
        send_telegram_message_with_keyboard(text, chat_id, keyboard)
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error handling events command: {e}")
        send_telegram_message_with_keyboard("Ошибка при получении мероприятий.", chat_id)
        return jsonify({'ok': True})

def handle_assignments_command(chat_id):
    """Показывает задания пользователя"""
    conn = get_db_connection()
    try:
        # Находим user_id по chat_id
        telegram_user = conn.execute('''
            SELECT user_id FROM telegram_users
            WHERE telegram_chat_id = ? AND verified = 1
        ''', (str(chat_id),)).fetchone()
        
        if not telegram_user:
            send_telegram_message_with_keyboard(
                "Ваш аккаунт не привязан к Telegram. Используйте /verify для привязки.",
                chat_id
            )
            return jsonify({'ok': True})
        
        user_id = telegram_user['user_id']
        
        # Получаем задания пользователя
        assignments = conn.execute('''
            SELECT ea.id, ea.event_id, e.name as event_name,
                   ea.recipient_user_id, u.username as recipient_username,
                   ea.santa_sent_at, ea.recipient_received_at
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            JOIN users u ON ea.recipient_user_id = u.user_id
            WHERE ea.santa_user_id = ? AND e.deleted_at IS NULL
            ORDER BY ea.assigned_at DESC
            LIMIT 10
        ''', (user_id,)).fetchall()
        conn.close()
        
        if not assignments:
            send_telegram_message_with_keyboard(
                "У вас пока нет заданий.",
                chat_id
            )
            return jsonify({'ok': True})
        
        base_url = get_base_url()
        text = "📋 Ваши задания:\n\n"
        keyboard = {'inline_keyboard': []}
        
        for assignment in assignments:
            event_name = assignment['event_name']
            recipient = assignment['recipient_username']
            sent = "✅" if assignment['santa_sent_at'] else "⏳"
            received = "✅" if assignment['recipient_received_at'] else "⏳"
            
            text += f"{sent} Отправить: {recipient}\n"
            text += f"{received} Получить от: {recipient}\n"
            text += f"Мероприятие: {event_name}\n\n"
            
            assignment_url = f"{base_url}/assignments"
            keyboard['inline_keyboard'].append([{
                'text': f"📋 {event_name}",
                'url': assignment_url
            }])
        
        send_telegram_message_with_keyboard(text, chat_id, keyboard)
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error handling assignments command: {e}")
        send_telegram_message_with_keyboard("Ошибка при получении заданий.", chat_id)
        return jsonify({'ok': True})

def handle_faq_command(chat_id):
    """Отправляет ссылку на FAQ"""
    base_url = get_base_url()
    faq_url = f"{base_url}/faq"
    keyboard = {'inline_keyboard': [[{'text': '📖 Открыть FAQ', 'url': faq_url}]]}
    send_telegram_message_with_keyboard(
        "❓ Часто задаваемые вопросы:\n\nНажмите кнопку ниже, чтобы открыть FAQ на сайте.",
        chat_id,
        keyboard
    )
    return jsonify({'ok': True})

def handle_rules_command(chat_id):
    """Отправляет ссылку на правила"""
    base_url = get_base_url()
    rules_url = f"{base_url}/rules"
    keyboard = {'inline_keyboard': [[{'text': '📜 Открыть правила', 'url': rules_url}]]}
    send_telegram_message_with_keyboard(
        "📜 Правила проекта:\n\nНажмите кнопку ниже, чтобы открыть правила на сайте.",
        chat_id,
        keyboard
    )
    return jsonify({'ok': True})

@app.route('/admin/test')
def admin_test():
    """Тестовый маршрут для проверки загрузки админ-панели"""
    user_id = session.get('user_id')
    roles = session.get('roles', [])
    has_admin = has_role(user_id, 'admin') if user_id else False
    return f"""
    <h1>Admin Test Route</h1>
    <p>User ID: {user_id or 'Not logged in'}</p>
    <p>Session roles: {roles}</p>
    <p>Has admin role (check): {has_admin}</p>
    <p>User roles from DB: {get_user_roles(user_id) if user_id else 'N/A'}</p>
    <p><a href="/admin">Try /admin</a></p>
    <p><a href="/dashboard">Dashboard</a></p>
    """

@app.route('/admin/users')
@require_role('admin')
def admin_users():
    """Управление пользователями"""
    conn = get_db_connection()
    users = conn.execute('''
        SELECT u.*, 
               GROUP_CONCAT(r.display_name, ', ') as roles
        FROM users u
        LEFT JOIN user_roles ur ON u.user_id = ur.user_id
        LEFT JOIN roles r ON ur.role_id = r.id
        GROUP BY u.user_id
        ORDER BY u.created_at DESC
    ''').fetchall()
    
    roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    roles_with_counts = []
    for role in roles:
        count = conn.execute('''
            SELECT COUNT(*) as count FROM user_roles WHERE role_id = ?
        ''', (role['id'],)).fetchone()
        roles_with_counts.append({
            **dict(role),
            'user_count': count['count']
        })
    
    conn.close()
    
    return render_template('admin/users.html', users=users, roles=roles_with_counts)

@app.route('/admin/users/<int:user_id>/impersonate', methods=['POST'])
@require_role('admin')
def admin_user_impersonate(user_id):
    """Позволяет администратору управлять выбранным пользователем"""
    # Проверяем, не активна ли уже импровизированная сессия
    if session.get('impersonation_original'):
        flash('Вы уже управляете другим пользователем. Завершите текущую сессию управления сначала.', 'warning')
        next_url = request.form.get('next')
        if not next_url or not next_url.startswith('/'):
            next_url = url_for('view_profile', user_id=user_id)
        return redirect(next_url)
    
    # Если администратор пытается управлять собой
    if session.get('user_id') == user_id:
        flash('Вы уже авторизованы под этим пользователем.', 'info')
        next_url = request.form.get('next')
        if not next_url or not next_url.startswith('/'):
            next_url = url_for('view_profile', user_id=user_id)
        return redirect(next_url)
    
    conn = get_db_connection()
    user = conn.execute('SELECT user_id, username, level, synd FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
    if not user:
        conn.close()
        flash('Пользователь не найден', 'error')
        next_url = request.form.get('next')
        if not next_url or not next_url.startswith('/'):
            next_url = url_for('admin_users')
        return redirect(next_url)
    
    # Сохраняем данные исходной сессии администратора
    original_info = {
        'user_id': session.get('user_id'),
        'username': session.get('username'),
        'roles': list(session.get('roles', [])) if session.get('roles') else [],
        'level': session.get('level'),
        'synd': session.get('synd')
    }
    session['impersonation_original'] = original_info
    session['impersonation_target'] = {
        'user_id': user['user_id'],
        'username': user['username']
    }
    session['impersonation_started_at'] = datetime.now().isoformat()
    
    return_url = request.form.get('return_url')
    if return_url and return_url.startswith('/'):
        session['impersonation_return_url'] = return_url
    else:
        session['impersonation_return_url'] = url_for('admin_users')
    
    log_activity(
        'impersonation_start',
        details=f"Начат режим управления пользователем {user['username']} ({user['user_id']})",
        metadata={
            'target_user_id': user['user_id'],
            'target_username': user['username']
        }
    )
    
    # Обновляем сессию под выбранного пользователя
    session['user_id'] = user['user_id']
    session['username'] = user['username']
    session['level'] = user['level']
    session['synd'] = user['synd']
    session['roles'] = get_user_role_names(user['user_id'])
    
    conn.close()
    
    next_url = request.form.get('next')
    if not next_url or not next_url.startswith('/'):
        next_url = url_for('view_profile', user_id=user['user_id'])
    
    flash(f'Вы управляете пользователем {user["username"]}', 'info')
    return redirect(next_url)

@app.route('/impersonation/stop', methods=['POST'])
@require_login
def stop_impersonation():
    """Завершает режим управления пользователем"""
    original_info = session.get('impersonation_original')
    target_info = session.get('impersonation_target') or {}
    if not original_info:
        flash('Режим управления не активен.', 'error')
        return redirect(url_for('dashboard'))
    
    # Восстанавливаем исходные данные администратора
    session['user_id'] = original_info.get('user_id')
    session['username'] = original_info.get('username')
    session['roles'] = original_info.get('roles', [])
    session['level'] = original_info.get('level')
    session['synd'] = original_info.get('synd')
    
    impersonation_started = session.get('impersonation_started_at')
    return_url = session.get('impersonation_return_url')
    
    duration_seconds = None
    if impersonation_started:
        try:
            start_dt = datetime.fromisoformat(impersonation_started)
            duration_seconds = max(0, int((datetime.now() - start_dt).total_seconds()))
        except (ValueError, TypeError):
            duration_seconds = None
    
    log_activity(
        'impersonation_stop',
        details=f"Завершен режим управления пользователем {target_info.get('username', '') or target_info.get('user_id', 'неизвестно')}",
        metadata={
            'target_user_id': target_info.get('user_id'),
            'target_username': target_info.get('username'),
            'duration_seconds': duration_seconds
        }
    )
    
    # Очищаем данные импровизированной сессии
    session.pop('impersonation_original', None)
    session.pop('impersonation_target', None)
    session.pop('impersonation_started_at', None)
    session.pop('impersonation_return_url', None)
    
    flash('Вы вернулись к своей учетной записи.', 'success')
    
    if return_url and return_url.startswith('/'):
        return redirect(return_url)
    
    # Если исходная ссылка недоступна, возвращаем на страницу управления пользователями
    if 'admin' in session.get('roles', []):
        return redirect(url_for('admin_users'))
    return redirect(url_for('dashboard'))
@app.route('/admin/users/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_create():
    """Создание нового пользователя"""
    available_languages = app.config.get('LANGUAGES', {'ru': 'Русский', 'en': 'English'})
    if request.method == 'POST':
        user_id = request.form.get('user_id', '').strip()
        username = request.form.get('username', '').strip()
        level = request.form.get('level', '0')
        synd = request.form.get('synd', '0')
        has_passport = request.form.get('has_passport', '0')
        has_mobile = request.form.get('has_mobile', '0')
        old_passport = request.form.get('old_passport', '0')
        usersex = request.form.get('usersex', '0')
        bio = request.form.get('bio', '').strip()
        contact_info = request.form.get('contact_info', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        telegram = request.form.get('telegram', '').strip()
        whatsapp = request.form.get('whatsapp', '').strip()
        viber = request.form.get('viber', '').strip()
        last_name = request.form.get('last_name', '').strip()
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        postal_code = request.form.get('postal_code', '').strip()
        country = request.form.get('country', '').strip()
        city = request.form.get('city', '').strip()
        street = request.form.get('street', '').strip()
        house = request.form.get('house', '').strip()
        building = request.form.get('building', '').strip()
        apartment = request.form.get('apartment', '').strip()
        language = request.form.get('language', 'ru').strip()
        avatar_seed_form = request.form.get('avatar_seed', '').strip()
        avatar_style = request.form.get('avatar_style', '').strip()
        
        if not user_id or not username:
            flash('ID и имя пользователя обязательны', 'error')
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
        
        try:
            user_id_int = int(user_id)
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = int(has_passport)
            has_mobile_int = int(has_mobile)
            old_passport_int = int(old_passport)
        except ValueError:
            flash('Неверный формат числовых полей', 'error')
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
        
        conn = get_db_connection()
        
        # Проверяем, существует ли пользователь
        existing = conn.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id_int,)).fetchone()
        if existing:
            flash('Пользователь с таким ID уже существует', 'error')
            conn.close()
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
        
        try:
            if language not in available_languages:
                language = 'ru'
            avatar_seed = avatar_seed_form or generate_unique_avatar_seed(user_id_int)
            if not avatar_style or avatar_style not in AVATAR_STYLES:
                avatar_style = 'avataaars'
            
            conn.execute('''
                INSERT INTO users 
                (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, 
                 avatar_seed, avatar_style, language,
                 bio, contact_info, email, phone, telegram, whatsapp, viber,
                 last_name, first_name, middle_name,
                 postal_code, country, city, street, house, building, apartment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id_int, username, level_int, synd_int, has_passport_int, has_mobile_int, old_passport_int,
                  usersex, avatar_seed, avatar_style, language,
                  bio, contact_info, email, phone, telegram, whatsapp, viber,
                  last_name, first_name, middle_name,
                  postal_code, country, city, street, house, building, apartment))
            conn.commit()
            log_activity(
                'admin_user_create',
                details=f'Создан пользователь {username} (ID {user_id_int})',
                metadata={'target_user_id': user_id_int, 'username': username}
            )
            flash('Пользователь успешно создан', 'success')
            conn.close()
            return redirect(url_for('admin_users'))
        except Exception as e:
            log_error(f"Error creating user: {e}")
            flash(f'Ошибка создания пользователя: {str(e)}', 'error')
            conn.close()
            return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
    
    return render_template('admin/user_form.html', user=None, avatar_styles=AVATAR_STYLES, available_languages=available_languages)
@app.route('/admin/users/<int:user_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_edit(user_id):
    """Редактирование пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    available_languages = app.config.get('LANGUAGES', {'ru': 'Русский', 'en': 'English'})
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('admin_users'))
    
    if request.method == 'POST':
        # Обработка назначения/удаления ролей
        role_action = request.form.get('role_action')
        if role_action:
            role_name = request.form.get('role_name')
            if role_action == 'assign' and role_name:
                if assign_role(user_id, role_name, assigned_by=session['user_id']):
                    flash(f'Роль "{role_name}" успешно назначена', 'success')
                else:
                    flash(f'Ошибка назначения роли', 'error')
            elif role_action == 'remove' and role_name:
                if remove_role(user_id, role_name):
                    flash(f'Роль "{role_name}" успешно удалена', 'success')
                else:
                    flash(f'Ошибка удаления роли', 'error')
        
        # Обработка назначения/удаления званий
        title_action = request.form.get('title_action')
        if title_action:
            title_id = request.form.get('title_id')
            if title_action == 'assign' and title_id:
                try:
                    title_id_int = int(title_id)
                    if assign_title(user_id, title_id_int, assigned_by=session['user_id']):
                        flash('Звание успешно назначено', 'success')
                    else:
                        flash('Ошибка назначения звания', 'error')
                except ValueError:
                    flash('Неверный формат ID звания', 'error')
            elif title_action == 'remove' and title_id:
                try:
                    title_id_int = int(title_id)
                    if remove_title(user_id, title_id_int):
                        flash('Звание успешно удалено', 'success')
                    else:
                        flash('Ошибка удаления звания', 'error')
                except ValueError:
                    flash('Неверный формат ID звания', 'error')
        
        # Обработка блокировки/разблокировки пользователя
        block_action = request.form.get('block_action')
        if block_action:
            if block_action == 'block':
                blocked_reason = request.form.get('blocked_reason', '').strip()
                if not blocked_reason:
                    flash('Причина блокировки обязательна', 'error')
                else:
                    try:
                        blocked_by = session['user_id']
                        blocked_at = datetime.utcnow()
                        conn.execute('''
                            UPDATE users SET
                                is_blocked = 1,
                                blocked_by = ?,
                                blocked_reason = ?,
                                blocked_at = ?
                            WHERE user_id = ?
                        ''', (blocked_by, blocked_reason, blocked_at, user_id))
                        conn.commit()
                        log_activity(
                            'admin_user_blocked',
                            details=f'Пользователь {user_id} заблокирован',
                            metadata={
                                'target_user_id': user_id,
                                'blocked_reason': blocked_reason,
                                'blocked_by': blocked_by
                            }
                        )
                        flash('Пользователь успешно заблокирован', 'success')
                    except Exception as e:
                        log_error(f"Error blocking user: {e}")
                        flash(f'Ошибка блокировки пользователя: {str(e)}', 'error')
            elif block_action == 'unblock':
                try:
                    conn.execute('''
                        UPDATE users SET
                            is_blocked = 0,
                            blocked_by = NULL,
                            blocked_reason = NULL,
                            blocked_at = NULL
                        WHERE user_id = ?
                    ''', (user_id,))
                    conn.commit()
                    log_activity(
                        'admin_user_unblocked',
                        details=f'Пользователь {user_id} разблокирован',
                        metadata={'target_user_id': user_id}
                    )
                    flash('Пользователь успешно разблокирован', 'success')
                except Exception as e:
                    log_error(f"Error unblocking user: {e}")
                    flash(f'Ошибка разблокировки пользователя: {str(e)}', 'error')
        
        # Обновление основных данных пользователя
        username = request.form.get('username', '').strip()
        level = request.form.get('level', '0')
        synd = request.form.get('synd', '0')
        has_passport = request.form.get('has_passport', '0')
        has_mobile = request.form.get('has_mobile', '0')
        usersex = request.form.get('usersex', '0')
        bio = request.form.get('bio', '').strip()
        contact_info = request.form.get('contact_info', '').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        telegram = request.form.get('telegram', '').strip()
        whatsapp = request.form.get('whatsapp', '').strip()
        viber = request.form.get('viber', '').strip()
        last_name = request.form.get('last_name', '').strip()
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        postal_code = request.form.get('postal_code', '').strip()
        country = request.form.get('country', '').strip()
        city = request.form.get('city', '').strip()
        street = request.form.get('street', '').strip()
        house = request.form.get('house', '').strip()
        building = request.form.get('building', '').strip()
        apartment = request.form.get('apartment', '').strip()
        language = request.form.get('language', (user['language'] or 'ru')).strip()
        avatar_seed = request.form.get('avatar_seed', '').strip()
        avatar_style = request.form.get('avatar_style', '').strip()
        old_passport = request.form.get('old_passport', str(user['old_passport'] or 0))
        
        if not username:
            flash('Имя пользователя обязательно', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
            # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
            blocker_info = None
            user_keys = user.keys()
            if 'blocked_by' in user_keys and user['blocked_by']:
                blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
                if blocker:
                    blocker_info = dict(blocker)
            conn.close()
            user_roles = get_user_roles(user_id)
            user_role_names = [r['name'] for r in user_roles]
            all_titles = get_all_titles()
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            return render_template('admin/user_form.html', 
                                 user=dict(user),
                                 all_roles=all_roles,
                                 user_roles=user_roles,
                                 user_role_names=user_role_names,
                                 all_titles=all_titles,
                                 user_titles=user_titles,
                                 user_title_ids=user_title_ids,
                                 avatar_styles=AVATAR_STYLES,
                                 available_languages=available_languages,
                                 blocker_info=blocker_info)
        
        try:
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = int(has_passport)
            has_mobile_int = int(has_mobile)
            old_passport_int = int(old_passport)
        except ValueError:
            flash('Неверный формат числовых полей', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
            # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
            blocker_info = None
            user_keys = user.keys()
            if 'blocked_by' in user_keys and user['blocked_by']:
                blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
                if blocker:
                    blocker_info = dict(blocker)
            conn.close()
            user_roles = get_user_roles(user_id)
            user_role_names = [r['name'] for r in user_roles]
            all_titles = get_all_titles()
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            return render_template('admin/user_form.html', 
                                 user=dict(user),
                                 all_roles=all_roles,
                                 user_roles=user_roles,
                                 user_role_names=user_role_names,
                                 all_titles=all_titles,
                                 user_titles=user_titles,
                                 user_title_ids=user_title_ids,
                                 avatar_styles=AVATAR_STYLES,
                                 available_languages=available_languages,
                                 blocker_info=blocker_info)
        
        try:
            if language not in available_languages:
                language = 'ru'
            if not avatar_seed:
                avatar_seed = user['avatar_seed']
            if not avatar_style or avatar_style not in AVATAR_STYLES:
                avatar_style = user['avatar_style'] or 'avataaars'
            conn.execute('''
                UPDATE users SET
                    username = ?, level = ?, synd = ?, has_passport = ?, has_mobile = ?, old_passport = ?,
                    usersex = ?, bio = ?, contact_info = ?,
                    email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?,
                    last_name = ?, first_name = ?, middle_name = ?,
                    postal_code = ?, country = ?, city = ?, street = ?, house = ?, building = ?, apartment = ?,
                    avatar_seed = ?, avatar_style = ?, language = ?
                WHERE user_id = ?
            ''', (username, level_int, synd_int, has_passport_int, has_mobile_int, old_passport_int,
                  usersex, bio, contact_info, email, phone, 
                  telegram, whatsapp, viber,
                  last_name, first_name, middle_name,
                  postal_code, country, city, street, house, building, apartment,
                  avatar_seed, avatar_style, language,
                  user_id))
            conn.commit()
            log_activity(
                'admin_user_update',
                details=f'Обновлены данные пользователя {user_id}',
                metadata={'target_user_id': user_id, 'username': username}
            )
            if not role_action and not title_action:
                flash('Пользователь успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('admin_user_edit', user_id=user_id))
        except Exception as e:
            log_error(f"Error updating user: {e}")
            flash(f'Ошибка обновления пользователя: {str(e)}', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
            # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
            blocker_info = None
            user_keys = user.keys()
            if 'blocked_by' in user_keys and user['blocked_by']:
                blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
                if blocker:
                    blocker_info = dict(blocker)
            conn.close()
            user_roles = get_user_roles(user_id)
            user_role_names = [r['name'] for r in user_roles]
            all_titles = get_all_titles()
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            return render_template('admin/user_form.html', 
                                 user=dict(user),
                                 all_roles=all_roles,
                                 user_roles=user_roles,
                                 user_role_names=user_role_names,
                                 all_titles=all_titles,
                                 user_titles=user_titles,
                                 user_title_ids=user_title_ids,
                                 avatar_styles=AVATAR_STYLES,
                                 available_languages=available_languages,
                                 blocker_info=blocker_info)
    
    # GET запрос - получаем данные для отображения
    all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    user_roles = get_user_roles(user_id)
    user_role_names = [r['name'] for r in user_roles]
    all_titles = get_all_titles()
    user_titles = get_user_titles(user_id)
    user_title_ids = [t['id'] for t in user_titles]
    
    # Получаем информацию о заблокировавшем пользователе ДО закрытия соединения
    blocker_info = None
    user_keys = user.keys()
    if 'blocked_by' in user_keys and user['blocked_by']:
        blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
        if blocker:
            blocker_info = dict(blocker)
    
    conn.close()
    return render_template('admin/user_form.html', 
                         user=dict(user),
                         all_roles=all_roles,
                         user_roles=user_roles,
                         user_role_names=user_role_names,
                         all_titles=all_titles,
                         user_titles=user_titles,
                         user_title_ids=user_title_ids,
                         avatar_styles=AVATAR_STYLES,
                         available_languages=available_languages,
                         blocker_info=blocker_info)

@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@require_role('admin')
def admin_user_delete(user_id):
    """Удаление пользователя администратором"""
    if session.get('user_id') == user_id:
        flash('Нельзя удалить собственную учетную запись', 'error')
        return redirect(url_for('admin_users'))

    conn = get_db_connection()
    try:
        user = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user_id,)).fetchone()
        if not user:
            conn.close()
            flash('Пользователь не найден', 'error')
            return redirect(url_for('admin_users'))

        username = user['username']

        conn.execute('BEGIN')
        conn.execute('UPDATE user_roles SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE user_titles SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE user_awards SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE awards SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE events SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE activity_logs SET user_id = NULL WHERE user_id = ?', (user_id,))
        conn.execute('UPDATE event_participant_approvals SET approved_by = NULL WHERE approved_by = ?', (user_id,))
        conn.execute('UPDATE event_assignments SET assigned_by = NULL WHERE assigned_by = ?', (user_id,))
        conn.execute('UPDATE faq_categories SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE faq_categories SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('UPDATE contacts SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE contacts SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('UPDATE faq_items SET created_by = NULL WHERE created_by = ?', (user_id,))
        conn.execute('UPDATE faq_items SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('UPDATE settings SET updated_by = NULL WHERE updated_by = ?', (user_id,))
        conn.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        conn.close()
        log_error(f"Ошибка удаления пользователя {user_id}: {e}")
        flash('Ошибка при удалении пользователя', 'error')
        return redirect(url_for('admin_users'))

    conn.close()

    log_activity(
        'admin_user_delete',
        details=f'Удален пользователь {username} (ID {user_id})',
        metadata={'target_user_id': user_id, 'target_username': username}
    )
    flash('Пользователь успешно удален', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/users/<int:user_id>/roles', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_roles(user_id):
    """Управление ролями пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        return redirect(url_for('admin_users'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        role_name = request.form.get('role_name')
        
        if action == 'assign':
            if assign_role(user_id, role_name, assigned_by=session['user_id']):
                flash(f'Роль "{role_name}" успешно назначена', 'success')
            else:
                flash(f'Ошибка назначения роли', 'error')
        elif action == 'remove':
            if remove_role(user_id, role_name):
                flash(f'Роль "{role_name}" успешно удалена', 'success')
            else:
                flash(f'Ошибка удаления роли', 'error')
    
    # Получаем все роли
    all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    
    # Получаем роли пользователя
    user_roles = get_user_roles(user_id)
    user_role_names = [r['name'] for r in user_roles]
    
    conn.close()
    
    return render_template('admin/user_roles.html', 
                         user=user, 
                         all_roles=all_roles, 
                         user_roles=user_roles,
                         user_role_names=user_role_names)

@app.route('/admin/roles')
@require_role('admin')
def admin_roles():
    """Редирект на вкладку ролей в управлении пользователями"""
    return redirect(url_for('admin_users') + '#roles')
@app.route('/admin/roles/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_role_create():
    """Создание новой роли"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        
        if not name or not display_name:
            flash('Имя и отображаемое имя роли обязательны', 'error')
            return render_template('admin/role_form.html')
        
        # Проверяем, что имя роли уникально
        conn = get_db_connection()
        existing = conn.execute('SELECT id FROM roles WHERE name = ?', (name,)).fetchone()
        if existing:
            flash('Роль с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/role_form.html')
        
        try:
            cursor = conn.execute('''
                INSERT INTO roles (name, display_name, description, is_system)
                VALUES (?, ?, ?, 0)
            ''', (name, display_name, description))
            role_id = cursor.lastrowid
            
            # Сохраняем выбранные права
            selected_permissions = request.form.getlist('permissions')
            for permission_id in selected_permissions:
                try:
                    permission_id_int = int(permission_id)
                    assign_permission_to_role(role_id, permission_id_int)
                except ValueError:
                    pass
            
            conn.commit()
            flash('Роль успешно создана', 'success')
            conn.close()
            return redirect(url_for('admin_roles'))
        except Exception as e:
            log_error(f"Error creating role: {e}")
            flash(f'Ошибка создания роли: {str(e)}', 'error')
            conn.close()
    
    # Получаем все права для отображения в форме
    permissions = get_all_permissions()
    return render_template('admin/role_form.html', permissions=permissions, role_permissions=[])

@app.route('/admin/roles/<int:role_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_role_edit(role_id):
    """Редактирование роли"""
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE id = ?', (role_id,)).fetchone()
    
    if not role:
        flash('Роль не найдена', 'error')
        conn.close()
        return redirect(url_for('admin_roles'))
    
    # Системные роли нельзя редактировать
    if role['is_system']:
        flash('Системные роли нельзя редактировать', 'error')
        conn.close()
        return redirect(url_for('admin_roles'))
    
    if request.method == 'POST':
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        
        if not display_name:
            flash('Отображаемое имя роли обязательно', 'error')
            permissions = get_all_permissions()
            conn.close()
            return render_template('admin/role_form.html', role=role, permissions=permissions)
        
        try:
            conn.execute('''
                UPDATE roles SET display_name = ?, description = ?
                WHERE id = ?
            ''', (display_name, description, role_id))
            
            # Обновляем права роли
            selected_permissions = request.form.getlist('permissions')
            selected_permission_ids = [int(pid) for pid in selected_permissions if pid.isdigit()]
            
            # Получаем текущие права роли
            current_permissions = get_role_permissions(role_id)
            current_permission_ids = [p['id'] for p in current_permissions]
            
            # Удаляем права, которые были сняты
            for perm_id in current_permission_ids:
                if perm_id not in selected_permission_ids:
                    remove_permission_from_role(role_id, perm_id)
            
            # Добавляем новые права
            for perm_id in selected_permission_ids:
                if perm_id not in current_permission_ids:
                    assign_permission_to_role(role_id, perm_id)
            
            conn.commit()
            flash('Роль успешно обновлена', 'success')
            conn.close()
            return redirect(url_for('admin_roles'))
        except Exception as e:
            log_error(f"Error updating role: {e}")
            flash(f'Ошибка обновления роли: {str(e)}', 'error')
            conn.close()
    
    permissions = get_all_permissions()
    # Получаем права текущей роли
    role_perms = get_role_permissions(role_id)
    role_permissions_list = [p['id'] for p in role_perms]
    conn.close()
    return render_template('admin/role_form.html', role=role, permissions=permissions, role_permissions=role_permissions_list)
@app.route('/admin/roles/<int:role_id>/delete', methods=['POST'])
@require_role('admin')
def admin_role_delete(role_id):
    """Удаление роли"""
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE id = ?', (role_id,)).fetchone()
    
    if not role:
        flash('Роль не найдена', 'error')
        conn.close()
        return redirect(url_for('admin_roles'))
    
    # Системные роли нельзя удалять
    if role['is_system']:
        flash('Системные роли нельзя удалять', 'error')
        conn.close()
        return redirect(url_for('admin_roles'))
    
    try:
        conn.execute('DELETE FROM roles WHERE id = ?', (role_id,))
        conn.commit()
        flash('Роль успешно удалена', 'success')
    except Exception as e:
        log_error(f"Error deleting role: {e}")
        flash(f'Ошибка удаления роли: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin_roles'))

# ========== Управление званиями ==========

@app.route('/admin/titles')
@require_role('admin')
def admin_titles():
    """Управление званиями"""
    conn = get_db_connection()
    titles = conn.execute('SELECT * FROM titles ORDER BY is_system DESC, display_name').fetchall()
    
    # Для каждого звания получаем количество пользователей
    titles_with_counts = []
    for title in titles:
        count = conn.execute('''
            SELECT COUNT(*) as count FROM user_titles WHERE title_id = ?
        ''', (title['id'],)).fetchone()
        titles_with_counts.append({
            **dict(title),
            'user_count': count['count']
        })
    
    conn.close()
    
    return render_template('admin/titles.html', titles=titles_with_counts)

@app.route('/admin/titles/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_title_create():
    """Создание нового звания"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#007bff').strip()
        icon = request.form.get('icon', '').strip()
        
        if not name or not display_name:
            flash('Имя и отображаемое имя звания обязательны', 'error')
            return render_template('admin/title_form.html')
        
        # Проверяем, что имя звания уникально
        conn = get_db_connection()
        existing = conn.execute('SELECT id FROM titles WHERE name = ?', (name,)).fetchone()
        if existing:
            flash('Звание с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/title_form.html')
        
        try:
            conn.execute('''
                INSERT INTO titles (name, display_name, description, color, icon, is_system)
                VALUES (?, ?, ?, ?, ?, 0)
            ''', (name, display_name, description, color, icon))
            conn.commit()
            flash('Звание успешно создано', 'success')
            conn.close()
            return redirect(url_for('admin_titles'))
        except Exception as e:
            log_error(f"Error creating title: {e}")
            flash(f'Ошибка создания звания: {str(e)}', 'error')
            conn.close()
    
    return render_template('admin/title_form.html')
@app.route('/admin/titles/<int:title_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_title_edit(title_id):
    """Редактирование звания"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE id = ?', (title_id,)).fetchone()
    
    if not title:
        flash('Звание не найдено', 'error')
        conn.close()
        return redirect(url_for('admin_titles'))
    
    # Системные звания нельзя редактировать
    if title['is_system']:
        flash('Системные звания нельзя редактировать', 'error')
        conn.close()
        return redirect(url_for('admin_titles'))
    
    if request.method == 'POST':
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#007bff').strip()
        icon = request.form.get('icon', '').strip()
        
        if not display_name:
            flash('Отображаемое имя звания обязательно', 'error')
            conn.close()
            return render_template('admin/title_form.html', title=title)
        
        try:
            conn.execute('''
                UPDATE titles SET display_name = ?, description = ?, color = ?, icon = ?
                WHERE id = ?
            ''', (display_name, description, color, icon, title_id))
            conn.commit()
            flash('Звание успешно обновлено', 'success')
            conn.close()
            return redirect(url_for('admin_titles'))
        except Exception as e:
            log_error(f"Error updating title: {e}")
            flash(f'Ошибка обновления звания: {str(e)}', 'error')
            conn.close()
    
    conn.close()
    return render_template('admin/title_form.html', title=title)

@app.route('/admin/titles/<int:title_id>/delete', methods=['POST'])
@require_role('admin')
def admin_title_delete(title_id):
    """Удаление звания"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE id = ?', (title_id,)).fetchone()
    
    if not title:
        flash('Звание не найдено', 'error')
        conn.close()
        return redirect(url_for('admin_titles'))
    
    # Системные звания нельзя удалять
    if title['is_system']:
        flash('Системные звания нельзя удалять', 'error')
        conn.close()
        return redirect(url_for('admin_titles'))
    
    try:
        conn.execute('DELETE FROM titles WHERE id = ?', (title_id,))
        conn.commit()
        flash('Звание успешно удалено', 'success')
    except Exception as e:
        log_error(f"Error deleting title: {e}")
        flash(f'Ошибка удаления звания: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin_titles'))

@app.route('/admin/users/<int:user_id>/titles', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_titles(user_id):
    """Управление званиями пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('admin_users'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        title_id = request.form.get('title_id')
        
        if action == 'assign' and title_id:
            try:
                title_id_int = int(title_id)
                if assign_title(user_id, title_id_int, assigned_by=session['user_id']):
                    flash('Звание успешно назначено', 'success')
                else:
                    flash('Ошибка назначения звания', 'error')
            except ValueError:
                flash('Неверный ID звания', 'error')
        elif action == 'remove' and title_id:
            try:
                title_id_int = int(title_id)
                if remove_title(user_id, title_id_int):
                    flash('Звание успешно удалено', 'success')
                else:
                    flash('Ошибка удаления звания', 'error')
            except ValueError:
                flash('Неверный ID звания', 'error')
    
    # Получаем все звания
    all_titles = get_all_titles()
    
    # Получаем звания пользователя
    user_titles = get_user_titles(user_id)
    user_title_ids = [t['id'] for t in user_titles]
    
    conn.close()
    
    return render_template('admin/user_titles.html', 
                         user=user, 
                         all_titles=all_titles, 
                         user_titles=user_titles,
                         user_title_ids=user_title_ids)
@app.route('/admin/settings', methods=['GET', 'POST'])
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
                
                conn.execute('''
                    UPDATE settings 
                    SET value = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                    WHERE key = ?
                ''', (value, session.get('user_id'), key))
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
        return redirect(url_for('admin_settings'))
    
    # Получаем все настройки, сгруппированные по категориям
    settings = conn.execute('''
        SELECT * FROM settings 
        ORDER BY category, key
    ''').fetchall()
    
    # Группируем по категориям
    settings_by_category = {}
    # Создаем словарь для быстрого доступа к настройкам
    settings_dict = {}
    for setting in settings:
        setting_dict = dict(setting)
        category = setting['category'] or 'general'
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
                             bot_menu_items=bot_menu_items)
    except Exception as e:
        log_error(f"Error rendering admin/settings.html: {e}")
        import traceback
        log_error(traceback.format_exc())
        raise

def verify_dadata_api(api_key, secret_key):
    """Проверяет валидность Dadata API ключей"""
    if not requests:
        return False, "Библиотека requests не установлена. Установите: pip install requests"
    
    if not api_key or not secret_key:
        return False, "API ключ и Secret ключ обязательны"
    
    try:
        # Используем простой endpoint для проверки (например, версия API)
        headers = {
            'Authorization': f'Token {api_key}',
            'X-Secret': secret_key,
            'Content-Type': 'application/json'
        }
        
        # Проверяем через endpoint /v1/version (более легкий запрос)
        response = requests.get('https://dadata.ru/api/v1/version', headers=headers, timeout=5)
        
        if response.status_code == 200:
            return True, "Ключи успешно проверены"
        elif response.status_code == 401:
            return False, "Неверный API ключ или Secret ключ"
        elif response.status_code == 403:
            return False, "Доступ запрещен. Проверьте права доступа для ключей"
        else:
            return False, f"Ошибка проверки: {response.status_code} - {response.text[:100]}"
    except requests.exceptions.Timeout:
        return False, "Таймаут при подключении к Dadata API"
    except requests.exceptions.ConnectionError:
        return False, "Ошибка подключения к Dadata API. Проверьте интернет-соединение"
    except Exception as e:
        return False, f"Ошибка при проверке: {str(e)}"


@app.route('/admin/settings/verify-dadata', methods=['POST'])
@require_role('admin')
def verify_dadata():
    """Проверка Dadata API ключей"""
    api_key = request.form.get('api_key', '').strip()
    secret_key = request.form.get('secret_key', '').strip()
    
    if not api_key or not secret_key:
        return jsonify({'success': False, 'message': 'API ключ и Secret ключ обязательны'}), 400
    
    success, message = verify_dadata_api(api_key, secret_key)
    
    if success:
        # Сохраняем ключи и помечаем как проверенные
        conn = get_db_connection()
        try:
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (api_key, 'dadata_api_key'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (secret_key, 'dadata_secret_key'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1', 'dadata_verified'))
            conn.commit()
        except Exception as e:
            log_error(f"Error saving Dadata keys: {e}")
            conn.close()
            return jsonify({'success': False, 'message': f'Ошибка сохранения ключей: {str(e)}'}), 500
        conn.close()
    
    return jsonify({'success': success, 'message': message})

def verify_smtp_connection(host, port, username, password, use_tls=False, from_email=None):
    """Проверяет подключение к SMTP серверу"""
    import smtplib
    from email.mime.text import MIMEText
    
    if not host or not port or not username or not password:
        return False, "Все поля обязательны для заполнения"
    
    try:
        port_int = int(port)
        if port_int < 1 or port_int > 65535:
            return False, "Порт должен быть в диапазоне 1-65535"
    except ValueError:
        return False, "Порт должен быть числом"
    
    try:
        # Создаем подключение к SMTP серверу
        if use_tls:
            # Для TLS (порт 587)
            server = smtplib.SMTP(host, port_int, timeout=10)
            server.starttls()
        else:
            # Для SSL (порт 465) или без шифрования (порт 25)
            if port_int == 465:
                server = smtplib.SMTP_SSL(host, port_int, timeout=10)
            else:
                server = smtplib.SMTP(host, port_int, timeout=10)
        
        # Пытаемся авторизоваться
        server.login(username, password)
        
        # Если указан email отправителя, пытаемся отправить тестовое письмо
        if from_email:
            try:
                test_msg = MIMEText('Тестовое письмо для проверки SMTP подключения.')
                test_msg['Subject'] = 'Проверка SMTP - Анонимные Деды Морозы'
                test_msg['From'] = from_email
                test_msg['To'] = from_email  # Отправляем себе для проверки
                
                # Отправляем тестовое письмо
                server.sendmail(from_email, [from_email], test_msg.as_string())
                server.quit()
                return True, "Подключение успешно. Тестовое письмо отправлено на " + from_email
            except Exception as e:
                server.quit()
                return False, f"Подключение установлено, но не удалось отправить тестовое письмо: {str(e)}"
        else:
            server.quit()
            return True, "Подключение успешно установлено"
            
    except smtplib.SMTPAuthenticationError:
        return False, "Ошибка аутентификации. Проверьте логин и пароль"
    except smtplib.SMTPConnectError as e:
        return False, f"Ошибка подключения к серверу: {str(e)}"
    except smtplib.SMTPException as e:
        return False, f"Ошибка SMTP: {str(e)}"
    except Exception as e:
        return False, f"Ошибка при проверке: {str(e)}"

@app.route('/admin/settings/verify-smtp', methods=['POST'])
@require_role('admin')
def verify_smtp():
    """Проверка SMTP подключения"""
    host = request.form.get('host', '').strip()
    port = request.form.get('port', '').strip()
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()
    use_tls = request.form.get('use_tls', '0') == '1'
    from_email = request.form.get('from_email', '').strip()
    
    if not host or not port or not username or not password:
        return jsonify({'success': False, 'message': 'Все поля обязательны для заполнения'}), 400
    
    success, message = verify_smtp_connection(host, port, username, password, use_tls, from_email)
    
    if success:
        # Сохраняем настройки и помечаем как проверенные
        conn = get_db_connection()
        try:
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (host, 'smtp_host'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (port, 'smtp_port'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (username, 'smtp_username'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (password, 'smtp_password'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1' if use_tls else '0', 'smtp_use_tls'))
            if from_email:
                conn.execute('UPDATE settings SET value = ? WHERE key = ?', (from_email, 'smtp_from_email'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1', 'smtp_verified'))
            conn.commit()
        except Exception as e:
            log_error(f"Error saving SMTP settings: {e}")
            conn.close()
            return jsonify({'success': False, 'message': f'Ошибка сохранения настроек: {str(e)}'}), 500
        conn.close()
    
    return jsonify({'success': success, 'message': message})

def verify_telegram_bot(token, chat_id=None):
    """Проверяет подключение к Telegram боту"""
    if not requests:
        return False, "Библиотека requests не установлена. Установите: pip install requests"
    
    if not token:
        return False, "Токен бота обязателен"
    
    try:
        # Проверяем токен через getMe
        api_url = f'https://api.telegram.org/bot{token}/getMe'
        response = requests.get(api_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('ok'):
                bot_info = data.get('result', {})
                bot_username = bot_info.get('username', 'неизвестен')
                bot_name = bot_info.get('first_name', 'Бот')
                
                # Пытаемся настроить вебхук автоматически
                try:
                    base_url = get_base_url()
                    webhook_url = f"{base_url}/telegram/webhook"
                    webhook_api_url = f'https://api.telegram.org/bot{token}/setWebhook'
                    webhook_response = requests.post(webhook_api_url, json={
                        'url': webhook_url
                    }, timeout=10)
                    if webhook_response.status_code == 200:
                        webhook_result = webhook_response.json()
                        if webhook_result.get('ok'):
                            log_debug(f"Webhook set successfully: {webhook_url}")
                        else:
                            log_error(f"Failed to set webhook: {webhook_result.get('description')}")
                except Exception as e:
                    log_error(f"Error setting webhook: {e}")
                    # Не критично, продолжаем
                
                # Если указан chat_id, пытаемся отправить тестовое сообщение
                if chat_id:
                    try:
                        send_url = f'https://api.telegram.org/bot{token}/sendMessage'
                        send_data = {
                            'chat_id': chat_id,
                            'text': '✅ Тестовое сообщение от бота "Анонимные Деды Морозы". Интеграция работает!'
                        }
                        send_response = requests.post(send_url, json=send_data, timeout=10)
                        
                        if send_response.status_code == 200 and send_response.json().get('ok'):
                            return True, f"Бот '{bot_name}' (@{bot_username}) подключен. Тестовое сообщение отправлено в чат {chat_id}"
                        else:
                            error_data = send_response.json() if send_response.status_code == 200 else {}
                            error_desc = error_data.get('description', 'Неизвестная ошибка')
                            return False, f"Бот подключен, но не удалось отправить сообщение в чат {chat_id}: {error_desc}"
                    except requests.exceptions.RequestException as e:
                        return False, f"Бот подключен, но ошибка при отправке тестового сообщения: {str(e)}"
                else:
                    return True, f"Бот '{bot_name}' (@{bot_username}) успешно подключен. Chat ID не указан - можно отправлять сообщения по username"
            else:
                return False, "Неверный ответ от Telegram API"
        elif response.status_code == 401:
            return False, "Неверный токен бота. Проверьте токен от @BotFather"
        else:
            error_text = response.text[:200] if response.text else 'Неизвестная ошибка'
            return False, f"Ошибка проверки: {response.status_code} - {error_text}"
            
    except requests.exceptions.Timeout:
        return False, "Таймаут при подключении к Telegram API"
    except requests.exceptions.ConnectionError:
        return False, "Ошибка подключения к Telegram API. Проверьте интернет-соединение"
    except Exception as e:
        return False, f"Ошибка при проверке: {str(e)}"

@app.route('/admin/settings/verify-telegram', methods=['POST'])
@require_role('admin')
def verify_telegram():
    """Проверка Telegram бота"""
    token = request.form.get('token', '').strip()
    chat_id = request.form.get('chat_id', '').strip() or None
    
    if not token:
        return jsonify({'success': False, 'message': 'Токен бота обязателен'}), 400
    
    success, message = verify_telegram_bot(token, chat_id)
    
    if success:
        # Сохраняем настройки и помечаем как проверенные
        conn = get_db_connection()
        try:
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (token, 'telegram_bot_token'))
            if chat_id:
                conn.execute('UPDATE settings SET value = ? WHERE key = ?', (chat_id, 'telegram_chat_id'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1', 'telegram_verified'))
            conn.commit()
        except Exception as e:
            log_error(f"Error saving Telegram settings: {e}")
            conn.close()
            return jsonify({'success': False, 'message': f'Ошибка сохранения настроек: {str(e)}'}), 500
        conn.close()
    
    return jsonify({'success': success, 'message': message})

def send_telegram_message(message, chat_id=None, parse_mode=None):
    """Отправляет сообщение через Telegram бота
    
    Args:
        message: Текст сообщения
        chat_id: Chat ID или username (может начинаться с @) получателя. 
                 Если не указан, используется из настроек.
        parse_mode: Режим парсинга (HTML, Markdown и т.д.)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not requests:
        return False, "Библиотека requests не установлена"
    
    # Получаем настройки Telegram
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    if not telegram_enabled:
        return False, "Telegram бот не включен в настройках"
    
    telegram_verified = get_setting('telegram_verified', '0') == '1'
    if not telegram_verified:
        return False, "Telegram бот не проверен. Проверьте подключение в настройках"
    
    token = get_setting('telegram_bot_token', '')
    if not token:
        return False, "Токен бота не настроен"
    
    # Используем chat_id из параметра или из настроек
    target_chat_id = chat_id or get_setting('telegram_chat_id', '')
    if not target_chat_id:
        return False, "Chat ID не указан. Укажите chat_id в параметрах или настройках"
    
    try:
        api_url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = {
            'chat_id': target_chat_id,
            'text': message
        }
        if parse_mode:
            data['parse_mode'] = parse_mode
        
        response = requests.post(api_url, json=data, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                return True, "Сообщение успешно отправлено"
            else:
                error_desc = result.get('description', 'Неизвестная ошибка')
                return False, f"Ошибка отправки: {error_desc}"
        else:
            error_data = response.json() if response.status_code == 200 else {}
            error_desc = error_data.get('description', f'HTTP {response.status_code}')
            return False, f"Ошибка отправки: {error_desc}"
            
    except requests.exceptions.Timeout:
        return False, "Таймаут при отправке сообщения"
    except requests.exceptions.ConnectionError:
        return False, "Ошибка подключения к Telegram API"
    except Exception as e:
        log_error(f"Error sending Telegram message: {e}")
        return False, f"Ошибка при отправке: {str(e)}"

def send_telegram_message_with_keyboard(message, chat_id, keyboard=None, parse_mode=None):
    """Отправляет сообщение через Telegram бота с клавиатурой (меню)
    
    Args:
        message: Текст сообщения
        chat_id: Chat ID получателя
        keyboard: InlineKeyboardMarkup или ReplyKeyboardMarkup (dict)
        parse_mode: Режим парсинга (HTML, Markdown и т.д.)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not requests:
        return False, "Библиотека requests не установлена"
    
    token = get_setting('telegram_bot_token', '')
    if not token:
        return False, "Токен бота не настроен"
    
    try:
        api_url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = {
            'chat_id': chat_id,
            'text': message
        }
        if parse_mode:
            data['parse_mode'] = parse_mode
        if keyboard:
            data['reply_markup'] = keyboard
        
        response = requests.post(api_url, json=data, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                return True, "Сообщение успешно отправлено"
            else:
                error_desc = result.get('description', 'Неизвестная ошибка')
                return False, f"Ошибка отправки: {error_desc}"
        else:
            error_data = response.json() if response.status_code == 200 else {}
            error_desc = error_data.get('description', f'HTTP {response.status_code}')
            return False, f"Ошибка отправки: {error_desc}"
            
    except Exception as e:
        log_error(f"Error sending Telegram message with keyboard: {e}")
        return False, f"Ошибка при отправке: {str(e)}"

def generate_telegram_verification_code(user_id):
    """Генерирует код верификации для пользователя"""
    conn = get_db_connection()
    try:
        # Генерируем 6-значный код
        code = ''.join([str(random.randint(0, 9)) for _ in range(6)])
        expires_at = datetime.utcnow() + timedelta(minutes=10)  # Код действителен 10 минут
        
        # Сохраняем или обновляем код
        conn.execute('''
            INSERT INTO telegram_users (user_id, verification_code, verification_code_expires_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                verification_code = excluded.verification_code,
                verification_code_expires_at = excluded.verification_code_expires_at,
                verified = 0
        ''', (user_id, code, expires_at))
        conn.commit()
        return code
    except Exception as e:
        log_error(f"Error generating verification code: {e}")
        return None
    finally:
        conn.close()

def verify_telegram_code(user_id, code, telegram_chat_id, telegram_username=None):
    """Проверяет код верификации и связывает пользователя с Telegram"""
    conn = get_db_connection()
    try:
        telegram_user = conn.execute('''
            SELECT verification_code, verification_code_expires_at
            FROM telegram_users
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if not telegram_user:
            return False, "Код верификации не найден. Запросите новый код."
        
        stored_code = telegram_user['verification_code']
        expires_at_str = telegram_user['verification_code_expires_at']
        
        if not stored_code or stored_code != code:
            return False, "Неверный код верификации."
        
        # Проверяем срок действия
        if expires_at_str:
            expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
            if expires_at.tzinfo:
                expires_at = expires_at.replace(tzinfo=None)
            if datetime.utcnow() > expires_at:
                return False, "Код верификации истёк. Запросите новый код."
        
        # Связываем пользователя с Telegram
        conn.execute('''
            UPDATE telegram_users
            SET telegram_chat_id = ?,
                telegram_username = ?,
                verified = 1,
                verified_at = CURRENT_TIMESTAMP,
                verification_code = NULL,
                verification_code_expires_at = NULL
            WHERE user_id = ?
        ''', (telegram_chat_id, telegram_username, user_id))
        conn.commit()
        
        return True, "Telegram успешно привязан к вашему аккаунту!"
    except Exception as e:
        log_error(f"Error verifying Telegram code: {e}")
        return False, f"Ошибка при верификации: {str(e)}"
    finally:
        conn.close()

def get_telegram_bot_menu():
    """Получает активные пункты меню бота"""
    conn = get_db_connection()
    try:
        menu_items = conn.execute('''
            SELECT button_text, button_type, action
            FROM telegram_bot_menu
            WHERE is_active = 1
            ORDER BY sort_order ASC
        ''').fetchall()
        conn.close()
        return menu_items
    except Exception as e:
        log_error(f"Error getting bot menu: {e}")
        if conn:
            conn.close()
        return []

def send_email_via_smtp(to_email, subject, body, html_body=None):
    """Отправляет email через настроенный SMTP сервер"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    # Получаем настройки SMTP
    smtp_enabled = get_setting('smtp_enabled', '0') == '1'
    if not smtp_enabled:
        return False, "SMTP не включен в настройках"
    
    smtp_verified = get_setting('smtp_verified', '0') == '1'
    if not smtp_verified:
        return False, "SMTP не проверен. Проверьте подключение в настройках"
    
    smtp_host = get_setting('smtp_host', '')
    smtp_port = get_setting('smtp_port', '587')
    smtp_username = get_setting('smtp_username', '')
    smtp_password = get_setting('smtp_password', '')
    smtp_use_tls = get_setting('smtp_use_tls', '0') == '1'
    smtp_from_email = get_setting('smtp_from_email', '')
    smtp_from_name = get_setting('smtp_from_name', 'Анонимные Деды Морозы')
    
    if not smtp_host or not smtp_username or not smtp_password or not smtp_from_email:
        return False, "SMTP настройки неполные. Проверьте настройки в админ-панели"
    
    try:
        port_int = int(smtp_port)
        
        # Создаем сообщение
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{smtp_from_name} <{smtp_from_email}>"
        msg['To'] = to_email
        
        # Добавляем текстовую и HTML версию
        if html_body:
            part1 = MIMEText(body, 'plain', 'utf-8')
            part2 = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(part1)
            msg.attach(part2)
        else:
            part = MIMEText(body, 'plain', 'utf-8')
            msg.attach(part)
        
        # Подключаемся к SMTP серверу
        if smtp_use_tls:
            server = smtplib.SMTP(smtp_host, port_int, timeout=10)
            server.starttls()
        else:
            if port_int == 465:
                server = smtplib.SMTP_SSL(smtp_host, port_int, timeout=10)
            else:
                server = smtplib.SMTP(smtp_host, port_int, timeout=10)
        
        # Авторизуемся и отправляем
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_from_email, [to_email], msg.as_string())
        server.quit()
        
        return True, "Письмо успешно отправлено"
        
    except smtplib.SMTPAuthenticationError:
        return False, "Ошибка аутентификации SMTP. Проверьте логин и пароль"
    except smtplib.SMTPException as e:
        return False, f"Ошибка SMTP: {str(e)}"
    except Exception as e:
        log_error(f"Error sending email: {e}")
        return False, f"Ошибка при отправке письма: {str(e)}"

def init_default_modal_texts():
    """Инициализирует дефолтные тексты модальных окон для регистрации на мероприятия"""
    conn = get_db_connection()
    try:
        # Дефолтные тексты модальных окон
        default_modal_texts = {
            'modal_title': ('Заполнение обязательных данных', 'Заголовок модального окна регистрации'),
            'modal_intro_title_new': ('Для регистрации на мероприятие необходимо заполнить обязательную информацию.', 'Заголовок вступительного сообщения (новый пользователь)'),
            'modal_intro_text_new': ('Это необходимо для организации мероприятия и связи с участниками.', 'Текст вступительного сообщения (новый пользователь)'),
            'modal_intro_description_new': ('Мы поможем вам заполнить все необходимые данные пошагово.', 'Описание вступительного сообщения (новый пользователь)'),
            'modal_intro_title_existing': ('Ваши данные уже заполнены в системе.', 'Заголовок вступительного сообщения (существующий пользователь)'),
            'modal_intro_text_existing': ('Мы просто хотим убедиться, что все данные актуальны.', 'Текст вступительного сообщения (существующий пользователь)'),
            'modal_intro_description_existing': ('Пожалуйста, проверьте и подтвердите ваши данные.', 'Описание вступительного сообщения (существующий пользователь)'),
            'modal_step_personal_title': ('Шаг 1: Личные данные', 'Заголовок шага личных данных'),
            'modal_step_address_title': ('Шаг 2: Адрес', 'Заголовок шага адреса'),
            'modal_step_contact_title_prefix': ('Шаг', 'Префикс заголовка шага контактов'),
            'modal_step_contact_description_required': ('Для связи с вами необходимо указать хотя бы один способ связи.', 'Описание шага контактов (обязательное поле)'),
            'modal_step_contact_description_optional': ('Вы можете добавить еще один способ связи или пропустить этот шаг.', 'Описание шага контактов (необязательное поле)'),
            'modal_step_contact_description_review': ('Проверьте ваш контакт. Вы можете изменить его или подтвердить.', 'Описание шага контактов (проверка)'),
            'modal_final_title': ('🎉 Все данные собраны, вы готовы стать Анонимным Дедом Морозом!', 'Заголовок финального шага'),
            'modal_final_text': ('Теперь вы можете принять участие в мероприятии и подарить радость другим участникам!', 'Текст финального шага'),
            'modal_btn_back': ('Назад', 'Кнопка "Назад"'),
            'modal_btn_next': ('Далее', 'Кнопка "Далее"'),
            'modal_btn_skip': ('Пропустить', 'Кнопка "Пропустить"'),
            'modal_btn_not_using': ('Не использую', 'Кнопка "Не использую"'),
            'modal_btn_confirm': ('Без сомнений, участвую!', 'Кнопка подтверждения участия'),
            'modal_btn_cancel': ('Я ещё подумаю...', 'Кнопка отмены'),
            'modal_btn_save_continue': ('Сохранить и продолжить', 'Кнопка "Сохранить и продолжить"'),
            'modal_btn_finish_register': ('Завершить и зарегистрироваться', 'Кнопка "Завершить и зарегистрироваться"'),
            'modal_error_email_invalid': ('Это не email', 'Сообщение об ошибке: неверный email'),
        }
        
        # Проверяем, есть ли уже настройки модальных окон
        existing = conn.execute('SELECT key FROM settings WHERE category = ?', ('modals',)).fetchone()
        if existing:
            # Если уже есть настройки, не добавляем дефолтные
            conn.close()
            return
        
        # Получаем первого администратора для created_by
        admin_user = conn.execute('SELECT user_id FROM users WHERE user_id IN (SELECT user_id FROM user_roles WHERE role_id = (SELECT id FROM roles WHERE name = "admin")) LIMIT 1').fetchone()
        created_by = admin_user['user_id'] if admin_user else None
        
        # Добавляем настройки
        for key, (value, description) in default_modal_texts.items():
            set_setting(key, value, description, 'modals')
        
        conn.commit()
        log_debug("Default modal texts initialized")
    except Exception as e:
        log_error(f"Error initializing default modal texts: {e}")
    finally:
        conn.close()

def init_default_faq_items():
    """Инициализирует дефолтные FAQ элементы из статического контента"""
    conn = get_db_connection()
    try:
        # Проверяем, есть ли уже FAQ элементы
        existing_count = conn.execute('SELECT COUNT(*) as count FROM faq_items').fetchone()['count']
        if existing_count > 0:
            # Если уже есть элементы, не добавляем дефолтные
            conn.close()
            return
        
        # Дефолтные FAQ элементы
        default_faq_items = [
            # Общие вопросы
            ('Что такое "Анонимные Деды Морозы"?', 
             '<p>Это платформа для анонимного обмена подарками между участниками. Вы можете зарегистрироваться на мероприятие, получить случайного получателя подарка и отправить ему подарок, оставаясь инкогнито до праздника.</p>', 
             'general', 10),
            ('Как зарегистрироваться?', 
             '<p>Для регистрации необходимо войти через GWars. Нажмите на иконку GWars в правом верхнем углу или используйте кнопку "Войти через GWars" на главной странице. После авторизации ваш аккаунт будет автоматически создан в системе.</p>', 
             'general', 20),
            ('Могу ли я участвовать без авторизации через GWars?', 
             '<p>Нет, для участия в мероприятиях необходимо авторизоваться через GWars. Это обеспечивает безопасность и проверку личности участников.</p>', 
             'general', 30),
            
            # Мероприятия
            ('Как принять участие в мероприятии?', 
             '<p>Перейдите в раздел "Мероприятия" в меню, выберите интересующее вас мероприятие и нажмите "Зарегистрироваться". Регистрация доступна только в определенные периоды времени, указанные администратором.</p>', 
             'events', 10),
            ('Что такое этапы мероприятия?', 
             '<p>Каждое мероприятие состоит из нескольких этапов:</p><ul><li><strong>Предварительная регистрация</strong> - ранний этап для желающих участвовать</li><li><strong>Основная регистрация</strong> - основной период регистрации</li><li><strong>Закрытие регистрации</strong> - регистрация завершена</li><li><strong>Жеребьёвка</strong> - случайное распределение получателей подарков</li><li><strong>Отправка подарков</strong> - период отправки подарков</li><li><strong>Обмен подарками</strong> - день, когда подарки вручаются</li><li><strong>Мероприятие завершено</strong> - период для обмена впечатлениями</li></ul>', 
             'events', 20),
            ('Как узнать, кому я должен отправить подарок?', 
             '<p>После завершения жеребьёвки вы получите информацию о получателе подарка в личном кабинете. Вы узнаете, кому отправлять подарок, но ваше имя останется неизвестным получателю до праздника.</p>', 
             'events', 30),
            ('Можно ли отменить регистрацию на мероприятие?', 
             '<p>Отмена регистрации возможна только до начала этапа "Закрытие регистрации". После этого этапа отмена регистрации невозможна, так как начинается процесс жеребьёвки.</p>', 
             'events', 40),
            
            # Профиль и настройки
            ('Как изменить свой аватар?', 
             '<p>Перейдите в свой профиль (иконка в правом верхнем углу) и нажмите "Редактировать профиль". В разделе "Аватар" вы сможете выбрать новый аватар из библиотеки DiceBear. Каждый аватар уникален и не может быть повторен.</p>', 
             'profile', 10),
            ('Что такое звания и как их получить?', 
             '<p>Звания - это особые метки, которые администраторы могут присвоить пользователям за вклад в проект. Существуют звания: Автор идеи, Разработчик, Амбассадор, Дизайнер. Звания отображаются в вашем профиле.</p>', 
             'profile', 20),
            ('Можно ли изменить информацию из GWars?', 
             '<p>Нет, информация из GWars (имя, уровень, синдикат и т.д.) синхронизируется автоматически и не может быть изменена вручную. Вы можете редактировать только дополнительную информацию: биографию, контакты и аватар.</p>', 
             'profile', 30),
            
            # Технические вопросы
            ('Поддерживается ли мобильная версия?', 
             '<p>Да, сайт полностью адаптирован для мобильных устройств. Вы можете использовать меню, свайпая его влево-вправо для открытия/закрытия и вверх-вниз для прокрутки. Также доступна темная и светлая темы оформления.</p>', 
             'technical', 10),
            ('Как переключить тему оформления?', 
             '<p>Используйте иконку переключения темы в правом верхнем углу сайта. Выбранная тема сохраняется и будет использоваться при следующих посещениях.</p>', 
             'technical', 20),
            ('Что делать, если возникла ошибка при входе?', 
             '<p>Если при входе через GWars возникает ошибка, убедитесь, что вы авторизованы на сайте GWars.io. Если проблема сохраняется, обратитесь к администратору через контакты в вашем профиле или напишите в поддержку GWars.</p>', 
             'technical', 30),
            
            # Безопасность и конфиденциальность
            ('Безопасны ли мои данные?', 
             '<p>Да, мы используем безопасную авторизацию через GWars и не храним пароли пользователей. Вся информация передается через защищенные соединения (HTTPS). Ваши контактные данные видны только вам и администраторам системы.</p>', 
             'security', 10),
            ('Кто может видеть мой профиль?', 
             '<p>Ваш профиль доступен для просмотра всем авторизованным пользователям. Однако контактная информация (email, телефон, мессенджеры) видна только вам и администраторам. Остальные пользователи видят только публичную информацию: имя, уровень, синдикат, роли, звания и биографию.</p>', 
             'security', 20),
        ]
        
        # Получаем первого администратора для created_by
        admin_user = conn.execute('SELECT user_id FROM users WHERE user_id IN (SELECT user_id FROM user_roles WHERE role_id = (SELECT id FROM roles WHERE name = "admin")) LIMIT 1').fetchone()
        created_by = admin_user['user_id'] if admin_user else None
        
        # Добавляем FAQ элементы
        for question, answer, category, sort_order in default_faq_items:
            conn.execute('''
                INSERT INTO faq_items (question, answer, category, sort_order, is_active, created_by, created_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
            ''', (question, answer, category, sort_order, created_by, datetime.now()))
        
        conn.commit()
        log_debug("Default FAQ items initialized")
    except Exception as e:
        log_error(f"Error initializing default FAQ items: {e}")
    finally:
        conn.close()

@app.route('/admin/faq')
@require_role('admin')
def admin_faq():
    """Управление FAQ"""
    # Инициализируем дефолтные FAQ элементы, если их еще нет
    init_default_faq_items()
    
    conn = get_db_connection()
    faq_items = conn.execute('''
        SELECT f.*, 
               u1.username as creator_name,
               u2.username as updater_name
        FROM faq_items f
        LEFT JOIN users u1 ON f.created_by = u1.user_id
        LEFT JOIN users u2 ON f.updated_by = u2.user_id
        ORDER BY f.category, f.sort_order, f.id
    ''').fetchall()
    
    faq_categories = conn.execute('''
        SELECT c.*, 
               COUNT(f.id) as items_count,
               u1.username as creator_name,
               u2.username as updater_name
        FROM faq_categories c
        LEFT JOIN faq_items f ON c.name = f.category
        LEFT JOIN users u1 ON c.created_by = u1.user_id
        LEFT JOIN users u2 ON c.updated_by = u2.user_id
        GROUP BY c.id
        ORDER BY c.sort_order, c.display_name
    ''').fetchall()
    
    conn.close()
    
    return render_template('admin/faq.html', 
                         faq_items=faq_items, 
                         faq_categories=faq_categories)

@app.route('/admin/faq/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_create():
    """Создание нового FAQ вопроса"""
    categories = get_faq_categories()
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        answer = request.form.get('answer', '').strip()
        category = request.form.get('category', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not question or not answer:
            flash('Вопрос и ответ обязательны для заполнения', 'error')
            return render_template('admin/faq_form.html', categories=categories)
        
        if not category and categories:
            category = categories[0]['name']
        elif not category:
            category = 'general'
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = 100
            is_active = 1
        
        conn = get_db_connection()
        try:
            conn.execute('''
                INSERT INTO faq_items (question, answer, category, sort_order, is_active, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (question, answer, category, sort_order, is_active, session['user_id']))
            conn.commit()
            flash('FAQ вопрос успешно создан', 'success')
            conn.close()
            return redirect(url_for('admin_faq'))
        except Exception as e:
            log_error(f"Error creating FAQ: {e}")
            flash(f'Ошибка создания FAQ: {str(e)}', 'error')
            conn.close()
            return render_template('admin/faq_form.html', categories=categories)
    
    return render_template('admin/faq_form.html', categories=categories)
@app.route('/admin/faq/<int:faq_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_edit(faq_id):
    """Редактирование FAQ вопроса"""
    conn = get_db_connection()
    faq_item = conn.execute('SELECT * FROM faq_items WHERE id = ?', (faq_id,)).fetchone()
    
    if not faq_item:
        flash('FAQ вопрос не найден', 'error')
        conn.close()
        return redirect(url_for('admin_faq'))
    
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        answer = request.form.get('answer', '').strip()
        category = request.form.get('category', 'general').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not question or not answer:
            flash('Вопрос и ответ обязательны для заполнения', 'error')
            conn.close()
            return render_template('admin/faq_form.html', faq_item=faq_item)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = faq_item['sort_order'] if faq_item['sort_order'] is not None else 100
            is_active = faq_item['is_active']
        
        try:
            conn.execute('''
                UPDATE faq_items 
                SET question = ?, answer = ?, category = ?, sort_order = ?, is_active = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (question, answer, category, sort_order, is_active, session['user_id'], faq_id))
            conn.commit()
            flash('FAQ вопрос успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('admin_faq'))
        except Exception as e:
            log_error(f"Error updating FAQ: {e}")
            flash(f'Ошибка обновления FAQ: {str(e)}', 'error')
            conn.close()
    
    categories = get_faq_categories()
    conn.close()
    return render_template('admin/faq_form.html', faq_item=faq_item, categories=categories)

@app.route('/admin/faq/<int:faq_id>/delete', methods=['POST'])
@require_role('admin')
def admin_faq_delete(faq_id):
    """Удаление FAQ вопроса"""
    conn = get_db_connection()
    faq_item = conn.execute('SELECT * FROM faq_items WHERE id = ?', (faq_id,)).fetchone()
    
    if not faq_item:
        flash('FAQ вопрос не найден', 'error')
        conn.close()
        return redirect(url_for('admin_faq'))
    
    try:
        conn.execute('DELETE FROM faq_items WHERE id = ?', (faq_id,))
        conn.commit()
        flash('FAQ вопрос успешно удален', 'success')
    except Exception as e:
        log_error(f"Error deleting FAQ: {e}")
        flash(f'Ошибка удаления FAQ: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin_faq'))
@app.route('/admin/faq/categories/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_category_create():
    """Создание новой категории FAQ"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not name or not display_name:
            flash('Имя категории и отображаемое имя обязательны для заполнения', 'error')
            return render_template('admin/faq_category_form.html')
        
        # Проверяем уникальность имени
        conn = get_db_connection()
        existing = conn.execute('SELECT id FROM faq_categories WHERE name = ?', (name,)).fetchone()
        if existing:
            flash('Категория с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/faq_category_form.html')
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = 100
            is_active = 1
        
        try:
            conn.execute('''
                INSERT INTO faq_categories (name, display_name, description, sort_order, is_active, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (name, display_name, description, sort_order, is_active, session['user_id']))
            conn.commit()
            flash('Категория FAQ успешно создана', 'success')
            conn.close()
            return redirect(url_for('admin_faq') + '#categories')
        except Exception as e:
            log_error(f"Error creating FAQ category: {e}")
            flash(f'Ошибка создания категории: {str(e)}', 'error')
            conn.close()
    
    return render_template('admin/faq_category_form.html')
@app.route('/admin/faq/categories/<int:category_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_category_edit(category_id):
    """Редактирование категории FAQ"""
    conn = get_db_connection()
    category = conn.execute('SELECT * FROM faq_categories WHERE id = ?', (category_id,)).fetchone()
    
    if not category:
        flash('Категория не найдена', 'error')
        conn.close()
        return redirect(url_for('admin_faq') + '#categories')
    
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not name or not display_name:
            flash('Имя категории и отображаемое имя обязательны для заполнения', 'error')
            conn.close()
            return render_template('admin/faq_category_form.html', category=category)
        
        # Проверяем уникальность имени (исключая текущую категорию)
        existing = conn.execute('SELECT id FROM faq_categories WHERE name = ? AND id != ?', (name, category_id)).fetchone()
        if existing:
            flash('Категория с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/faq_category_form.html', category=category)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = category['sort_order'] if category['sort_order'] is not None else 100
            is_active = category['is_active']
        
        try:
            # Если имя категории изменилось, обновляем все FAQ элементы с этой категорией
            if name != category['name']:
                conn.execute('''
                    UPDATE faq_items 
                    SET category = ? 
                    WHERE category = ?
                ''', (name, category['name']))
            
            conn.execute('''
                UPDATE faq_categories 
                SET name = ?, display_name = ?, description = ?, sort_order = ?, is_active = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (name, display_name, description, sort_order, is_active, session['user_id'], category_id))
            conn.commit()
            flash('Категория FAQ успешно обновлена', 'success')
            conn.close()
            return redirect(url_for('admin_faq') + '#categories')
        except Exception as e:
            log_error(f"Error updating FAQ category: {e}")
            flash(f'Ошибка обновления категории: {str(e)}', 'error')
            conn.close()
    
    conn.close()
    return render_template('admin/faq_category_form.html', category=category)
@app.route('/admin/faq/categories/<int:category_id>/delete', methods=['POST'])
@require_role('admin')
def admin_faq_category_delete(category_id):
    """Удаление категории FAQ"""
    conn = get_db_connection()
    category = conn.execute('SELECT * FROM faq_categories WHERE id = ?', (category_id,)).fetchone()
    
    if not category:
        flash('Категория не найдена', 'error')
        conn.close()
        return redirect(url_for('admin_faq') + '#categories')
    
    # Проверяем, есть ли FAQ элементы с этой категорией
    items_count = conn.execute('SELECT COUNT(*) as count FROM faq_items WHERE category = ?', (category['name'],)).fetchone()
    
    if items_count['count'] > 0:
        flash(f'Нельзя удалить категорию, в которой есть вопросы ({items_count["count"]} шт.). Сначала переместите или удалите вопросы.', 'error')
        conn.close()
        return redirect(url_for('admin_faq') + '#categories')
    
    try:
        conn.execute('DELETE FROM faq_categories WHERE id = ?', (category_id,))
        conn.commit()
        flash('Категория FAQ успешно удалена', 'success')
    except Exception as e:
        log_error(f"Error deleting FAQ category: {e}")
        flash(f'Ошибка удаления категории: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin_faq') + '#categories')
def init_default_rules():
    """Инициализирует дефолтные правила для тестирования"""
    import json
    conn = get_db_connection()
    try:
        # Проверяем, есть ли уже правила с непустым содержимым
        existing = conn.execute('SELECT * FROM settings WHERE key = ?', ('rules_content',)).fetchone()
        if existing:
            # Проверяем, есть ли реальное содержимое (не пустая строка и не только пробелы)
            existing_value = existing.get('value', '').strip() if existing.get('value') else ''
            if existing_value:
                # Пытаемся распарсить как JSON, чтобы убедиться, что это валидные правила
                try:
                    parsed = json.loads(existing_value)
                    if isinstance(parsed, list) and len(parsed) > 0:
                        # Если это валидный JSON с правилами, не добавляем дефолтные
                        conn.close()
                        return
                except (json.JSONDecodeError, ValueError):
                    # Если не JSON, но есть содержимое - возможно старый HTML формат
                    # Не добавляем дефолтные, чтобы не перезаписать существующие
                    conn.close()
                    return
            # Если значение пустое, удаляем запись и создадим новую с дефолтными правилами
            conn.execute('DELETE FROM settings WHERE key = ?', ('rules_content',))
            conn.commit()
        
        # Дефолтные правила для тестирования
        default_rules = [
            {'point': '1', 'text': 'Все участники должны быть зарегистрированы через GWars и иметь активный аккаунт в игре.'},
            {'point': '1.1', 'text': 'При регистрации на мероприятие необходимо заполнить все обязательные поля профиля.'},
            {'point': '1.1.1', 'text': 'Обязательные поля включают: фамилию, имя, отчество, полный адрес и хотя бы один контакт для связи.'},
            {'point': '1.2', 'text': 'Администраторы могут участвовать в мероприятиях без заполнения обязательных полей профиля.'},
            {'point': '2', 'text': 'Регистрация на мероприятие возможна только в установленные администратором сроки.'},
            {'point': '2.1', 'text': 'Предварительная регистрация открывается для раннего участия.'},
            {'point': '2.2', 'text': 'Основная регистрация является основным периодом для записи на мероприятие.'},
            {'point': '2.3', 'text': 'После закрытия регистрации отмена участия невозможна.'},
            {'point': '3', 'text': 'Жеребьёвка проводится автоматически после закрытия регистрации.'},
            {'point': '3.1', 'text': 'Каждый участник получает случайного получателя подарка.'},
            {'point': '3.2', 'text': 'Информация о получателе становится доступна только после завершения жеребьёвки.'},
            {'point': '4', 'text': 'Подарки должны быть отправлены в установленные сроки.'},
            {'point': '4.1', 'text': 'Участник обязан отправить подарок своему получателю до даты праздника.'},
            {'point': '4.2', 'text': 'Администраторы могут отслеживать статус отправки подарков.'},
            {'point': '5', 'text': 'Конфиденциальность участников строго соблюдается.'},
            {'point': '5.1', 'text': 'Имя отправителя подарка остается неизвестным получателю до даты праздника.'},
            {'point': '5.2', 'text': 'Контактная информация участников видна только администраторам и самим участникам.'},
            {'point': '6', 'text': 'Нарушение правил может привести к исключению из мероприятия или системы.'},
            {'point': '6.1', 'text': 'Администраторы оставляют за собой право исключить участника за нарушение правил.'},
            {'point': '6.2', 'text': 'При исключении участника его получатель подарка будет переназначен другому участнику.'},
        ]
        
        # Сохраняем в JSON формате
        rules_json = json.dumps(default_rules, ensure_ascii=False, indent=2)
        
        # Получаем первого администратора для created_by
        admin_user = conn.execute('SELECT user_id FROM users WHERE user_id IN (SELECT user_id FROM user_roles WHERE role_id = (SELECT id FROM roles WHERE name = "admin")) LIMIT 1').fetchone()
        created_by = admin_user['user_id'] if admin_user else None
        
        # Сохраняем правила в настройках
        # Проверяем структуру таблицы settings
        table_info = conn.execute("PRAGMA table_info(settings)").fetchall()
        columns = [col[1] for col in table_info]
        
        # Формируем запрос в зависимости от наличия колонок
        if 'updated_at' in columns and 'updated_by' in columns:
            # Структура с updated_at и updated_by
            conn.execute('''
                INSERT INTO settings (key, value, category, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now(), created_by))
        elif 'updated_at' in columns:
            # Структура только с updated_at
            conn.execute('''
                INSERT INTO settings (key, value, category, updated_at)
                VALUES (?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now()))
        else:
            # Минимальная структура
            conn.execute('''
                INSERT INTO settings (key, value, category)
                VALUES (?, ?, ?)
            ''', ('rules_content', rules_json, 'general'))
        
        conn.commit()
        log_debug("Default rules initialized successfully")
        log_debug(f"Rules JSON length: {len(rules_json)}")
    except Exception as e:
        log_error(f"Error initializing default rules: {e}")
        log_error(traceback.format_exc())
    finally:
        conn.close()
@app.route('/admin/rules/init-defaults', methods=['POST'])
@require_role('admin')
def admin_rules_init_defaults():
    """Принудительная инициализация дефолтных правил"""
    try:
        import json
        conn = get_db_connection()
        user_id = session.get('user_id')
        
        # Дефолтные правила для тестирования
        default_rules = [
            {'point': '1', 'text': 'Все участники должны быть зарегистрированы через GWars и иметь активный аккаунт в игре.'},
            {'point': '1.1', 'text': 'При регистрации на мероприятие необходимо заполнить все обязательные поля профиля.'},
            {'point': '1.1.1', 'text': 'Обязательные поля включают: фамилию, имя, отчество, полный адрес и хотя бы один контакт для связи.'},
            {'point': '1.2', 'text': 'Администраторы могут участвовать в мероприятиях без заполнения обязательных полей профиля.'},
            {'point': '2', 'text': 'Регистрация на мероприятие возможна только в установленные администратором сроки.'},
            {'point': '2.1', 'text': 'Предварительная регистрация открывается для раннего участия.'},
            {'point': '2.2', 'text': 'Основная регистрация является основным периодом для записи на мероприятие.'},
            {'point': '2.3', 'text': 'После закрытия регистрации отмена участия невозможна.'},
            {'point': '3', 'text': 'Жеребьёвка проводится автоматически после закрытия регистрации.'},
            {'point': '3.1', 'text': 'Каждый участник получает случайного получателя подарка.'},
            {'point': '3.2', 'text': 'Информация о получателе становится доступна только после завершения жеребьёвки.'},
            {'point': '4', 'text': 'Подарки должны быть отправлены в установленные сроки.'},
            {'point': '4.1', 'text': 'Участник обязан отправить подарок своему получателю до даты праздника.'},
            {'point': '4.2', 'text': 'Администраторы могут отслеживать статус отправки подарков.'},
            {'point': '5', 'text': 'Конфиденциальность участников строго соблюдается.'},
            {'point': '5.1', 'text': 'Имя отправителя подарка остается неизвестным получателю до даты праздника.'},
            {'point': '5.2', 'text': 'Контактная информация участников видна только администраторам и самим участникам.'},
            {'point': '6', 'text': 'Нарушение правил может привести к исключению из мероприятия или системы.'},
            {'point': '6.1', 'text': 'Администраторы оставляют за собой право исключить участника за нарушение правил.'},
            {'point': '6.2', 'text': 'При исключении участника его получатель подарка будет переназначен другому участнику.'},
        ]
        
        # Сохраняем в JSON формате
        rules_json = json.dumps(default_rules, ensure_ascii=False, indent=2)
        
        # Удаляем существующую запись, если есть
        conn.execute('DELETE FROM settings WHERE key = ?', ('rules_content',))
        
        # Проверяем структуру таблицы settings
        table_info = conn.execute("PRAGMA table_info(settings)").fetchall()
        columns = [col[1] for col in table_info]
        
        # Формируем запрос в зависимости от наличия колонок
        if 'created_at' in columns and 'created_by' in columns and 'updated_at' in columns and 'updated_by' in columns:
            # Полная структура с датами и пользователями
            conn.execute('''
                INSERT INTO settings (key, value, category, created_at, created_by, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now(), user_id, datetime.now(), user_id))
        elif 'updated_at' in columns:
            # Структура с updated_at
            conn.execute('''
                INSERT INTO settings (key, value, category, updated_at)
                VALUES (?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now()))
        else:
            # Минимальная структура
            conn.execute('''
                INSERT INTO settings (key, value, category)
                VALUES (?, ?, ?)
            ''', ('rules_content', rules_json, 'general'))
        
        conn.commit()
        conn.close()
        
        flash('Дефолтные правила успешно инициализированы', 'success')
        log_debug("Default rules initialized via admin panel")
    except Exception as e:
        log_error(f"Error initializing default rules: {e}")
        log_error(traceback.format_exc())
        flash(f'Ошибка инициализации правил: {str(e)}', 'error')
    
    return redirect(url_for('admin_rules'))

@app.route('/admin/rules')
@require_role('admin')
def admin_rules():
    """Управление правилами"""
    try:
        import json
        rules_content = get_setting('rules_content', '')
        rules_items = []
        has_rules = False
        
        if rules_content:
            try:
                # Пытаемся распарсить как JSON
                rules_items = json.loads(rules_content)
                if isinstance(rules_items, list) and len(rules_items) > 0:
                    has_rules = True
                else:
                    rules_items = []
            except (json.JSONDecodeError, ValueError):
                # Старый формат HTML - оставляем как есть для обратной совместимости
                if rules_content.strip():
                    has_rules = True
        
        return render_template('admin/rules.html', rules_content=rules_content, rules_items=rules_items, has_rules=has_rules)
    except Exception as e:
        log_error(f"Error in admin_rules route: {e}")
        flash(f'Ошибка загрузки правил: {str(e)}', 'error')
        return render_template('admin/rules.html', rules_content='', rules_items=[], has_rules=False)

@app.route('/admin/rules/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_rules_edit():
    """Редактирование правил"""
    try:
        if request.method == 'POST':
            import json
            
            # Получаем данные из формы
            rule_points = request.form.getlist('rule_point[]')
            rule_texts = request.form.getlist('rule_text[]')
            
            # Формируем список правил
            rules_items = []
            for point, text in zip(rule_points, rule_texts):
                point = point.strip()
                text = text.strip()
                if point and text:  # Сохраняем только заполненные строки
                    rules_items.append({
                        'point': point,
                        'text': text
                    })
            
            # Сортируем по пунктам (1, 1.1, 1.1.1 и т.д.)
            def sort_key(item):
                parts = item['point'].split('.')
                return tuple(int(p) for p in parts)
            
            rules_items.sort(key=sort_key)
            
            # Сохраняем в JSON формате
            rules_json = json.dumps(rules_items, ensure_ascii=False, indent=2)
            
            # Сохраняем правила в настройках
            conn = get_db_connection()
            user_id = session.get('user_id')
            
            try:
                # Проверяем, существует ли настройка
                existing = conn.execute('SELECT * FROM settings WHERE key = ?', ('rules_content',)).fetchone()
                
                if existing:
                    # Обновляем существующую настройку
                    conn.execute('''
                        UPDATE settings 
                        SET value = ?, updated_at = ?, updated_by = ?
                        WHERE key = ?
                    ''', (rules_json, datetime.now(), user_id, 'rules_content'))
                else:
                    # Создаем новую настройку
                    conn.execute('''
                        INSERT INTO settings (key, value, category, created_at, created_by, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', ('rules_content', rules_json, 'general', datetime.now(), user_id, datetime.now(), user_id))
                
                conn.commit()
                flash('Правила успешно сохранены', 'success')
            except Exception as e:
                log_error(f"Error saving rules: {e}")
                flash(f'Ошибка сохранения правил: {str(e)}', 'error')
            finally:
                conn.close()
            
            return redirect(url_for('admin_rules'))
        
        # GET запрос - получаем существующие правила
        rules_content = get_setting('rules_content', '')
        rules_items = []
        
        if rules_content:
            try:
                import json
                # Пытаемся распарсить как JSON
                rules_items = json.loads(rules_content)
                if not isinstance(rules_items, list):
                    rules_items = []
            except (json.JSONDecodeError, ValueError):
                # Если не JSON, значит старый формат - конвертируем в новый
                # Для старых данных можно оставить пустым или попытаться распарсить HTML
                rules_items = []
        
        return render_template('admin/rules_edit.html', rules_items=rules_items)
    except Exception as e:
        log_error(f"Error in admin_rules_edit route: {e}")
        flash(f'Ошибка: {str(e)}', 'error')
        return redirect(url_for('admin_rules'))

def get_faq_categories():
    """Получает список активных категорий FAQ"""
    conn = get_db_connection()
    categories = conn.execute('''
        SELECT * FROM faq_categories 
        WHERE is_active = 1 
        ORDER BY sort_order, display_name
    ''').fetchall()
    conn.close()
    return [dict(c) for c in categories]

def get_setting(key, default=None):
    """Получает значение настройки из БД"""
    try:
        conn = get_db_connection()
        setting = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
        conn.close()
        return setting['value'] if setting and setting['value'] else default
    except Exception as e:
        log_error(f"Error getting setting {key}: {e}")
        return default

def set_setting(key, value, description=None, category='general'):
    """Устанавливает значение настройки"""
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR REPLACE INTO settings (key, value, description, category, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (key, value, description or '', category))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error setting {key}: {e}")
        conn.close()
        return False

# ============================================
# МЕРОПРИЯТИЯ
# ============================================

EVENT_STAGES = [
    {'type': 'pre_registration', 'name': 'Предварительная регистрация', 'required': False, 'has_start': True, 'has_end': False},
    {'type': 'main_registration', 'name': 'Основная регистрация', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'registration_closed', 'name': 'Закрытие регистрации', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'lottery', 'name': 'Жеребьёвка', 'required': False, 'has_start': False, 'has_end': False},
    {'type': 'celebration_date', 'name': 'Обмен подарками', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'after_party', 'name': 'Мероприятие завершено', 'required': True, 'has_start': False, 'has_end': True},
]

AVATAR_STYLES = ['avataaars', 'bottts', 'identicon', 'initials', 'micah']

def is_event_finished(event_id):
    """Проверяет, закончилось ли мероприятие полностью"""
    conn = get_db_connection()
    stage_rows = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()

    stages = [dict(row) for row in stage_rows]
    
    if not stages:
        return False
    
    now = get_event_now()
    
    # Мероприятие считается завершенным, если последний этап (after_party) имеет end_datetime и оно прошло
    after_party_stage = None
    for stage in stages:
        if stage['stage_type'] == 'after_party':
            after_party_stage = stage
            break
    
    if after_party_stage and after_party_stage['end_datetime']:
        try:
            end_dt = datetime.strptime(after_party_stage['end_datetime'], '%Y-%m-%d %H:%M:%S')
        except:
            try:
                end_dt = datetime.strptime(after_party_stage['end_datetime'], '%Y-%m-%dT%H:%M')
            except:
                return False
        
        return now > end_dt
    
    return False

def distribute_event_awards(event_id, require_sent=False):
    """Выдает награды участникам мероприятия.

    Если require_sent=True, награда выдается только Дедам Морозам, которые отметили отправку подарка.
    """
    conn = get_db_connection()
    
    # Проверяем, есть ли награда для мероприятия
    event = conn.execute('SELECT award_id FROM events WHERE id = ?', (event_id,)).fetchone()
    if not event or not event['award_id']:
        conn.close()
        return False
    
    award_id = event['award_id']
    
    if require_sent:
        participants = conn.execute('''
            SELECT DISTINCT santa_user_id AS user_id
            FROM event_assignments
            WHERE event_id = ?
              AND santa_user_id IS NOT NULL
              AND santa_sent_at IS NOT NULL
        ''', (event_id,)).fetchall()
    else:
        # Получаем всех участников мероприятия
        participants = conn.execute('''
            SELECT DISTINCT user_id FROM event_registrations WHERE event_id = ?
        ''', (event_id,)).fetchall()
    
    if not participants:
        conn.close()
        return False
    
    # Выдаем награду каждому участнику
    admin_user_id = session.get('user_id') or 1  # Используем текущего пользователя или системного
    awarded_count = 0
    
    for participant in participants:
        user_id = participant['user_id']
        try:
            # Проверяем, не выдана ли уже награда
            existing = conn.execute('''
                SELECT id FROM user_awards WHERE user_id = ? AND award_id = ?
            ''', (user_id, award_id)).fetchone()
            
            if not existing:
                conn.execute('''
                    INSERT INTO user_awards (user_id, award_id, assigned_by)
                    VALUES (?, ?, ?)
                ''', (user_id, award_id, admin_user_id))
                awarded_count += 1
        except sqlite3.IntegrityError:
            pass  # Награда уже выдана
        except Exception as e:
            log_error(f"Error awarding user {user_id} with award {award_id}: {e}")
    
    if awarded_count > 0:
        conn.commit()
        log_debug(f"Distributed {awarded_count} awards for event {event_id} (require_sent={require_sent})")
    
    conn.close()
    return awarded_count > 0
def get_current_event_stage(event_id):
    """Определяет текущий этап мероприятия на основе текущей даты"""
    conn = get_db_connection()
    stage_rows = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()

    stages = [dict(row) for row in stage_rows]
    for stage in stages:
        if (
            stage.get('stage_type') == 'after_party'
            and not stage.get('start_datetime')
            and stage.get('end_datetime')
        ):
            stage['start_datetime'] = stage['end_datetime']

    if not stages:
        return None
    
    now = get_event_now()
    
    # Проверяем, начался ли этап "Закрытие регистрации" - если да, создаем записи для ревью
    registration_closed_stage = None
    for stage in stages:
        if stage['stage_type'] == 'registration_closed' and stage['start_datetime']:
            try:
                start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
            except:
                try:
                    start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%dT%H:%M')
                except:
                    continue
            if now >= start_dt:
                registration_closed_stage = stage
                break
    
    # Если регистрация закрылась, создаем записи для ревью
    if registration_closed_stage:
        create_participant_approvals_for_event(event_id)
    
    # Создаем словарь этапов с их информацией
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    stages_info_dict = {stage['type']: stage for stage in EVENT_STAGES}
    
    # Ищем текущий этап
    current_stage = None
    
    for stage_info in EVENT_STAGES:
        stage_type = stage_info['type']
        if stage_type not in stages_dict:
            continue
        
        stage = dict(stages_dict[stage_type])
        
        # Проверяем, начался ли этап
        if stage['start_datetime']:
            try:
                start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
            except:
                try:
                    start_dt = datetime.strptime(stage['start_datetime'], '%Y-%m-%dT%H:%M')
                except:
                    log_debug(f"get_current_event_stage: cannot parse start_datetime for stage {stage_type}: {stage['start_datetime']}")
                    continue
            
            # Если этап еще не начался, пропускаем
            if now < start_dt:
                log_debug(f"get_current_event_stage: stage {stage_type} not started yet (start: {start_dt}, now: {now})")
                continue
        
        # Проверяем, закончился ли этап
        if stage['end_datetime']:
            try:
                end_dt = datetime.strptime(stage['end_datetime'], '%Y-%m-%d %H:%M:%S')
            except:
                try:
                    end_dt = datetime.strptime(stage['end_datetime'], '%Y-%m-%dT%H:%M')
                except:
                    end_dt = None
            if stage_type == 'after_party':
                end_dt = None

            if end_dt and now > end_dt:
                continue
        
        # Проверяем, не начался ли следующий этап (если следующий этап начался, текущий должен закончиться)
        # Это работает для всех этапов, не только для тех, у которых нет даты начала
        current_order = stage['stage_order']
        next_stage_started = False
        for next_stage in stages:
            if next_stage['stage_order'] > current_order and next_stage['start_datetime']:
                try:
                    next_start_dt = datetime.strptime(next_stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        next_start_dt = datetime.strptime(next_stage['start_datetime'], '%Y-%m-%dT%H:%M')
                    except:
                        continue
                if now >= next_start_dt:
                    next_stage_started = True
                    log_debug(f"get_current_event_stage: stage {stage_type} ended because next stage {next_stage['stage_type']} started at {next_start_dt}")
                    break
        
        if next_stage_started:
            continue
        
        # Этот этап активен
        current_stage = {
            'data': stage,
            'info': stage_info
        }
        log_debug(f"get_current_event_stage: found active stage {stage_type} for event {event_id}")
        break
    
    if not current_stage:
        log_debug(f"get_current_event_stage: no active stage found for event {event_id}")
    
    return current_stage

def get_event_gifts_statistics(event_id):
    """Получает статистику по подаркам для мероприятия"""
    conn = get_db_connection()
    try:
        # Всего назначений (сколько подарков должно быть отправлено)
        total_result = conn.execute('''
            SELECT COUNT(*) as count
            FROM event_assignments
            WHERE event_id = ?
        ''', (event_id,)).fetchone()
        total_assignments = total_result['count'] if total_result else 0
        
        # Отправлено, но не подтверждено получение
        # Учитываем только явно отмеченные (santa_sent_at) - нажатие кнопки "Отправил"
        # Сообщения не считаются признаком отправки подарка
        sent_not_received_result = conn.execute('''
            SELECT COUNT(DISTINCT ea.id) as count
            FROM event_assignments ea
            WHERE ea.event_id = ?
              AND (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '')
              AND (ea.recipient_received_at IS NULL OR ea.recipient_received_at = '')
        ''', (event_id,)).fetchone()
        sent_not_received = sent_not_received_result['count'] if sent_not_received_result else 0
        
        # Отправлено и подтверждено получение
        # Учитываем только явно отмеченные (santa_sent_at) - нажатие кнопки "Отправил"
        # Сообщения не считаются признаком отправки подарка
        sent_and_received_result = conn.execute('''
            SELECT COUNT(DISTINCT ea.id) as count
            FROM event_assignments ea
            WHERE ea.event_id = ?
              AND (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '')
              AND ea.recipient_received_at IS NOT NULL
              AND ea.recipient_received_at != ''
        ''', (event_id,)).fetchone()
        sent_and_received = sent_and_received_result['count'] if sent_and_received_result else 0
        
        # Не отправлено
        not_sent = total_assignments - sent_not_received - sent_and_received
        
        # Логирование для отладки
        log_debug(f"Event {event_id} gifts stats: total={total_assignments}, sent_not_received={sent_not_received}, sent_and_received={sent_and_received}, not_sent={not_sent}")
        
        return {
            'total': total_assignments,
            'sent_not_received': sent_not_received,
            'sent_and_received': sent_and_received,
            'not_sent': not_sent
        }
    except Exception as e:
        log_error(f"Error getting gifts statistics for event {event_id}: {e}")
        log_error(traceback.format_exc())
        return {
            'total': 0,
            'sent_not_received': 0,
            'sent_and_received': 0,
            'not_sent': 0
        }
    finally:
        conn.close()

def is_registration_open(event_id):
    """Проверяет, открыта ли регистрация на мероприятие"""
    current_stage = get_current_event_stage(event_id)
    if not current_stage:
        log_debug(f"is_registration_open: no current stage for event {event_id}")
        return False
    
    stage_type = current_stage['info']['type']
    # Регистрация открыта на этапах предварительной и основной регистрации
    is_open = stage_type in ['pre_registration', 'main_registration']
    log_debug(f"is_registration_open: event {event_id}, stage_type={stage_type}, is_open={is_open}")
    return is_open

def is_user_registered(event_id, user_id):
    """Проверяет, зарегистрирован ли пользователь на мероприятие"""
    if not user_id:
        return False
    conn = get_db_connection()
    registration = conn.execute('''
        SELECT id FROM event_registrations 
        WHERE event_id = ? AND user_id = ?
    ''', (event_id, user_id)).fetchone()
    conn.close()
    return registration is not None
def get_event_registrations_count(event_id):
    """Получает количество зарегистрированных пользователей на мероприятие"""
    conn = get_db_connection()
    count = conn.execute('''
        SELECT COUNT(*) as count FROM event_registrations 
        WHERE event_id = ?
    ''', (event_id,)).fetchone()
    conn.close()
    return count['count'] if count else 0
def get_event_registrations(event_id):
    """Получает список зарегистрированных пользователей на мероприятие"""
    conn = get_db_connection()
    registrations = conn.execute('''
        SELECT er.*, u.user_id, u.username, u.avatar_seed, u.avatar_style, u.level, u.synd
        FROM event_registrations er
        JOIN users u ON er.user_id = u.user_id
        WHERE er.event_id = ?
        ORDER BY er.registered_at ASC
    ''', (event_id,)).fetchall()
    conn.close()
    return registrations

def get_event_registrations_paginated(event_id, page=1, per_page=20):
    """Получает список зарегистрированных пользователей на мероприятие с пагинацией"""
    # Ограничиваем per_page разумными значениями
    per_page = min(max(per_page, 10), 100)
    
    conn = get_db_connection()
    
    # Подсчитываем общее количество
    total_count = conn.execute('''
        SELECT COUNT(*) as count 
        FROM event_registrations 
        WHERE event_id = ?
    ''', (event_id,)).fetchone()
    total_count = total_count['count'] if total_count else 0
    
    # Вычисляем offset
    offset = (page - 1) * per_page
    
    # Получаем участников с пагинацией
    registrations = conn.execute('''
        SELECT er.*, u.user_id, u.username, u.avatar_seed, u.avatar_style, u.level, u.synd
        FROM event_registrations er
        JOIN users u ON er.user_id = u.user_id
        WHERE er.event_id = ?
        ORDER BY er.registered_at ASC
        LIMIT ? OFFSET ?
    ''', (event_id, per_page, offset)).fetchall()
    conn.close()
    
    # Вычисляем данные для пагинации
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    has_prev = page > 1
    has_next = page < total_pages
    
    return {
        'registrations': registrations,
        'total_count': total_count,
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'has_prev': has_prev,
        'has_next': has_next
    }

def get_event_stages(event_id):
    """Возвращает список этапов мероприятия в порядке их следования"""
    conn = get_db_connection()
    try:
        stage_rows = conn.execute('''
            SELECT stage_type, stage_order, start_datetime, end_datetime
            FROM event_stages
            WHERE event_id = ?
            ORDER BY stage_order
        ''', (event_id,)).fetchall()
    finally:
        conn.close()
    stages = []
    for row in stage_rows:
        stage = dict(row)
        if (
            stage.get('stage_type') == 'after_party'
            and not stage.get('start_datetime')
            and stage.get('end_datetime')
        ):
            stage['start_datetime'] = stage['end_datetime']
        stages.append(stage)
    return stages
def create_participant_approvals_for_event(event_id):
    """Создает записи для ревью участников при закрытии регистрации"""
    conn = get_db_connection()
    try:
        # Получаем всех зарегистрированных участников
        registrations = conn.execute('''
            SELECT user_id FROM event_registrations WHERE event_id = ?
        ''', (event_id,)).fetchall()
        
        # Создаем записи для ревью (если их еще нет)
        for reg in registrations:
            conn.execute('''
                INSERT OR IGNORE INTO event_participant_approvals 
                (event_id, user_id, approved) 
                VALUES (?, ?, 0)
            ''', (event_id, reg['user_id']))
            _ensure_registration_bonus_event(conn, event_id, reg['user_id'])
        
        conn.commit()
        log_debug(f"Created participant approvals for event {event_id}")
    except Exception as e:
        log_error(f"Error creating participant approvals: {e}")
        conn.rollback()
    finally:
        conn.close()
def get_participants_for_review(event_id):
    """Получает список участников для ревью с полной информацией"""
    conn = get_db_connection()
    participants = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.last_name,
            u.first_name,
            u.middle_name,
            u.postal_code,
            u.country,
            u.city,
            u.street,
            u.house,
            u.building,
            u.apartment,
            u.email,
            u.phone,
            u.telegram,
            u.whatsapp,
            u.viber,
            epa.approved,
            epa.approved_at,
            epa.notes,
            epa.approved_by,
            er.registered_at
        FROM event_registrations er
        JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_participant_approvals epa ON er.event_id = epa.event_id AND er.user_id = epa.user_id
        WHERE er.event_id = ?
        ORDER BY er.registered_at ASC
    ''', (event_id,)).fetchall()
    conn.close()
    return participants

def approve_participant(event_id, user_id, approved_by, approved=True, notes=None):
    """Утверждает или отклоняет участника"""
    conn = get_db_connection()
    try:
        if approved:
            conn.execute('''
                UPDATE event_participant_approvals 
                SET approved = 1, approved_at = CURRENT_TIMESTAMP, approved_by = ?, notes = ?
                WHERE event_id = ? AND user_id = ?
            ''', (approved_by, notes, event_id, user_id))
        else:
            conn.execute('''
                UPDATE event_participant_approvals 
                SET approved = 0, approved_at = NULL, approved_by = ?, notes = ?
                WHERE event_id = ? AND user_id = ?
            ''', (approved_by, notes, event_id, user_id))
        conn.commit()
        return True
    except Exception as e:
        log_error(f"Error approving participant: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_approved_participants(event_id):
    """Получает список утвержденных участников"""
    conn = get_db_connection()
    participants = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.last_name,
            u.first_name,
            u.middle_name
        FROM event_participant_approvals epa
        JOIN users u ON epa.user_id = u.user_id
        WHERE epa.event_id = ? AND epa.approved = 1
        ORDER BY epa.approved_at ASC
    ''', (event_id,)).fetchall()
    conn.close()
    return [dict(row) for row in participants]
def create_random_assignments(event_id, assigned_by):
    """Создает случайное распределение Деда Мороза и Внучки"""
    conn = get_db_connection()
    try:
        # Получаем утвержденных участников
        participants = get_approved_participants(event_id)
        
        if len(participants) < 2:
            return False, "Недостаточно утвержденных участников (нужно минимум 2)"
        
        # Создаем список ID участников
        participant_ids = [p['user_id'] for p in participants]
        
        # Перемешиваем список
        random.shuffle(participant_ids)
        
        # Создаем циклическое распределение (каждый дарит следующему)
        assignments = []
        for i in range(len(participant_ids)):
            santa_id = participant_ids[i]
            recipient_id = participant_ids[(i + 1) % len(participant_ids)]  # Циклическое распределение
            assignments.append((santa_id, recipient_id))
        
        success, result = save_event_assignments(event_id, assignments, assigned_by, connection=conn)
        if success:
            return True, f"Создано {result} заданий"
        return False, result
    except Exception as e:
        log_error(f"Error creating random assignments: {e}")
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def save_event_assignments(event_id, assignments, assigned_by, locked_pairs=None, assignment_locked=False, connection=None):
    """Сохраняет распределение пар"""
    conn = connection or get_db_connection()
    try:
        existing_rows = conn.execute('''
            SELECT santa_user_id, recipient_user_id, locked, assignment_locked, santa_sent_at, santa_send_info, recipient_received_at, recipient_thanks_message, recipient_receipt_image, assigned_at, assigned_by
            FROM event_assignments
            WHERE event_id = ?
        ''', (event_id,)).fetchall()
        
        # Создаем словарь для быстрого поиска старых данных по паре (santa, recipient)
        existing_data_map = {}
        for row in existing_rows:
            key = (row['santa_user_id'], row['recipient_user_id'])
            existing_data_map[key] = {
                'locked': row['locked'] if 'locked' in row.keys() else 0,
                'assignment_locked': row['assignment_locked'] if 'assignment_locked' in row.keys() else 0,
                'santa_sent_at': row['santa_sent_at'] if 'santa_sent_at' in row.keys() else None,
                'santa_send_info': row['santa_send_info'] if 'santa_send_info' in row.keys() else None,
                'recipient_received_at': row['recipient_received_at'] if 'recipient_received_at' in row.keys() else None,
                'recipient_thanks_message': row['recipient_thanks_message'] if 'recipient_thanks_message' in row.keys() else None,
                'recipient_receipt_image': row['recipient_receipt_image'] if 'recipient_receipt_image' in row.keys() else None,
                'assigned_at': row['assigned_at'] if 'assigned_at' in row.keys() else None,
                'assigned_by': row['assigned_by'] if 'assigned_by' in row.keys() else None
            }
        
        # Получаем старые assignment_id для переноса сообщений
        old_assignments_map = {}
        old_assignments_rows = conn.execute('''
            SELECT id, santa_user_id, recipient_user_id
            FROM event_assignments
            WHERE event_id = ?
        ''', (event_id,)).fetchall()
        for row in old_assignments_rows:
            key = (row['santa_user_id'], row['recipient_user_id'])
            old_assignments_map[key] = row['id']
        
        conn.execute('DELETE FROM event_assignments WHERE event_id = ?', (event_id,))
        locked_map = {}
        if locked_pairs:
            for entry in locked_pairs:
                if isinstance(entry, dict):
                    santa = entry.get('santa_id')
                    recipient = entry.get('recipient_id')
                else:
                    try:
                        santa, recipient = entry
                    except Exception:
                        continue
                try:
                    santa = int(santa)
                    recipient = int(recipient)
                except (TypeError, ValueError):
                    continue
                locked_map[santa] = recipient

        data = []
        assignment_id_mapping = {}  # Старый ID -> Новый ID для переноса сообщений
        for santa, recipient in assignments:
            # Проверяем, есть ли старые данные для этой пары
            old_data = existing_data_map.get((santa, recipient), {})
            
            # Определяем locked статус
            locked_flag = 0
            if assignment_locked or locked_map.get(santa) == recipient:
                locked_flag = 1
            elif old_data.get('locked'):
                locked_flag = 1
            
            assignment_locked_flag = 1 if assignment_locked else (old_data.get('assignment_locked', 0))
            
            # Сохраняем старые данные о отправке/получении, если они есть
            santa_sent_at = old_data.get('santa_sent_at')
            santa_send_info = old_data.get('santa_send_info')
            recipient_received_at = old_data.get('recipient_received_at')
            recipient_thanks_message = old_data.get('recipient_thanks_message')
            recipient_receipt_image = old_data.get('recipient_receipt_image')
            assigned_at = old_data.get('assigned_at') or datetime.now().isoformat()
            assigned_by_final = old_data.get('assigned_by') or assigned_by
            
            data.append((
                event_id, santa, recipient, assigned_by_final, locked_flag, assignment_locked_flag,
                santa_sent_at, santa_send_info, recipient_received_at, recipient_thanks_message, recipient_receipt_image,
                assigned_at
            ))
            
            # Сохраняем маппинг старых assignment_id на пару (santa, recipient) для переноса сообщений
            old_key = (santa, recipient)
            if old_key in old_assignments_map:
                assignment_id_mapping[old_assignments_map[old_key]] = (santa, recipient)
        
        # Вставляем новые назначения по одному, чтобы получить их ID для переноса сообщений
        cursor = conn.cursor()
        new_assignments_id_map = {}  # (santa, recipient) -> новый assignment_id
        for assignment_data in data:
            cursor.execute('''
                INSERT INTO event_assignments (
                    event_id, santa_user_id, recipient_user_id, assigned_by, locked, assignment_locked,
                    santa_sent_at, santa_send_info, recipient_received_at, recipient_thanks_message, recipient_receipt_image,
                    assigned_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', assignment_data)
            new_assignment_id = cursor.lastrowid
            santa = assignment_data[1]
            recipient = assignment_data[2]
            new_assignments_id_map[(santa, recipient)] = new_assignment_id
        
        # Переносим сообщения на новые assignment_id
        if assignment_id_mapping:
            for old_id, pair_key in assignment_id_mapping.items():
                if pair_key in new_assignments_id_map:
                    new_id = new_assignments_id_map[pair_key]
                    conn.execute('''
                        UPDATE letter_messages
                        SET assignment_id = ?
                        WHERE assignment_id = ?
                    ''', (new_id, old_id))
                    log_debug(f"Transferred messages from old assignment_id {old_id} to new assignment_id {new_id} for pair {pair_key}")
        
        conn.commit()
        log_activity(
            'assignments_saved',
            details=f'Сохранено распределение для мероприятия #{event_id}',
            metadata={
                'event_id': event_id,
                'pairs_count': len(assignments),
                'assigned_by': assigned_by,
            },
            user_id=assigned_by
        )
        return True, len(assignments)
    except Exception as e:
        log_error(f"Error saving assignments for event {event_id}: {e}")
        conn.rollback()
        try:
            conn.execute('DELETE FROM event_assignments WHERE event_id = ?', (event_id,))
            conn.executemany('''
                INSERT INTO event_assignments (
                    event_id,
                    santa_user_id,
                    recipient_user_id,
                    locked,
                    assignment_locked,
                    santa_sent_at,
                    santa_send_info,
                    recipient_received_at,
                    recipient_thanks_message,
                    recipient_receipt_image,
                    assigned_at,
                    assigned_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (
                    event_id,
                    row['santa_user_id'],
                    row['recipient_user_id'],
                    row['locked'] if 'locked' in row.keys() else 0,
                    row['assignment_locked'] if 'assignment_locked' in row.keys() else 0,
                    row['santa_sent_at'] if 'santa_sent_at' in row.keys() else None,
                    row['santa_send_info'] if 'santa_send_info' in row.keys() else None,
                    row['recipient_received_at'] if 'recipient_received_at' in row.keys() else None,
                    row['recipient_thanks_message'] if 'recipient_thanks_message' in row.keys() else None,
                    row['recipient_receipt_image'] if 'recipient_receipt_image' in row.keys() else None,
                    row['assigned_at'] if 'assigned_at' in row.keys() else None,
                    row['assigned_by'] if 'assigned_by' in row.keys() else None
                )
                for row in existing_rows
            ])
            conn.commit()
        except Exception as restore_error:
            log_error(f"Failed to restore previous assignments for event {event_id}: {restore_error}")
        return False, str(e)

def get_user_assignments(user_id):
    """Получает задания пользователя (где он Дед Мороз и где Внучка)"""
    conn = get_db_connection()
    # Получаем задания, где пользователь Дед Мороз
    as_santa_rows = conn.execute('''
        SELECT 
            ea.*,
            e.name AS event_name,
            e.id AS event_id,
            recipient.username AS recipient_username,
            recipient.level AS recipient_level,
            recipient.synd AS recipient_synd,
            COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
            COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
            COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
            COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
            COALESCE(rd.country, recipient.country) AS recipient_country,
            COALESCE(rd.city, recipient.city) AS recipient_city,
            COALESCE(rd.street, recipient.street) AS recipient_street,
            COALESCE(rd.house, recipient.house) AS recipient_house,
            COALESCE(rd.building, recipient.building) AS recipient_building,
            COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
            COALESCE(rd.email, recipient.email) AS recipient_email,
            COALESCE(rd.phone, recipient.phone) AS recipient_phone,
            COALESCE(rd.telegram, recipient.telegram) AS recipient_telegram,
            COALESCE(rd.whatsapp, recipient.whatsapp) AS recipient_whatsapp,
            COALESCE(rd.viber, recipient.viber) AS recipient_viber,
            rd.bio AS recipient_bio
        FROM event_assignments ea
        JOIN events e ON ea.event_id = e.id
        JOIN users recipient ON ea.recipient_user_id = recipient.user_id
        LEFT JOIN event_registration_details rd
            ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
        WHERE ea.santa_user_id = ?
        ORDER BY ea.assigned_at DESC
    ''', (user_id,)).fetchall()
    
    # Получаем задания, где пользователь Внучка
    as_recipient_rows = conn.execute('''
        SELECT 
            ea.*,
            e.name as event_name,
            e.id as event_id,
            santa.username as santa_username,
            santa.level as santa_level,
            santa.synd as santa_synd,
            recipient.username AS recipient_username,
            recipient.level AS recipient_level,
            recipient.synd AS recipient_synd,
            COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
            COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
            COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
            COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
            COALESCE(rd.country, recipient.country) AS recipient_country,
            COALESCE(rd.city, recipient.city) AS recipient_city,
            COALESCE(rd.street, recipient.street) AS recipient_street,
            COALESCE(rd.house, recipient.house) AS recipient_house,
            COALESCE(rd.building, recipient.building) AS recipient_building,
            COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
            COALESCE(rd.email, recipient.email) AS recipient_email,
            COALESCE(rd.phone, recipient.phone) AS recipient_phone,
            COALESCE(rd.telegram, recipient.telegram) AS recipient_telegram,
            COALESCE(rd.whatsapp, recipient.whatsapp) AS recipient_whatsapp,
            COALESCE(rd.viber, recipient.viber) AS recipient_viber,
            rd.bio AS recipient_bio
        FROM event_assignments ea
        JOIN events e ON ea.event_id = e.id
        JOIN users santa ON ea.santa_user_id = santa.user_id
        JOIN users recipient ON ea.recipient_user_id = recipient.user_id
        LEFT JOIN event_registration_details rd
            ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
        WHERE ea.recipient_user_id = ?
        ORDER BY ea.assigned_at DESC
    ''', (user_id,)).fetchall()
    
    assignments = []
    send_info_updates = []
    thanks_updates = []
    
    for row in as_santa_rows:
        record = dict(row)
        info = record.get('santa_send_info')
        if info:
            normalized = _normalize_multiline_text(info)
            if normalized != info:
                record['santa_send_info'] = normalized
                send_info_updates.append((normalized, record['id']))
        assignments.append(record)
    
    for row in as_recipient_rows:
        record = dict(row)
        send_info = record.get('santa_send_info')
        if send_info:
            normalized = _normalize_multiline_text(send_info)
            if normalized != send_info:
                record['santa_send_info'] = normalized
                send_info_updates.append((normalized, record['id']))
        thanks = record.get('recipient_thanks_message')
        if thanks:
            normalized_thanks = _normalize_multiline_text(thanks)
            if normalized_thanks != thanks:
                record['recipient_thanks_message'] = normalized_thanks
                thanks_updates.append((normalized_thanks, record['id']))
        assignments.append(record)
    
    if send_info_updates:
        conn.executemany('UPDATE event_assignments SET santa_send_info = ? WHERE id = ?', send_info_updates)
    if thanks_updates:
        conn.executemany('UPDATE event_assignments SET recipient_thanks_message = ? WHERE id = ?', thanks_updates)
    if send_info_updates or thanks_updates:
        conn.commit()
    conn.close()
    
    return assignments

def get_admin_letter_assignments():
    """Возвращает все переписки для администраторов"""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT
            ea.*,
            e.name AS event_name,
            santa.username AS santa_username,
            santa.first_name AS santa_first_name,
            santa.last_name AS santa_last_name,
            santa.middle_name AS santa_middle_name,
            COALESCE(sd.country, santa.country) AS santa_country,
            COALESCE(sd.city, santa.city) AS santa_city,
            recipient.username AS recipient_username,
            COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
            COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
            COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
            COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
            COALESCE(rd.country, recipient.country) AS recipient_country,
            COALESCE(rd.city, recipient.city) AS recipient_city,
            COALESCE(rd.street, recipient.street) AS recipient_street,
            COALESCE(rd.house, recipient.house) AS recipient_house,
            COALESCE(rd.building, recipient.building) AS recipient_building,
            COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
            rd.bio AS recipient_bio,
            lm.message_count,
            lm.last_message_at
        FROM event_assignments ea
        JOIN events e ON ea.event_id = e.id
        JOIN users santa ON ea.santa_user_id = santa.user_id
        JOIN users recipient ON ea.recipient_user_id = recipient.user_id
        LEFT JOIN event_registration_details rd
            ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
        LEFT JOIN event_registration_details sd
            ON sd.event_id = ea.event_id AND sd.user_id = ea.santa_user_id
        LEFT JOIN (
            SELECT assignment_id,
                   COUNT(*) AS message_count,
                   MAX(created_at) AS last_message_at
            FROM letter_messages
            GROUP BY assignment_id
        ) lm ON lm.assignment_id = ea.id
        ORDER BY
            CASE WHEN lm.last_message_at IS NULL THEN 1 ELSE 0 END,
            lm.last_message_at DESC,
            ea.id ASC
    ''').fetchall()

    assignments = []
    send_info_updates = []
    thanks_updates = []
    for row in rows:
        record = dict(row)
        info = record.get('santa_send_info')
        if info:
            normalized = _normalize_multiline_text(info)
            if normalized != info:
                record['santa_send_info'] = normalized
                send_info_updates.append((normalized, record['id']))
        thanks = record.get('recipient_thanks_message')
        if thanks:
            normalized_thanks = _normalize_multiline_text(thanks)
            if normalized_thanks != thanks:
                record['recipient_thanks_message'] = normalized_thanks
                thanks_updates.append((normalized_thanks, record['id']))
        record['message_count'] = record.get('message_count') or 0
        record['last_message_at'] = record.get('last_message_at')
        santa_parts = [record.get('santa_last_name') or '', record.get('santa_first_name') or '', record.get('santa_middle_name') or '']
        record['santa_full_name'] = ' '.join(part for part in santa_parts if part).strip() or record.get('santa_username')
        recipient_parts = [record.get('recipient_last_name') or '', record.get('recipient_first_name') or '', record.get('recipient_middle_name') or '']
        record['recipient_full_name'] = ' '.join(part for part in recipient_parts if part).strip() or record.get('recipient_username')
        record['chat_role'] = 'admin'
        assignments.append(record)

    if send_info_updates:
        conn.executemany('UPDATE event_assignments SET santa_send_info = ? WHERE id = ?', send_info_updates)
    if thanks_updates:
        conn.executemany('UPDATE event_assignments SET recipient_thanks_message = ? WHERE id = ?', thanks_updates)
    if send_info_updates or thanks_updates:
        conn.commit()
    conn.close()
    return assignments

def mark_assignment_sent(assignment_id, user_id, send_info):
    """Отмечает, что подарок отправлен"""
    clear_requested = False
    if not send_info or not send_info.strip():
        return False, 'Введите данные об отправке'
    send_info = _normalize_multiline_text(send_info, max_length=500)
    if not send_info:
        return False, 'Введите данные об отправке'
    
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return False, 'Некорректный идентификатор пользователя'
    
    conn = get_db_connection()
    assignment = conn.execute('SELECT * FROM event_assignments WHERE id = ?', (assignment_id,)).fetchone()
    
    if not assignment:
        conn.close()
        return False, 'Задание не найдено'
    
    if assignment['santa_user_id'] != user_id_int:
        conn.close()
        return False, 'Вы не можете обновить это задание'
    
    if is_event_finished(assignment['event_id']):
        conn.close()
        return False, 'Мероприятие завершено. Действия с заданием недоступны.'

    try:
        chat_message = None
        if clear_requested:
            conn.execute('''
                UPDATE event_assignments
                SET santa_send_info = NULL
                WHERE id = ?
            ''', (assignment_id,))
            system_message = (
                "Дорогой внучок! Я скорректировал информацию об отправке. "
                "Если будут вопросы — пиши!"
            )
            conn.execute('''
                INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
                VALUES (?, 'santa', ?, NULL)
            ''', (assignment_id, system_message))
        else:
            chat_message = (
                f"Дорогой внучок! Я всё отправил! {send_info}\n"
                "Если будут вопросы — пиши!"
            ).strip()
        previous_info = assignment['santa_send_info']
        updated_existing = bool(previous_info)
        conn.execute('''
            UPDATE event_assignments
            SET santa_sent_at = CURRENT_TIMESTAMP,
                santa_send_info = ?
            WHERE id = ?
        ''', (send_info, assignment_id))
        if updated_existing:
            chat_message = (
                f"Внучок! Данные для получения изменились: {send_info}"
            ).strip()
        else:
            chat_message = (
                f"Дорогой внучок! Я всё отправил! {send_info}\n"
                "Если будут вопросы — пиши!"
            ).strip()
        conn.execute('''
            INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
            VALUES (?, 'santa', ?, NULL)
        ''', (assignment_id, chat_message))

        conn.commit()
        log_activity(
            'assignment_sent',
            details=f'Подарок отправлен по назначению #{assignment_id}',
            metadata={'assignment_id': assignment_id, 'event_id': assignment['event_id']}
        )
        return True, 'Информация об отправке сохранена'
    except Exception as e:
        log_error(f"Error marking assignment sent (id={assignment_id}): {e}")
        conn.rollback()
        return False, 'Не удалось сохранить информацию об отправке'
    finally:
        conn.close()
def mark_assignment_received(assignment_id, user_id, thank_you_message, receipt_file):
    """Отмечает, что подарок получен"""
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return False, 'Некорректный идентификатор пользователя'
    
    conn = get_db_connection()
    assignment = conn.execute('SELECT * FROM event_assignments WHERE id = ?', (assignment_id,)).fetchone()
    
    if not assignment:
        conn.close()
        return False, 'Задание не найдено'
    
    if assignment['recipient_user_id'] != user_id_int:
        conn.close()
        return False, 'Вы не можете обновить это задание'
    
    if is_event_finished(assignment['event_id']):
        conn.close()
        return False, 'Мероприятие завершено. Действия с заданием недоступны.'
    
    if not assignment['santa_sent_at']:
        conn.close()
        return False, 'Даритель еще не отметил отправку подарка'
    conn.close()

    thank_you_message = _normalize_multiline_text(thank_you_message, max_length=1000)
    if not thank_you_message:
        return False, 'Напишите спасибо для Деда Мороза.'

    if not receipt_file or not receipt_file.filename:
        return False, 'Приложите фотографию подарка.'

    filename = secure_filename(receipt_file.filename)
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in ALLOWED_LETTER_IMAGE_EXTENSIONS:
        return False, 'Допускается загрузка только изображений (PNG, JPG, JPEG, GIF, WEBP).'

    unique_name = f"{assignment_id}_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}{ext}"
    saved_filepath = os.path.join(ASSIGNMENT_RECEIPT_FOLDER, unique_name)
    try:
        receipt_file.save(saved_filepath)
    except Exception as exc:
        log_error(f"Failed to save assignment receipt image {unique_name}: {exc}")
        return False, 'Не удалось загрузить изображение.'

    receipt_relative_path = f"{ASSIGNMENT_RECEIPT_RELATIVE}/{unique_name}"

    conn = get_db_connection()
    try:
        conn.execute('''
            UPDATE event_assignments
            SET recipient_received_at = CURRENT_TIMESTAMP,
                recipient_thanks_message = ?,
                recipient_receipt_image = ?
            WHERE id = ?
        ''', (thank_you_message, receipt_relative_path, assignment_id))
        conn.commit()
        log_activity(
            'assignment_received',
            details=f'Получение подарка подтверждено по заданию #{assignment_id}',
            metadata={'assignment_id': assignment_id, 'event_id': assignment['event_id']}
        )
        conn.execute('''
            INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
            VALUES (?, 'grandchild', ?, ?)
        ''', (
            assignment_id,
            f"Дорогой Дед Мороз! Спасибо за подарок! {thank_you_message}",
            receipt_relative_path
        ))
        conn.commit()
        return True, 'Получение подарка подтверждено'
    except Exception as e:
        log_error(f"Error marking assignment received (id={assignment_id}): {e}")
        conn.rollback()
        try:
            if os.path.exists(saved_filepath):
                os.remove(saved_filepath)
        except OSError:
            pass
        return False, 'Не удалось подтвердить получение подарка'
    finally:
        conn.close()

@app.route('/events')
def events():
    """Публичная страница со списком всех мероприятий"""
    conn = get_db_connection()
    events_list = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.deleted_at IS NULL
        ORDER BY e.created_at DESC
    ''').fetchall()
    conn.close()
    
    event_ids = [event['id'] for event in events_list]
    user_id = session.get('user_id')
    user_registrations = {}
    if user_id and event_ids:
        placeholders = ','.join(['?'] * len(event_ids))
        conn = get_db_connection()
        rows = conn.execute(
            f'''
            SELECT event_id, registered_at
            FROM event_registrations
            WHERE user_id = ? AND event_id IN ({placeholders})
            ''',
            (user_id, *event_ids)
        ).fetchall()
        conn.close()
        for row in rows:
            user_registrations[row['event_id']] = row['registered_at']
    
    # Определяем текущий этап и ближайший будущий этап для каждого мероприятия
    events_with_stages_raw = []
    now = get_event_now()
    stage_info_map = {stage['type']: stage for stage in EVENT_STAGES}

    def parse_dt(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    for event in events_list:
        current_stage = get_current_event_stage(event['id'])
        display_stage_name = None
        next_stage = None
        if current_stage:
            display_stage_name = current_stage['info']['name']
            if current_stage['info']['type'] == 'registration_closed':
                lottery_stage = next((stage for stage in EVENT_STAGES if stage['type'] == 'lottery'), None)
                display_stage_name = lottery_stage['name'] if lottery_stage else 'Жеребьёвка'
        
        # Определяем следующий этап для таймера
        stages = get_event_stages(event['id'])
        for stage in stages:
            start_dt = parse_dt(stage['start_datetime'])
            if not start_dt or start_dt <= now:
                continue

            stage_info = stage_info_map.get(stage['stage_type'])
            stage_name = stage_info['name'] if stage_info else stage['stage_type']

            if (not next_stage) or start_dt < next_stage['start_dt']:
                next_stage = {
                    'name': stage_name,
                    'start_dt': start_dt,
                    'start_iso': start_dt.isoformat()
                }

        registered_at_str = user_registrations.get(event['id'])
        registered_at = parse_dt(registered_at_str) if registered_at_str else None
        is_registered = registered_at is not None
        pre_stage_start_dt = None
        main_stage_start_dt = None
        for stage in stages:
            stage_type = stage['stage_type']
            stage_start = parse_dt(stage['start_datetime'])
            if stage_type == 'pre_registration':
                pre_stage_start_dt = stage_start
            elif stage_type == 'main_registration':
                main_stage_start_dt = stage_start

        needs_confirmation = False
        if (
            is_registered
            and pre_stage_start_dt
            and main_stage_start_dt
            and registered_at
            and registered_at >= pre_stage_start_dt
            and registered_at < main_stage_start_dt
            and now >= main_stage_start_dt
        ):
            needs_confirmation = True

        if not is_registration_open(event['id']):
            needs_confirmation = False

        current_stage = get_current_event_stage(event['id'])
        value = {
            'event': event,
            'current_stage': current_stage,
            'display_stage_name': display_stage_name,
            'next_stage': next_stage,
            'is_registered': is_registered,
            'needs_confirmation': needs_confirmation,
            'registration_open': is_registration_open(event['id'])
        }

        # если текущего этапа нет и следующего будущего этапа тоже нет, значит все этапы завершены
        value['next_stage_is_past'] = False
        if not current_stage and not next_stage:
            # Проверяем, были ли когда-то этапы
            value['next_stage_is_past'] = True

        events_with_stages_raw.append(value)

    events_with_stages = events_with_stages_raw

    for item in events_with_stages:
        event = item['event']
        item['registrations_count'] = get_event_registrations_count(event['id'])

    # Название проекта
    project_name = get_setting('project_name', 'Анонимные Деды Морозы')
    
    # Получаем тексты модальных окон
    modal_texts = {}
    conn = get_db_connection()
    modal_settings = conn.execute('SELECT key, value FROM settings WHERE category = ?', ('modals',)).fetchall()
    conn.close()
    for setting in modal_settings:
        modal_texts[setting['key']] = setting['value']
    
    return render_template('events.html', events_with_stages=events_with_stages, modal_texts=modal_texts)
@app.route('/events/<int:event_id>')
def event_view(event_id):
    """Просмотр мероприятия для пользователей"""
    conn = get_db_connection()
    event = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.id = ?
    ''', (event_id,)).fetchone()
    conn.close()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        return redirect(url_for('events'))
    
    user_id = session.get('user_id')
    current_stage = get_current_event_stage(event_id)
    registration_open = is_registration_open(event_id)

    registration_row = None
    if user_id:
        conn = get_db_connection()
        registration_row = conn.execute(
            '''
            SELECT registered_at
            FROM event_registrations
            WHERE event_id = ? AND user_id = ?
            ''',
            (event_id, user_id)
        ).fetchone()
        conn.close()

    is_registered = registration_row is not None
    registrations_count = get_event_registrations_count(event_id)
    
    # Параметры пагинации для участников
    participants_page = request.args.get('participants_page', 1, type=int)
    participants_per_page = 20  # По 20 участников на странице
    
    # Получаем участников с пагинацией
    registrations_data = get_event_registrations_paginated(event_id, participants_page, participants_per_page)
    registrations = registrations_data['registrations']
    
    is_admin = 'admin' in session.get('roles', []) if session.get('roles') else False
    
    award_needed = False
    if current_stage and current_stage['info']['type'] == 'after_party':
        award_needed = True
    elif is_event_finished(event_id):
        award_needed = True

    if award_needed:
        distribute_event_awards(event_id, require_sent=True)
    
    # Получаем все этапы мероприятия
    conn = get_db_connection()
    stage_rows = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()

    stages = [dict(row) for row in stage_rows]
    for stage in stages:
        if (
            stage.get('stage_type') == 'after_party'
            and not stage.get('start_datetime')
            and stage.get('end_datetime')
        ):
            stage['start_datetime'] = stage['end_datetime']
    
    # Определяем статус каждого этапа (past, current, future)
    now = get_event_now()
    current_stage_type = current_stage['info']['type'] if current_stage else None
    
    stages_with_info = []
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    
    next_stage_candidate = None
    main_stage_start_dt = None
    pre_stage_start_dt = None
    if 'main_registration' in stages_dict:
        main_stage_row = stages_dict['main_registration']
        main_keys = main_stage_row.keys()
        main_start_val = main_stage_row['start_datetime'] if 'start_datetime' in main_keys else None
        if main_start_val:
            try:
                main_stage_start_dt = datetime.fromisoformat(str(main_start_val))
            except ValueError:
                main_stage_start_dt = None
    if 'pre_registration' in stages_dict:
        pre_stage_row = stages_dict['pre_registration']
        pre_keys = pre_stage_row.keys()
        pre_start_val = pre_stage_row['start_datetime'] if 'start_datetime' in pre_keys else None
        if pre_start_val:
            try:
                pre_stage_start_dt = datetime.fromisoformat(str(pre_start_val))
            except ValueError:
                pre_stage_start_dt = None

    registration_dt = None
    if registration_row and registration_row['registered_at']:
        try:
            registration_dt = datetime.fromisoformat(str(registration_row['registered_at']))
        except ValueError:
            registration_dt = None

    needs_main_confirmation = False
    if (
        is_registered
        and main_stage_start_dt
        and pre_stage_start_dt
        and registration_dt
        and registration_dt >= pre_stage_start_dt
        and registration_dt < main_stage_start_dt
        and now >= main_stage_start_dt
    ):
        needs_main_confirmation = True

    for stage_info in EVENT_STAGES:
        stage_type = stage_info['type']
        stage_data = stages_dict.get(stage_type, None)
        
        # Определяем статус этапа
        stage_status = 'future'  # по умолчанию будущий
        start_dt = None
        end_dt = None
        if stage_data:
            stage_data = dict(stage_data)
            stage_keys = stage_data.keys()
            start_value = stage_data['start_datetime'] if 'start_datetime' in stage_keys else None
            end_value = stage_data['end_datetime'] if 'end_datetime' in stage_keys else None

            # Пропускаем необязательные этапы без даты начала
            if stage_info.get('has_start') and not stage_info.get('required') and not start_value:
                log_debug(f"get_current_event_stage: skipping optional stage {stage_type} without start date for event {event_id}")
                continue

            if start_value:
                try:
                    start_dt = datetime.fromisoformat(str(start_value))
                except ValueError:
                    start_dt = None

            if end_value:
                try:
                    end_dt = datetime.fromisoformat(str(end_value))
                except ValueError:
                    end_dt = None

            # Проверяем, является ли это текущим этапом
            if current_stage_type == stage_type:
                stage_status = 'current'
            else:
                if start_dt:
                    if now < start_dt:
                        stage_status = 'future'
                    else:
                        # Этап уже начался или завершился
                        if end_dt and now < end_dt:
                            stage_status = 'past'
                        else:
                            stage_status = 'past'
                else:
                    stage_status = 'future'
        
        stages_with_info.append({
            'info': stage_info,
            'data': stage_data,
            'status': stage_status
        })

        if start_dt and start_dt > now:
            if (not next_stage_candidate) or start_dt < next_stage_candidate['start_dt']:
                next_stage_candidate = {
                    'name': stage_info['name'],
                    'start_datetime': stage_data['start_datetime'],
                    'start_dt': start_dt,
                    'stage_type': stage_type
                }
    
    if current_stage and not next_stage_candidate:
        current_type = current_stage['info']['type']
        try:
            current_index = next(i for i, s in enumerate(EVENT_STAGES) if s['type'] == current_type)
        except StopIteration:
            current_index = None

        if current_index is not None:
            for idx in range(current_index + 1, len(EVENT_STAGES)):
                next_info = EVENT_STAGES[idx]
                next_data = stages_dict.get(next_info['type'])
                candidate_raw = None
                candidate_dt = None

                if next_data and next_data.get('start_datetime'):
                    candidate_raw = next_data['start_datetime']
                elif next_data and next_data.get('end_datetime'):
                    candidate_raw = next_data['end_datetime']
                elif next_info['type'] == 'after_party' and current_stage['data'] and current_stage['data'].get('end_datetime'):
                    candidate_raw = current_stage['data']['end_datetime']

                if candidate_raw:
                    candidate_dt = parse_event_datetime(str(candidate_raw))

                if candidate_dt and candidate_dt > now:
                    next_stage_candidate = {
                        'name': next_info['name'],
                        'start_datetime': candidate_raw,
                        'start_dt': candidate_dt,
                        'stage_type': next_info['type']
                    }
                    break

    # Получаем тексты модальных окон
    modal_texts = {}
    conn = get_db_connection()
    modal_settings = conn.execute('SELECT key, value FROM settings WHERE category = ?', ('modals',)).fetchall()
    conn.close()
    for setting in modal_settings:
        modal_texts[setting['key']] = setting['value']
    
    if not next_stage_candidate:
        for stage_info in EVENT_STAGES:
            data = stages_dict.get(stage_info['type'])
            if not data:
                continue

            candidate_raw = None
            candidate_dt = None

            if data.get('start_datetime'):
                try:
                    candidate_dt = datetime.fromisoformat(str(data['start_datetime']))
                    candidate_raw = data['start_datetime']
                except ValueError:
                    candidate_dt = None

            if (not candidate_dt or candidate_dt <= now) and data.get('end_datetime') and stage_info['type'] == 'after_party':
                try:
                    candidate_dt = datetime.fromisoformat(str(data['end_datetime']))
                    candidate_raw = data['end_datetime']
                except ValueError:
                    candidate_dt = None

            if candidate_dt and candidate_dt > now:
                next_stage_candidate = {
                    'name': stage_info['name'],
                    'start_datetime': candidate_raw,
                    'start_dt': candidate_dt,
                    'stage_type': stage_info['type']
                }
                break

    next_stage_payload = None
    if next_stage_candidate:
        start_dt_local = next_stage_candidate.get('start_dt')
        if isinstance(start_dt_local, datetime):
            start_iso = start_dt_local.strftime('%Y-%m-%dT%H:%M:%S')
            next_stage_payload = {
                'name': next_stage_candidate.get('name'),
                'start_datetime': next_stage_candidate.get('start_datetime'),
                'start_iso': start_iso,
                'stage_type': next_stage_candidate.get('stage_type')
            }

    # Получаем статистику по подаркам (только после закрытия регистрации)
    gifts_stats = None
    show_gifts_stats = False
    if current_stage:
        stage_type = current_stage['info']['type']
        # Показываем статистику после закрытия регистрации
        if stage_type in ['registration_closed', 'lottery', 'celebration_date', 'after_party']:
            show_gifts_stats = True
            gifts_stats = get_event_gifts_statistics(event_id)
    
    return render_template('event_view.html', 
                         event=event,
                         current_stage=current_stage,
                         modal_texts=modal_texts,
                         registration_open=registration_open,
                         is_registered=is_registered,
                         needs_main_confirmation=needs_main_confirmation,
                         registrations_count=registrations_count,
                         registrations=registrations,
                         stages_with_info=stages_with_info,
                         is_admin=is_admin,
                         next_stage=next_stage_payload,
                         show_gifts_stats=show_gifts_stats,
                         gifts_stats=gifts_stats,
                         participants_page=registrations_data['page'],
                         participants_per_page=registrations_data['per_page'],
                         participants_total_count=registrations_data['total_count'],
                         participants_total_pages=registrations_data['total_pages'],
                         participants_has_prev=registrations_data['has_prev'],
                         participants_has_next=registrations_data['has_next'])

def has_required_contacts(user_id):
    """Проверяет, заполнены ли обязательные контактные данные пользователя"""
    conn = get_db_connection()
    try:
        user = conn.execute('''
            SELECT email, phone, telegram, whatsapp, viber,
                   last_name, first_name, middle_name,
                   postal_code, country, city, street, house, building, apartment,
                   bio
            FROM users 
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        conn.close()
        
        if not user:
            return False
        
        # Проверяем обязательные поля:
        # 1. Хотя бы одно контактное поле (email, phone, telegram, whatsapp, viber)
        has_contact = bool(user['email'] or user['phone'] or user['telegram'] or user['whatsapp'] or user['viber'])
        
        # 2. Все личные данные (фамилия, имя, отчество)
        has_personal_data = bool(user['last_name'] and user['first_name'] and user['middle_name'])
        
        # 3. Все поля адреса (индекс, страна, город, улица, дом, корпус/строение, квартира)
        has_address = bool(user['postal_code'] and user['country'] and user['city'] and 
                         user['street'] and user['house'] and user['building'] and user['apartment'])
        
        return has_contact and has_personal_data and has_address
    except Exception as e:
        log_error(f"Ошибка проверки контактов пользователя: {e}")
        conn.close()
        return False

def get_missing_required_fields(user_id):
    """Возвращает информацию о незаполненных обязательных полях"""
    conn = get_db_connection()
    try:
        user = conn.execute('''
            SELECT email, phone, telegram, whatsapp, viber,
                   last_name, first_name, middle_name,
                   postal_code, country, city, street, house, building, apartment,
                   bio
            FROM users 
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        conn.close()
        
        if not user:
            return {
                'has_personal_data': False,
                'has_address': False,
                'has_contact': False,
                'missing_personal': ['last_name', 'first_name', 'middle_name'],
                'missing_address': ['postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment'],
                'missing_contacts': ['email', 'phone', 'telegram', 'whatsapp', 'viber']
            }
        
        missing_personal = []
        if not user['last_name']:
            missing_personal.append('last_name')
        if not user['first_name']:
            missing_personal.append('first_name')
        if not user['middle_name']:
            missing_personal.append('middle_name')
        
        missing_address = []
        if not user['postal_code']:
            missing_address.append('postal_code')
        if not user['country']:
            missing_address.append('country')
        if not user['city']:
            missing_address.append('city')
        if not user['street']:
            missing_address.append('street')
        if not user['house']:
            missing_address.append('house')
        if not user['building']:
            missing_address.append('building')
        if not user['apartment']:
            missing_address.append('apartment')
        
        missing_contacts = []
        if not user['email']:
            missing_contacts.append('email')
        if not user['phone']:
            missing_contacts.append('phone')
        if not user['telegram']:
            missing_contacts.append('telegram')
        if not user['whatsapp']:
            missing_contacts.append('whatsapp')
        if not user['viber']:
            missing_contacts.append('viber')
        
        return {
            'has_personal_data': len(missing_personal) == 0,
            'has_address': len(missing_address) == 0,
            'has_contact': bool(user['email'] or user['phone'] or user['telegram'] or user['whatsapp'] or user['viber']),
            'missing_personal': missing_personal,
            'missing_address': missing_address,
            'missing_contacts': missing_contacts
        }
    except Exception as e:
        log_error(f"Ошибка получения незаполненных полей: {e}")
        conn.close()
        return {
            'has_personal_data': False,
            'has_address': False,
            'has_contact': False,
            'missing_personal': ['last_name', 'first_name', 'middle_name'],
            'missing_address': ['postal_code', 'country', 'city', 'street', 'house', 'building', 'apartment'],
            'missing_contacts': ['email', 'phone', 'telegram', 'whatsapp', 'viber']
        }
@app.route('/events/<int:event_id>/register', methods=['POST'])
@require_login
def event_register(event_id):
    """Регистрация пользователя на мероприятие"""
    user_id = session.get('user_id')
    # Проверяем, является ли запрос AJAX/JSON запросом
    is_json_request = (
        request.headers.get('Content-Type') == 'application/json'
        or request.headers.get('Accept') == 'application/json'
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        or request.is_json
    )
    payload = {}
    if is_json_request:
        payload = request.get_json(silent=True) or {}
    start_flow = bool(payload.get('start_registration_flow'))
    final_registration = bool(payload.get('final_registration'))

    if not user_id:
        if is_json_request:
            return jsonify({'success': False, 'error': 'Необходимо авторизоваться'}), 401
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('login'))

    # Получаем информацию о регистрации пользователя
    registration_row = None
    main_stage_row = None
    pre_stage_row = None
    if user_id:
        conn = get_db_connection()
        registration_row = conn.execute(
            '''
            SELECT registered_at
            FROM event_registrations
            WHERE event_id = ? AND user_id = ?
            ''',
            (event_id, user_id)
        ).fetchone()
        main_stage_row = conn.execute(
            '''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'main_registration'
            ''',
            (event_id,)
        ).fetchone()
        pre_stage_row = conn.execute(
            '''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'pre_registration'
            ''',
            (event_id,)
        ).fetchone()
        conn.close()

    is_registered = registration_row is not None

    needs_confirmation = False
    registration_dt = None
    if registration_row and registration_row['registered_at']:
        try:
            registration_dt = datetime.fromisoformat(str(registration_row['registered_at']))
        except ValueError:
            registration_dt = None

    main_stage_start_dt = None
    pre_stage_start_dt = None
    if main_stage_row and main_stage_row['start_datetime']:
        try:
            main_stage_start_dt = datetime.fromisoformat(str(main_stage_row['start_datetime']))
        except ValueError:
            main_stage_start_dt = None
    if pre_stage_row and pre_stage_row['start_datetime']:
        try:
            pre_stage_start_dt = datetime.fromisoformat(str(pre_stage_row['start_datetime']))
        except ValueError:
            pre_stage_start_dt = None

    if (
        is_registered
        and main_stage_start_dt
        and pre_stage_start_dt
        and registration_dt
        and registration_dt >= pre_stage_start_dt
        and registration_dt < main_stage_start_dt
        and datetime.now() >= main_stage_start_dt
    ):
        needs_confirmation = True

    # Проверяем, открыта ли регистрация (или доступно подтверждение)
    if not is_registration_open(event_id):
        if is_json_request:
            return jsonify({'success': False, 'error': 'Регистрация на это мероприятие закрыта'}), 400
        flash('Регистрация на это мероприятие закрыта', 'error')
        return redirect(url_for('event_view', event_id=event_id))

    # Запрос на начало модального сценария
    if start_flow:
        if is_registered and not needs_confirmation:
            return jsonify({'success': False, 'error': 'Вы уже зарегистрированы на это мероприятие'}), 400
        missing_fields = get_missing_required_fields(user_id)
        return jsonify({
            'success': True,
            'missing_fields': missing_fields
        }), 200

    def complete_registration():
        """Проводит регистрацию и сохраняет слепок данных пользователя."""
        conn = get_db_connection()
        try:
            profile_row = conn.execute('''
                SELECT last_name, first_name, middle_name,
                       postal_code, country, city, street, house, building, apartment,
                       email, phone, telegram, whatsapp, viber, bio
                FROM users
                WHERE user_id = ?
            ''', (user_id,)).fetchone()

            if not profile_row:
                return {'status': 'error', 'message': 'Пользователь не найден'}

            profile = {key: (profile_row[key] or '').strip() for key in profile_row.keys()}
            required_fields = [
                ('last_name', 'Фамилия'),
                ('first_name', 'Имя'),
                ('middle_name', 'Отчество'),
                ('postal_code', 'Индекс'),
                ('country', 'Страна'),
                ('city', 'Город'),
                ('street', 'Улица'),
                ('house', 'Дом'),
                ('building', 'Корпус/строение'),
                ('apartment', 'Квартира'),
                ('phone', 'Номер телефона')
            ]
            missing_required = [label for field, label in required_fields if not profile.get(field)]
            if missing_required:
                return {
                    'status': 'missing',
                    'missing': missing_required
                }

            cursor = conn.execute('''
                INSERT OR IGNORE INTO event_registrations (event_id, user_id)
                VALUES (?, ?)
            ''', (event_id, user_id))
            already_registered = cursor.rowcount == 0

            conn.execute('''
                INSERT INTO event_registration_details (
                    event_id, user_id, last_name, first_name, middle_name,
                    postal_code, country, city, street, house, building, apartment,
                    email, phone, telegram, whatsapp, viber, bio
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id, user_id) DO UPDATE SET
                    last_name = excluded.last_name,
                    first_name = excluded.first_name,
                    middle_name = excluded.middle_name,
                    postal_code = excluded.postal_code,
                    country = excluded.country,
                    city = excluded.city,
                    street = excluded.street,
                    house = excluded.house,
                    building = excluded.building,
                    apartment = excluded.apartment,
                    email = excluded.email,
                    phone = excluded.phone,
                    telegram = excluded.telegram,
                    whatsapp = excluded.whatsapp,
                    viber = excluded.viber,
                    bio = excluded.bio,
                    updated_at = CURRENT_TIMESTAMP
            ''', (
                event_id,
                user_id,
                profile.get('last_name'),
                profile.get('first_name'),
                profile.get('middle_name'),
                profile.get('postal_code'),
                profile.get('country'),
                profile.get('city'),
                profile.get('street'),
                profile.get('house'),
                profile.get('building'),
                profile.get('apartment'),
                profile.get('email'),
                profile.get('phone'),
                profile.get('telegram'),
                profile.get('whatsapp'),
                profile.get('viber'),
                profile.get('bio')
            ))

            if already_registered and needs_confirmation:
                conn.execute('''
                    UPDATE event_registrations
                    SET registered_at = CURRENT_TIMESTAMP
                    WHERE event_id = ? AND user_id = ?
                ''', (event_id, user_id))

            conn.commit()
            return {
                'status': 'success',
                'already_registered': already_registered,
                'reconfirmed': already_registered and needs_confirmation
            }
        except Exception as exc:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            log_error(f"Ошибка регистрации на мероприятие #{event_id}: {exc}")
            return {'status': 'error', 'message': 'Ошибка при регистрации'}
        finally:
            conn.close()

    # Финальный запрос из модального окна
    if final_registration:
        result = complete_registration()
        if result['status'] == 'success':
            if result.get('reconfirmed'):
                log_activity(
                    'event_confirm',
                    details=f'Подтверждение участия в мероприятии #{event_id}',
                    metadata={'event_id': event_id}
                )
                message = 'Ваше участие подтверждено!'
            elif not result.get('already_registered'):
                log_activity(
                    'event_register',
                    details=f'Регистрация на мероприятие #{event_id}',
                    metadata={'event_id': event_id}
                )
                message = 'Вы успешно зарегистрированы на мероприятие!'
            else:
                message = 'Вы уже зарегистрированы на это мероприятие'
            return jsonify({
                'success': True,
                'message': message,
                'already_registered': result.get('already_registered', False),
                'reconfirmed': result.get('reconfirmed', False)
            }), 200

        if result['status'] == 'missing':
            return jsonify({
                'success': False,
                'error': 'Пожалуйста, заполните все обязательные поля',
                'missing': result.get('missing', [])
            }), 400

        return jsonify({'success': False, 'error': result.get('message', 'Ошибка при регистрации')}), 500

    # Если это JSON-запрос без уточнения, возвращаем ошибку
    if is_json_request:
        return jsonify({'success': False, 'error': 'Некорректный запрос'}), 400

    # Обычный POST-запрос (без модальных окон) — пытаемся завершить регистрацию
    if is_registered and not needs_confirmation:
        flash('Вы уже зарегистрированы на это мероприятие', 'info')
        return redirect(url_for('event_view', event_id=event_id))

    result = complete_registration()
    if result['status'] == 'success':
        if result.get('reconfirmed'):
            log_activity(
                'event_confirm',
                details=f'Подтверждение участия в мероприятии #{event_id}',
                metadata={'event_id': event_id}
            )
            flash('Ваше участие подтверждено!', 'success')
        elif not result.get('already_registered'):
            log_activity(
                'event_register',
                details=f'Регистрация на мероприятие #{event_id}',
                metadata={'event_id': event_id}
            )
            flash('Вы успешно зарегистрированы на мероприятие!', 'success')
        else:
            flash('Вы уже зарегистрированы на это мероприятие', 'info')
    elif result['status'] == 'missing':
        missing_list = result.get('missing', [])
        flash(
            'Для регистрации необходимо заполнить обязательные поля: ' + ', '.join(missing_list),
            'error'
        )
    else:
        flash('Ошибка при регистрации', 'error')

    return redirect(url_for('event_view', event_id=event_id))
@app.route('/api/profile/data', methods=['GET'])
@require_login
def api_profile_data():
    """API endpoint для получения текущих данных профиля пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Необходимо авторизоваться'}), 401
    
    conn = get_db_connection()
    try:
        # Логируем для отладки
        log_debug(f"api_profile_data: Fetching data for user_id={user_id}")
        
        user = conn.execute('''
            SELECT email, phone, telegram, whatsapp, viber,
                   last_name, first_name, middle_name,
                   postal_code, country, city, street, house, building, apartment,
                   bio
            FROM users 
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if not user:
            conn.close()
            log_error(f"api_profile_data: User {user_id} not found in database")
            return jsonify({'error': 'Пользователь не найден'}), 404
        
        # Логируем полученные данные для отладки
        log_debug(f"api_profile_data: User {user_id} data: email={user['email']}, phone={user['phone']}, telegram={user['telegram']}")
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'email': user['email'] or '',
                'phone': user['phone'] or '',
                'telegram': user['telegram'] or '',
                'whatsapp': user['whatsapp'] or '',
                'viber': user['viber'] or '',
                'last_name': user['last_name'] or '',
                'first_name': user['first_name'] or '',
                'middle_name': user['middle_name'] or '',
                'postal_code': user['postal_code'] or '',
                'country': user['country'] or '',
                'city': user['city'] or '',
                'street': user['street'] or '',
                'house': user['house'] or '',
                'building': user['building'] or '',
                'apartment': user['apartment'] or '',
                'bio': user['bio'] or ''
            }
        })
    except Exception as e:
        log_error(f"Error getting profile data for user_id={user_id}: {e}")
        log_error(traceback.format_exc())
        conn.close()
        return jsonify({'error': f'Ошибка получения данных: {str(e)}'}), 500
@app.route('/api/profile/update', methods=['POST'])
@require_login
def api_profile_update():
    """API endpoint для обновления профиля через AJAX"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходимо авторизоваться'}), 401
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'Нет данных'}), 400
    
    conn = get_db_connection()
    try:
        # Обновляем только переданные поля
        update_fields = []
        update_values = []
        
        if 'last_name' in data:
            update_fields.append('last_name = ?')
            update_values.append(data['last_name'].strip())
        if 'first_name' in data:
            update_fields.append('first_name = ?')
            update_values.append(data['first_name'].strip())
        if 'middle_name' in data:
            update_fields.append('middle_name = ?')
            update_values.append(data['middle_name'].strip())
        if 'postal_code' in data:
            update_fields.append('postal_code = ?')
            update_values.append(data['postal_code'].strip())
        if 'country' in data:
            update_fields.append('country = ?')
            update_values.append(data['country'].strip())
        if 'city' in data:
            update_fields.append('city = ?')
            update_values.append(data['city'].strip())
        if 'street' in data:
            update_fields.append('street = ?')
            update_values.append(data['street'].strip())
        if 'house' in data:
            update_fields.append('house = ?')
            update_values.append(data['house'].strip())
        if 'building' in data:
            update_fields.append('building = ?')
            update_values.append(data['building'].strip())
        if 'apartment' in data:
            update_fields.append('apartment = ?')
            update_values.append(data['apartment'].strip())
        if 'email' in data:
            update_fields.append('email = ?')
            update_values.append(data['email'].strip())
        if 'phone' in data:
            update_fields.append('phone = ?')
            update_values.append(data['phone'].strip())
        if 'telegram' in data:
            update_fields.append('telegram = ?')
            update_values.append(data['telegram'].strip())
        if 'whatsapp' in data:
            update_fields.append('whatsapp = ?')
            update_values.append(data['whatsapp'].strip())
        if 'viber' in data:
            update_fields.append('viber = ?')
            update_values.append(data['viber'].strip())
        if 'bio' in data:
            update_fields.append('bio = ?')
            update_values.append(data['bio'].strip())
        
        if not update_fields:
            return jsonify({'success': False, 'error': 'Нет полей для обновления'}), 400
        
        # Логируем для отладки
        log_debug(f"api_profile_update: Updating user_id={user_id}, fields: {', '.join(update_fields)}")
        
        update_values.append(user_id)
        update_query = f'''
            UPDATE users 
            SET {', '.join(update_fields)}
            WHERE user_id = ?
        '''
        conn.execute(update_query, update_values)
        conn.commit()
        
        # Проверяем, что обновление прошло успешно
        verify_user = conn.execute('SELECT email, phone, telegram FROM users WHERE user_id = ?', (user_id,)).fetchone()
        if verify_user:
            log_debug(f"api_profile_update: Verified update for user_id={user_id}: email={verify_user['email']}, phone={verify_user['phone']}, telegram={verify_user['telegram']}")
        
        # Проверяем, все ли обязательные поля заполнены
        missing_fields = get_missing_required_fields(user_id)
        
        return jsonify({
            'success': True,
            'message': 'Данные успешно обновлены',
            'missing_fields': missing_fields
        }), 200
    except Exception as e:
        log_error(f"Ошибка обновления профиля через API: {e}")
        return jsonify({'success': False, 'error': 'Ошибка при обновлении данных'}), 500
    finally:
        conn.close()

@app.route('/events/<int:event_id>/unregister', methods=['POST'])
@require_login
def event_unregister(event_id):
    """Отмена регистрации пользователя на мероприятие"""
    user_id = session.get('user_id')
    if not user_id:
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('login'))
    
    # Проверяем, открыта ли регистрация (можно отменить только если регистрация открыта)
    if not is_registration_open(event_id):
        flash('Регистрация закрыта, нельзя отменить участие', 'error')
        return redirect(url_for('event_view', event_id=event_id))
    
    conn = get_db_connection()
    try:
        cursor = conn.execute('''
            DELETE FROM event_registrations 
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id))
        conn.commit()
        
        if cursor.rowcount > 0:
            log_activity(
                'event_unregister',
                details=f'Отмена регистрации на мероприятие #{event_id}',
                metadata={'event_id': event_id}
            )
            flash('Регистрация отменена', 'success')
        else:
            flash('Вы не были зарегистрированы на это мероприятие', 'info')
    except Exception as e:
        log_error(f"Ошибка отмены регистрации: {e}")
        flash('Ошибка при отмене регистрации', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('event_view', event_id=event_id))

@app.route('/gwars-required')
def gwars_required():
    """Страница с сообщением о необходимости авторизации в GWars"""
    return render_template('gwars_required.html')

@app.route('/faq')
def faq():
    """Страница с часто задаваемыми вопросами"""
    conn = get_db_connection()
    categories_rows = conn.execute('''
        SELECT name, display_name
        FROM faq_categories
        WHERE is_active = 1
        ORDER BY sort_order, display_name
    ''').fetchall()
    items_rows = conn.execute('''
        SELECT question, answer, category, sort_order, id
        FROM faq_items
        WHERE is_active = 1
        ORDER BY sort_order, id
    ''').fetchall()
    conn.close()

    from collections import OrderedDict

    def _format_category_label(key: str, display: str | None) -> str:
        if display:
            return display
        mapping = {
            'general': 'Общие вопросы',
            'events': 'Мероприятия',
            'profile': 'Профиль и настройки',
            'technical': 'Технические вопросы',
            'security': 'Безопасность и конфиденциальность',
        }
        return mapping.get(key, key.replace('_', ' ').title())

    sections = OrderedDict()
    for row in categories_rows:
        key = row['name']
        sections[key] = {
            'key': key,
            'display_name': _format_category_label(key, row['display_name']),
            'entries': []
        }

    for item in items_rows:
        key = (item['category'] or '').strip() or 'general'
        if key not in sections:
            sections[key] = {
                'key': key,
                'display_name': _format_category_label(key, None),
                'entries': []
            }
        sections[key]['entries'].append({
            'id': item['id'],
            'question': item['question'],
            'answer': item['answer']
        })

    faq_sections = [section for section in sections.values() if section['entries']]

    return render_template('faq.html', faq_sections=faq_sections)


@app.route('/rules')
def rules():
    """Страница с правилами"""
    try:
        import json
        rules_content = get_setting('rules_content', '')
        rules_items = []
        
        if rules_content:
            try:
                # Пытаемся распарсить как JSON
                rules_items = json.loads(rules_content)
                if not isinstance(rules_items, list):
                    rules_items = []
            except (json.JSONDecodeError, ValueError):
                # Старый формат HTML - оставляем как есть для обратной совместимости
                pass
        
        return render_template('rules.html', rules_content=rules_content, rules_items=rules_items)
    except Exception as e:
        log_error(f"Error in rules route: {e}")
        return render_template('rules.html', rules_content='', rules_items=[])
def contacts():
    """Страница контактов - показывает администраторов/модераторов и пользователей со званиями"""
    conn = get_db_connection()
    
    # Получаем пользователей с ролями администратора или модератора
    admins_moderators = conn.execute('''
        SELECT DISTINCT u.*, 
               GROUP_CONCAT(DISTINCT r.name) as roles_list
        FROM users u
        INNER JOIN user_roles ur ON u.user_id = ur.user_id
        INNER JOIN roles r ON ur.role_id = r.id
        WHERE r.name IN ('admin', 'moderator')
        GROUP BY u.user_id
        ORDER BY 
            CASE WHEN r.name = 'admin' THEN 1 ELSE 2 END,
            u.username
    ''').fetchall()
    
    # Получаем пользователей со званиями
    users_with_titles = conn.execute('''
        SELECT DISTINCT u.*
        FROM users u
        INNER JOIN user_titles ut ON u.user_id = ut.user_id
        WHERE u.user_id NOT IN (
            SELECT DISTINCT u2.user_id
            FROM users u2
            INNER JOIN user_roles ur2 ON u2.user_id = ur2.user_id
            INNER JOIN roles r2 ON ur2.role_id = r2.id
            WHERE r2.name IN ('admin', 'moderator')
        )
        GROUP BY u.user_id
        ORDER BY u.username
    ''').fetchall()
    
    # Получаем звания для пользователей со званиями
    users_with_titles_data = []
    for user in users_with_titles:
        user_dict = dict(user)
        user_titles = get_user_titles(user['user_id'])
        user_dict['titles'] = user_titles
        users_with_titles_data.append(user_dict)
    
    # Получаем роли для администраторов/модераторов
    admins_moderators_data = []
    for user in admins_moderators:
        user_dict = dict(user)
        user_roles = get_user_roles(user['user_id'])
        user_dict['roles'] = user_roles
        admins_moderators_data.append(user_dict)
    
    conn.close()
    
    return render_template('contacts.html', 
                           admins_moderators=admins_moderators_data,
                           users_with_titles=users_with_titles_data)

# ========== Логи ==========

@app.route('/admin/logs')
@require_role('admin')
def admin_logs():
    """Отображение действий пользователей."""
    limit = request.args.get('limit', type=int)
    user_filter = request.args.get('user_id', type=int)
    action_filter = request.args.get('action', '').strip()
    
    if not limit or limit <= 0:
        limit = 200
    limit = max(50, min(limit, 1000))
    
    conn = get_db_connection()
    params = []
    where_clauses = []
    
    if user_filter:
        where_clauses.append('user_id = ?')
        params.append(user_filter)
    
    if action_filter:
        where_clauses.append('action LIKE ?')
        params.append(f'%{action_filter}%')
    
    query = '''
        SELECT id, user_id, username, action, details, metadata, ip_address, created_at
        FROM activity_logs
    '''
    if where_clauses:
        query += ' WHERE ' + ' AND '.join(where_clauses)
    query += ' ORDER BY created_at DESC LIMIT ?'
    params.append(limit)
    
    rows = conn.execute(query, params).fetchall()
    conn.close()
    
    logs = []
    for row in rows:
        item = dict(row)
        metadata_value = item.get('metadata')
        if metadata_value:
            try:
                item['metadata'] = json.loads(metadata_value)
            except (json.JSONDecodeError, TypeError):
                item['metadata'] = metadata_value
        else:
            item['metadata'] = None
        logs.append(item)
    
    return render_template('admin/logs.html', logs=logs, limit=limit, user_filter=user_filter, action_filter=action_filter)

# ========== Управление наградами ==========
@app.route('/admin/awards')
@require_role('admin')
def admin_awards():
    """Список наград"""
    conn = get_db_connection()
    awards = conn.execute('''
        SELECT a.*, 
               COUNT(ua.id) as users_count,
               u.username as creator_name
        FROM awards a
        LEFT JOIN user_awards ua ON a.id = ua.award_id
        LEFT JOIN users u ON a.created_by = u.user_id
        GROUP BY a.id
        ORDER BY a.sort_order, a.created_at DESC
    ''').fetchall()
    conn.close()
    return render_template('admin/awards.html', awards=awards)

@app.route('/admin/awards/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_award_create():
    """Создание награды"""
    conn = get_db_connection()
    
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        icon = request.form.get('icon', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        image_file = request.files.get('image')
        selected_users = request.form.getlist('users')  # Получаем список выбранных пользователей
        
        if not title:
            flash('Заголовок награды обязателен', 'error')
            users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
            conn.close()
            return render_template('admin/award_form.html', users=users)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
        except ValueError:
            sort_order = 100
        
        # Обработка загрузки изображения
        image_path = None
        if image_file and image_file.filename:
            upload_dir = os.path.join(app.static_folder, 'uploads', 'awards')
            os.makedirs(upload_dir, exist_ok=True)
            
            # Проверяем расширение
            allowed_extensions = {'.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp'}
            file_ext = os.path.splitext(image_file.filename)[1].lower()
            if file_ext in allowed_extensions:
                filename = f"award_{int(datetime.now().timestamp())}{file_ext}"
                filepath = os.path.join(upload_dir, filename)
                image_file.save(filepath)
                image_path = f'/static/uploads/awards/{filename}'
        
        try:
            # Создаем награду
            cursor = conn.execute('''
                INSERT INTO awards (title, icon, image, sort_order, created_by)
                VALUES (?, ?, ?, ?, ?)
            ''', (title, icon, image_path, sort_order, session['user_id']))
            award_id = cursor.lastrowid
            
            # Присваиваем награду выбранным пользователям
            if selected_users:
                for user_id_str in selected_users:
                    try:
                        user_id = int(user_id_str)
                        assign_award(user_id, award_id, assigned_by=session['user_id'])
                    except ValueError:
                        continue
            
            conn.commit()
            flash('Награда успешно создана', 'success')
            conn.close()
            return redirect(url_for('admin_awards'))
        except Exception as e:
            log_error(f"Error creating award: {e}")
            flash(f'Ошибка создания награды: {str(e)}', 'error')
            conn.close()
    
    # GET запрос - получаем список пользователей
    users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
    conn.close()
    return render_template('admin/award_form.html', users=users)
@app.route('/admin/awards/<int:award_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_award_edit(award_id):
    """Редактирование награды"""
    conn = get_db_connection()
    award = conn.execute('SELECT * FROM awards WHERE id = ?', (award_id,)).fetchone()
    
    if not award:
        flash('Награда не найдена', 'error')
        conn.close()
        return redirect(url_for('admin_awards'))
    
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        icon = request.form.get('icon', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        image_file = request.files.get('image')
        delete_image = request.form.get('delete_image', '0')
        selected_users = request.form.getlist('users')  # Получаем список выбранных пользователей
        
        if not title:
            flash('Заголовок награды обязателен', 'error')
            users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
            # Получаем текущих пользователей с наградой
            current_users = conn.execute('''
                SELECT user_id FROM user_awards WHERE award_id = ?
            ''', (award_id,)).fetchall()
            current_user_ids = [u['user_id'] for u in current_users]
            conn.close()
            return render_template('admin/award_form.html', award=award, users=users, current_user_ids=current_user_ids)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
        except ValueError:
            sort_order = 100
        
        # Обработка загрузки/удаления изображения
        image_path = award['image']
        
        if delete_image == '1':
            # Удаляем старое изображение
            if image_path:
                old_filepath = os.path.join(app.static_folder, image_path.replace('/static/', ''))
                if os.path.exists(old_filepath):
                    try:
                        os.remove(old_filepath)
                    except Exception as e:
                        log_debug(f"Error deleting old image: {e}")
            image_path = None
        
        if image_file and image_file.filename:
            # Удаляем старое изображение при загрузке нового
            if image_path:
                old_filepath = os.path.join(app.static_folder, image_path.replace('/static/', ''))
                if os.path.exists(old_filepath):
                    try:
                        os.remove(old_filepath)
                    except Exception as e:
                        log_debug(f"Error deleting old image: {e}")
            
            upload_dir = os.path.join(app.static_folder, 'uploads', 'awards')
            os.makedirs(upload_dir, exist_ok=True)
            
            # Проверяем расширение
            allowed_extensions = {'.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp'}
            file_ext = os.path.splitext(image_file.filename)[1].lower()
            if file_ext in allowed_extensions:
                filename = f"award_{int(datetime.now().timestamp())}{file_ext}"
                filepath = os.path.join(upload_dir, filename)
                image_file.save(filepath)
                image_path = f'/static/uploads/awards/{filename}'
        
        try:
            # Обновляем награду
            conn.execute('''
                UPDATE awards SET title = ?, icon = ?, image = ?, sort_order = ?
                WHERE id = ?
            ''', (title, icon, image_path, sort_order, award_id))
            
            # Обновляем присвоение наград пользователям
            # Получаем текущих пользователей с наградой
            current_users = conn.execute('''
                SELECT user_id FROM user_awards WHERE award_id = ?
            ''', (award_id,)).fetchall()
            current_user_ids = {u['user_id'] for u in current_users}
            
            # Преобразуем selected_users в множество int
            selected_user_ids = set()
            for uid in selected_users:
                try:
                    selected_user_ids.add(int(uid))
                except (ValueError, TypeError):
                    continue
            
            assigned_by = session.get('user_id')
            
            # Добавляем новых пользователей
            for user_id in selected_user_ids:
                if user_id not in current_user_ids:
                    try:
                        conn.execute('''
                            INSERT OR REPLACE INTO user_awards (user_id, award_id, assigned_by)
                            VALUES (?, ?, ?)
                        ''', (user_id, award_id, assigned_by))
                    except Exception as e:
                        log_error(f"Error assigning award to user {user_id}: {e}")
            
            # Удаляем пользователей, которых больше нет в списке
            for user_id in current_user_ids:
                if user_id not in selected_user_ids:
                    try:
                        conn.execute('''
                            DELETE FROM user_awards
                            WHERE user_id = ? AND award_id = ?
                        ''', (user_id, award_id))
                    except Exception as e:
                        log_error(f"Error removing award from user {user_id}: {e}")
            
            conn.commit()
            flash('Награда успешно обновлена', 'success')
            conn.close()
            return redirect(url_for('admin_awards'))
        except Exception as e:
            log_error(f"Error updating award: {e}")
            flash(f'Ошибка обновления награды: {str(e)}', 'error')
            conn.close()
    
    # GET запрос - получаем список пользователей и текущих пользователей с наградой
    users = conn.execute('SELECT user_id, username FROM users ORDER BY username').fetchall()
    current_users = conn.execute('''
        SELECT user_id FROM user_awards WHERE award_id = ?
    ''', (award_id,)).fetchall()
    current_user_ids = [u['user_id'] for u in current_users]
    conn.close()
    return render_template('admin/award_form.html', award=award, users=users, current_user_ids=current_user_ids)

@app.route('/admin/awards/<int:award_id>/delete', methods=['POST'])
@require_role('admin')
def admin_award_delete(award_id):
    """Удаление награды"""
    conn = get_db_connection()
    award = conn.execute('SELECT * FROM awards WHERE id = ?', (award_id,)).fetchone()
    
    if not award:
        flash('Награда не найдена', 'error')
        conn.close()
        return redirect(url_for('admin_awards'))
    
    try:
        # Удаляем изображение если есть
        if award['image']:
            image_path = os.path.join(app.static_folder, award['image'].replace('/static/', ''))
            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                except Exception as e:
                    log_debug(f"Error deleting award image: {e}")
        
        conn.execute('DELETE FROM awards WHERE id = ?', (award_id,))
        conn.commit()
        flash('Награда успешно удалена', 'success')
    except Exception as e:
        log_error(f"Error deleting award: {e}")
        flash(f'Ошибка удаления награды: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin_awards'))

def get_events_requiring_review():
    """Получает список мероприятий, требующих модерации участников"""
    conn = get_db_connection()
    now = datetime.now()
    
    # Получаем мероприятия, где регистрация закрыта, но есть неутвержденные участники
    events = conn.execute('''
        SELECT DISTINCT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        INNER JOIN event_stages es ON e.id = es.event_id
        INNER JOIN event_registrations er ON e.id = er.event_id
        LEFT JOIN event_participant_approvals epa ON e.id = epa.event_id AND er.user_id = epa.user_id
        WHERE es.stage_type = 'registration_closed'
        AND es.start_datetime IS NOT NULL
        AND datetime(es.start_datetime) <= datetime(?)
        AND (epa.approved IS NULL OR epa.approved = 0)
        ORDER BY es.start_datetime DESC
    ''', (now,)).fetchall()
    
    conn.close()
    return events
@app.route('/admin/events')
@require_role('admin')
def admin_events():
    """Список мероприятий"""
    conn = get_db_connection()
    events = conn.execute('''
        SELECT e.*, u.username as creator_name,
               COUNT(es.id) as stages_count
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        LEFT JOIN event_stages es ON e.id = es.event_id
        GROUP BY e.id
        ORDER BY e.created_at DESC
    ''').fetchall()
    conn.close()
    
    events_with_info = []
    for event in events:
        event_dict = dict(event)
        current_stage = get_current_event_stage(event_dict['id'])
        event_dict['current_stage'] = current_stage
        event_dict['needs_review'] = False
        event_dict['review_pending_count'] = 0
        event_dict['review_approved_count'] = 0
        
        if current_stage and current_stage.get('info', {}).get('type') == 'registration_closed':
            event_id = event_dict['id']
            create_participant_approvals_for_event(event_id)
            conn_counts = get_db_connection()
            counts = conn_counts.execute('''
                SELECT 
                    SUM(CASE WHEN approved = 1 THEN 1 ELSE 0 END) as approved_count,
                    SUM(CASE WHEN approved IS NULL OR approved = 0 THEN 1 ELSE 0 END) as pending_count
                FROM event_participant_approvals
                WHERE event_id = ?
            ''', (event_id,)).fetchone()
            conn_counts.close()
            
            approved_count = counts['approved_count'] if counts and counts['approved_count'] else 0
            pending_count = counts['pending_count'] if counts and counts['pending_count'] else 0
            
            event_dict['needs_review'] = True
            event_dict['review_pending_count'] = pending_count
            event_dict['review_approved_count'] = approved_count
        
        events_with_info.append(event_dict)
    
    return render_template('admin/events.html', events=events_with_info)

@app.route('/admin/events/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_event_create():
    """Создание мероприятия"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        award_id = request.form.get('award_id', '').strip()
        award_id = int(award_id) if award_id else None
        
        if not name:
            flash('Название мероприятия обязательно', 'error')
            conn = get_db_connection()
            awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
            conn.close()
            return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES, awards=awards)
        
        conn = get_db_connection()
        try:
            # Создаем мероприятие
            cursor = conn.execute('''
                INSERT INTO events (name, description, created_by, award_id)
                VALUES (?, ?, ?, ?)
            ''', (name, description, session.get('user_id'), award_id))
            event_id = cursor.lastrowid
            
            # Создаем этапы
            stage_order = 1
            for stage in EVENT_STAGES:
                start_datetime = None
                end_datetime = None
                
                if stage['has_start']:
                    start_str = request.form.get(f"stage_{stage['type']}_start", '').strip()
                    if start_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in start_str:
                                if len(start_str) == 16:  # YYYY-MM-DDTHH:MM
                                    start_datetime = datetime.strptime(start_str, '%Y-%m-%dT%H:%M')
                                elif len(start_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    start_datetime = datetime.strptime(start_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                start_datetime = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты начала этапа {stage['type']}: {e}, строка: {start_str}")
                            pass
                
                if stage['has_end']:
                    end_str = request.form.get(f"stage_{stage['type']}_end", '').strip()
                    if end_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in end_str:
                                if len(end_str) == 16:  # YYYY-MM-DDTHH:MM
                                    end_datetime = datetime.strptime(end_str, '%Y-%m-%dT%H:%M')
                                elif len(end_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    end_datetime = datetime.strptime(end_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                end_datetime = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты окончания этапа {stage['type']}: {e}, строка: {end_str}")
                            pass
                
                # Проверяем обязательность
                is_required = 1 if stage['required'] else 0
                is_optional = 1 if not stage['required'] else 0
                
                # Для обязательных этапов проверяем наличие даты начала
                if stage['required'] and stage['has_start'] and not start_datetime:
                    flash(f'Дата начала этапа "{stage["name"]}" обязательна', 'error')
                    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES, awards=awards)
                
                # Форматируем datetime для сохранения в БД
                start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
                end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None
                
                log_debug(f"Создание этапа {stage['type']}: start={start_datetime_str}, end={end_datetime_str}")
                
                conn.execute('''
                    INSERT INTO event_stages 
                    (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (event_id, stage['type'], stage_order, start_datetime_str, end_datetime_str, is_required, is_optional))
                stage_order += 1
            
            conn.commit()
            flash('Мероприятие успешно создано', 'success')
            conn.close()
            return redirect(url_for('admin_events'))
        except Exception as e:
            log_error(f"Error creating event: {e}")
            flash(f'Ошибка создания мероприятия: {str(e)}', 'error')
            conn.rollback()
            conn.close()
    
    # GET запрос - получаем список наград
    conn = get_db_connection()
    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
    conn.close()
    return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES, awards=awards)
@app.route('/admin/events/<int:event_id>')
@require_role('admin')
def admin_event_view(event_id):
    """Просмотр мероприятия"""
    conn = get_db_connection()
    event = conn.execute('SELECT * FROM events WHERE id = ?', (event_id,)).fetchone()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        conn.close()
        return redirect(url_for('admin_events'))
    
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    
    conn.close()
    
    # Сопоставляем этапы с их типами
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    stages_with_info = []
    for stage_info in EVENT_STAGES:
        stage_data = stages_dict.get(stage_info['type'], None)
        stages_with_info.append({
            'info': stage_info,
            'data': stage_data
        })
    
    # Определяем текущий этап для отображения кнопки ревью
    current_stage = get_current_event_stage(event_id)
    
    # Преобразуем event в словарь для корректной работы в шаблоне
    event_dict = dict(event) if event else {}
    
    return render_template('admin/event_view.html', event=event_dict, stages_with_info=stages_with_info, current_stage=current_stage)


@app.route('/admin/events/<int:event_id>/participants')
@require_role('admin')
def admin_event_participants(event_id):
    """Детальный список участников мероприятия для администраторов"""
    conn = get_db_connection()
    event = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.id = ?
    ''', (event_id,)).fetchone()

    if not event:
        conn.close()
        flash('Мероприятие не найдено', 'error')
        return redirect(url_for('admin_events'))

    stages = conn.execute('''
        SELECT stage_type, start_datetime
        FROM event_stages
        WHERE event_id = ?
    ''', (event_id,)).fetchall()

    participants = conn.execute('''
        SELECT 
            er.user_id,
            er.registered_at,
            COALESCE(d.last_name, u.last_name) AS last_name,
            COALESCE(d.first_name, u.first_name) AS first_name,
            COALESCE(d.middle_name, u.middle_name) AS middle_name,
            COALESCE(d.postal_code, u.postal_code) AS postal_code,
            COALESCE(d.country, u.country) AS country,
            COALESCE(d.city, u.city) AS city,
            COALESCE(d.street, u.street) AS street,
            COALESCE(d.house, u.house) AS house,
            COALESCE(d.building, u.building) AS building,
            COALESCE(d.apartment, u.apartment) AS apartment,
            COALESCE(d.phone, u.phone) AS phone,
            COALESCE(d.telegram, u.telegram) AS telegram,
            COALESCE(d.whatsapp, u.whatsapp) AS whatsapp,
            COALESCE(d.viber, u.viber) AS viber,
            u.username,
            u.avatar_seed,
            u.avatar_style,
            u.email,
            epa.approved AS approval_flag,
            epa.notes AS approval_notes,
            epa.approved_at AS approval_timestamp,
            epa.approved_by AS approval_by
        FROM event_registrations er
        LEFT JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
        LEFT JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
        WHERE er.event_id = ?
        ORDER BY u.username COLLATE NOCASE
    ''', (event_id,)).fetchall()
    conn.close()

    stage_times = {row['stage_type']: row['start_datetime'] for row in stages}

    def parse_dt(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            try:
                return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None

    pre_start = parse_dt(stage_times.get('pre_registration'))
    main_start = parse_dt(stage_times.get('main_registration'))
    registration_closed_start = parse_dt(stage_times.get('registration_closed'))

    participants_data = []
    for row in participants:
        registered_at_dt = parse_dt(row['registered_at'])

        stage_label = 'main'
        if pre_start and main_start and registered_at_dt:
            if registered_at_dt >= pre_start and registered_at_dt < main_start:
                stage_label = 'pre'
            else:
                stage_label = 'main'
        elif pre_start and registered_at_dt and not main_start:
            if registered_at_dt < pre_start:
                stage_label = 'pre'
        elif main_start and registered_at_dt:
            stage_label = 'pre' if registered_at_dt < main_start else 'main'

        if registration_closed_start and registered_at_dt and registered_at_dt >= registration_closed_start:
            stage_label = 'main'

        approval_flag = row['approval_flag']
        approval_timestamp = row['approval_timestamp']
        approval_status = 'pending'
        if approval_flag == 1:
            approval_status = 'approved'
        elif approval_flag == 0 and approval_timestamp:
            approval_status = 'rejected'

        approval_notes = row['approval_notes']

        participants_data.append({
            'user_id': row['user_id'],
            'username': row['username'] or f'ID {row["user_id"]}',
            'registered_at': row['registered_at'],
            'last_name': row['last_name'],
            'first_name': row['first_name'],
            'middle_name': row['middle_name'],
            'postal_code': row['postal_code'],
            'country': row['country'],
            'city': row['city'],
            'street': row['street'],
            'house': row['house'],
            'building': row['building'],
            'apartment': row['apartment'],
            'phone': row['phone'],
            'telegram': row['telegram'],
            'whatsapp': row['whatsapp'],
            'viber': row['viber'],
            'email': row['email'],
            'stage': stage_label,
            'can_upgrade_to_main': stage_label == 'pre',
            'can_downgrade_to_pre': stage_label == 'main',
            'approval_status': approval_status,
            'approval_notes': approval_notes,
            'can_confirm_participant': stage_label == 'main' and approval_status != 'approved',
            'can_reject_participant': stage_label == 'main' and approval_status != 'rejected'
        })

    pre_participants = [p for p in participants_data if p['stage'] == 'pre']
    main_participants = [p for p in participants_data if p['stage'] == 'main']
    positive_participants = [p for p in participants_data if p['approval_status'] == 'approved']
    negative_participants = [p for p in participants_data if p['approval_status'] == 'rejected']
    na_participants = [p for p in participants_data if p['stage'] == 'main' and p['approval_status'] == 'pending']

    return render_template(
        'admin/event_participants.html',
        event=event,
        participants_all=participants_data,
        participants_pre=pre_participants,
        participants_main=main_participants,
        participants_positive=positive_participants,
        participants_negative=negative_participants,
        participants_na=na_participants,
        participants_count=len(participants_data),
        participants_pre_count=len(pre_participants),
        participants_main_count=len(main_participants),
        participants_positive_count=len(positive_participants),
        participants_negative_count=len(negative_participants),
        participants_na_count=len(na_participants)
    )
@app.route('/admin/events/<int:event_id>/distribution/positive')
@require_role('admin')
def admin_event_distribution_positive_view(event_id):
    """Отображает участников со статусом 'Позитив' для распределения"""
    try:
        conn = get_db_connection()
        event = conn.execute('''
            SELECT e.*, u.username as creator_name
            FROM events e
            LEFT JOIN users u ON e.created_by = u.user_id
            WHERE e.id = ?
        ''', (event_id,)).fetchone()

        if not event:
            conn.close()
            flash('Мероприятие не найдено', 'error')
            return redirect(url_for('admin_events'))

        participants = conn.execute('''
            SELECT 
                er.user_id,
                u.username,
                u.last_name,
                u.first_name,
                u.middle_name,
                COALESCE(d.postal_code, u.postal_code) AS postal_code,
                COALESCE(d.country, u.country) AS country,
                COALESCE(d.city, u.city) AS city,
                COALESCE(d.street, u.street) AS street,
                COALESCE(d.house, u.house) AS house,
                COALESCE(d.building, u.building) AS building,
                COALESCE(d.apartment, u.apartment) AS apartment,
                COALESCE(d.phone, u.phone) AS phone,
                COALESCE(d.telegram, u.telegram) AS telegram,
                COALESCE(d.whatsapp, u.whatsapp) AS whatsapp,
                COALESCE(d.viber, u.viber) AS viber,
                epa.notes as approval_notes,
                er.registered_at
            FROM event_registrations er
            LEFT JOIN users u ON er.user_id = u.user_id
            LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
            INNER JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
            WHERE er.event_id = ?
              AND epa.approved = 1
            ORDER BY u.username COLLATE NOCASE
        ''', (event_id,)).fetchall()
        conn.close()

        participants_data = []
        participants_lookup = {}
        for row in participants:
            participant_dict = {
            'user_id': row['user_id'],
            'username': row['username'] or f'ID {row["user_id"]}',
            'last_name': row['last_name'],
            'first_name': row['first_name'],
            'middle_name': row['middle_name'],
            'address': {
                'postal_code': row['postal_code'],
                'country': row['country'],
                'city': row['city'],
                'street': row['street'],
                'house': row['house'],
                'building': row['building'],
                'apartment': row['apartment'],
            },
            'country': row['country'],
            'city': row['city'],
            'contacts': {
                'phone': row['phone'],
                'telegram': row['telegram'],
                'whatsapp': row['whatsapp'],
                'viber': row['viber'],
            },
            'notes': row['approval_notes'],
                'registered_at': row['registered_at'],
            }
            participants_data.append(participant_dict)
            participants_lookup[row['user_id']] = participant_dict

        conn_assignments = get_db_connection()
        # Загружаем пары и проверяем наличие сообщений от Деда Мороза для определения статуса отправки
        # Используем подзапрос для проверки наличия сообщений
        saved_rows = conn_assignments.execute('''
        SELECT 
            ea.santa_user_id, 
            ea.recipient_user_id, 
            ea.santa_sent_at, 
            ea.santa_send_info, 
            ea.recipient_received_at, 
            ea.locked, 
            ea.assignment_locked,
            ea.id as assignment_id,
            CASE 
                WHEN (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '') THEN 1
                WHEN EXISTS (
                    SELECT 1 FROM letter_messages lm 
                    WHERE lm.assignment_id = ea.id AND lm.sender = 'santa'
                ) THEN 1
                ELSE 0 
            END as has_sent_indicator
        FROM event_assignments ea
        WHERE ea.event_id = ?
            ORDER BY ea.assigned_at ASC, ea.id ASC
        ''', (event_id,)).fetchall()
        conn_assignments.close()

        saved_pairs = []
        locked_santas = set()
        for record in saved_rows:
            santa = participants_lookup.get(record['santa_user_id'])
            recipient = participants_lookup.get(record['recipient_user_id'])
            if not santa or not recipient:
                continue
            locked_flag = bool(record['locked'])
            assignment_locked_flag = bool(record['assignment_locked'])
            if assignment_locked_flag:
                locked_santas.add(record['santa_user_id'])
            # Если santa_sent_at пустой, но есть сообщение от Деда Мороза, считаем что подарок отправлен
            santa_sent_at = record['santa_sent_at']
            has_sent_indicator = bool(record['has_sent_indicator']) if 'has_sent_indicator' in record.keys() else False
            
            saved_pairs.append({
                'santa_id': santa['user_id'],
                'santa_name': santa['username'],
                'santa_country': santa.get('country'),
                'santa_city': santa.get('city'),
                'recipient_id': recipient['user_id'],
                'recipient_name': recipient['username'],
                'recipient_country': recipient.get('country'),
                'recipient_city': recipient.get('city'),
                'santa_sent_at': santa_sent_at if (santa_sent_at and santa_sent_at != '') else None,
                'santa_send_info': record['santa_send_info'] if 'santa_send_info' in record.keys() else None,
                'recipient_received_at': record['recipient_received_at'],
                'has_sent_indicator': has_sent_indicator,
                'locked': locked_flag,
                'assignment_locked': assignment_locked_flag
            })

        distribution_url = url_for('admin_event_distribution_positive_generate', event_id=event_id)
        distribution_save_url = url_for('admin_event_distribution_positive_save', event_id=event_id)

        return render_template(
            'admin/event_distribution.html',
            event=event,
            distribution_type='positive',
            participants=participants_data,
            participants_count=len(participants_data),
            distribution_generate_url=distribution_url,
            distribution_save_url=distribution_save_url,
            distribution_create_assignments_url=url_for('admin_event_distribution_positive_create_assignments', event_id=event_id),
            distribution_unassign_url=url_for('admin_event_distribution_positive_unassign', event_id=event_id),
            saved_pairs=saved_pairs,
            saved_locked_santas=list(locked_santas)
        )
    except Exception as e:
        log_error(f"Error in admin_event_distribution_positive_view for event {event_id}: {e}")
        log_error(f"Traceback: {traceback.format_exc()}")
        flash(f'Ошибка при загрузке страницы распределения: {str(e)}', 'error')
        return redirect(url_for('admin_event_view', event_id=event_id))

@app.route('/admin/events/<int:event_id>/distribution/positive/assignments', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_create_assignments(event_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходима авторизация'}), 403

    conn = get_db_connection()
    rows = conn.execute('''
        SELECT santa_user_id, recipient_user_id, santa_sent_at, recipient_received_at, locked
        FROM event_assignments
        WHERE event_id = ?
        ORDER BY assigned_at ASC, id ASC
    ''', (event_id,)).fetchall()
    conn.close()

    log_debug(f"create_assignments: event {event_id}, saved_pairs={len(rows)}")
    if not rows:
        return jsonify({'success': False, 'error': 'Нет сохранённого распределения для создания заданий'}), 400

    if any(not row['locked'] for row in rows):
        return jsonify({'success': False, 'error': 'Закрепите замком каждую пару перед созданием заданий.'}), 400

    assignments = [(row['santa_user_id'], row['recipient_user_id']) for row in rows]
    success, result = save_event_assignments(
        event_id,
        assignments,
        user_id,
        locked_pairs={(row['santa_user_id'], row['recipient_user_id']) for row in rows},
        assignment_locked=True
    )
    if success:
        log_debug(f"create_assignments: assignments locked for event {event_id}, count={result}")
        return jsonify({'success': True, 'message': f'Создано {result} заданий для участников.'})
    return jsonify({'success': False, 'error': result}), 500

@app.route('/admin/events/<int:event_id>/distribution/positive/unassign', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_unassign(event_id):
    data = request.get_json(silent=True) or {}
    santa_id = data.get('santa_id')
    try:
        santa_id = int(santa_id)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Некорректный идентификатор Деда Мороза'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.execute('''
            DELETE FROM event_assignments
            WHERE event_id = ? AND santa_user_id = ?
        ''', (event_id, santa_id))
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Задание для выбранного участника не найдено'}), 404
        conn.commit()
        log_activity(
            'assignment_removed',
            details=f'Задание отменено для мероприятия #{event_id} (Дед Мороз #{santa_id})',
            metadata={'event_id': event_id, 'santa_user_id': santa_id}
        )
        return jsonify({'success': True, 'message': 'Задание отменено. Пара снова доступна для редактирования.'})
    except Exception as e:
        conn.rollback()
        log_error(f"Error removing assignment for event {event_id}, santa {santa_id}: {e}")
        return jsonify({'success': False, 'error': 'Не удалось отменить задание'}), 500
    finally:
        conn.close()
@app.route('/admin/events/<int:event_id>/distribution/positive/random', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_generate(event_id):
    request_data = request.get_json(silent=True) or {}
    group_by_country = bool(request_data.get('group_by_country'))
    locked_pairs_raw = request_data.get('locked_pairs') or []
    assignment_locked_santas_raw = request_data.get('assignment_locked_santas') or []
    conn = get_db_connection()
    participants = conn.execute('''
        SELECT 
            er.user_id,
            u.username,
            COALESCE(d.country, u.country) AS country,
            COALESCE(d.city, u.city) AS city
        FROM event_registrations er
        LEFT JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
        LEFT JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
        WHERE er.event_id = ?
          AND epa.approved = 1
        ORDER BY u.username COLLATE NOCASE
    ''', (event_id,)).fetchall()
    conn.close()

    if not participants or len(participants) < 2:
        return jsonify({'success': False, 'error': 'Недостаточно участников для распределения'}), 400

    participants_map = {
        row['user_id']: {
            'name': row['username'] or f'ID {row["user_id"]}',
            'country': row['country'],
            'city': row['city'],
        }
        for row in participants
    }

    user_ids = [row['user_id'] for row in participants]

    locked_assignments = {}
    locked_recipient_ids = set()
    try:
        for entry in locked_pairs_raw:
            santa_id_raw = entry.get('santa_id')
            recipient_id_raw = entry.get('recipient_id')
            santa_id = int(santa_id_raw)
            recipient_id = int(recipient_id_raw)
            if santa_id == recipient_id:
                return jsonify({'success': False, 'error': 'Закреплённая пара не может совпадать с самим собой'}), 400
            if santa_id not in participants_map or recipient_id not in participants_map:
                return jsonify({'success': False, 'error': 'Закреплённая пара содержит неизвестного участника'}), 400
            if group_by_country:
                santa_country = participants_map[santa_id].get('country')
                recipient_country = participants_map[recipient_id].get('country')
                if santa_country and recipient_country and santa_country != recipient_country:
                    return jsonify({'success': False, 'error': 'Закреплённая пара нарушает правило «По странам»'}), 400
            if santa_id in locked_assignments:
                return jsonify({'success': False, 'error': 'Каждый Дед Мороз может быть закреплён только один раз'}), 400
            if recipient_id in locked_recipient_ids:
                return jsonify({'success': False, 'error': 'Получатель уже закреплён в другой паре'}), 400
            locked_assignments[santa_id] = recipient_id
            locked_recipient_ids.add(recipient_id)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Некорректные данные закреплённых пар'}), 400

    assignment_locked_santas = set()
    try:
        assignment_locked_santas = {int(santa_id) for santa_id in assignment_locked_santas_raw}
    except (TypeError, ValueError):
        assignment_locked_santas = set()

    def is_valid_pair(santa_id, recipient_id, require_same_country: bool):
        if santa_id == recipient_id:
            return False
        if require_same_country:
            santa_country = participants_map.get(santa_id, {}).get('country')
            recipient_country = participants_map.get(recipient_id, {}).get('country')
            if santa_country and recipient_country and santa_country != recipient_country:
                return False
        return True

    used_santas = set(locked_assignments.keys())
    used_recipients = set(locked_assignments.values())

    remaining_candidate_santas = [sid for sid in user_ids if sid not in used_santas]
    random.shuffle(remaining_candidate_santas)

    recipients_by_country = defaultdict(list)
    for rid in user_ids:
        if rid in used_recipients:
            continue
        country = participants_map.get(rid, {}).get('country')
        recipients_by_country[country].append(rid)

    for country_list in recipients_by_country.values():
        random.shuffle(country_list)

    same_country_pairs = []
    if group_by_country:
        for santa_id in remaining_candidate_santas:
            if santa_id in used_santas:
                continue
            country = participants_map.get(santa_id, {}).get('country')
            candidates = recipients_by_country.get(country)
            if not candidates:
                continue
            recipient_id = None
            for idx, candidate in enumerate(candidates):
                if candidate != santa_id:
                    recipient_id = candidates.pop(idx)
                    break
            if recipient_id is None:
                continue
            same_country_pairs.append((santa_id, recipient_id))
            used_santas.add(santa_id)
            used_recipients.add(recipient_id)
            if not candidates:
                recipients_by_country.pop(country, None)

    remaining_santas = [sid for sid in user_ids if sid not in used_santas]
    available_recipients = [rid for rid in user_ids if rid not in used_recipients]

    if len(remaining_santas) != len(available_recipients):
        return jsonify({'success': False, 'error': 'Количество доступных Дедов Морозов и получателей не совпадает. Проверьте закреплённые пары.'}), 400

    def try_assignments(rem_santas, rem_recipients, require_same_country: bool, attempts: int = 3000):
        if len(rem_santas) != len(rem_recipients):
            return None
        rem_santas = rem_santas[:]
        rem_recipients = rem_recipients[:]
        for _ in range(attempts):
            random.shuffle(rem_santas)
            random.shuffle(rem_recipients)
            valid = True
            for santa_id, recipient_id in zip(rem_santas, rem_recipients):
                if not is_valid_pair(santa_id, recipient_id, require_same_country):
                    valid = False
                    break
            if valid:
                return list(zip(rem_santas, rem_recipients))
        return None

    extra_pairs = []
    if remaining_santas:
        if group_by_country:
            extra_pairs = try_assignments(remaining_santas, available_recipients, True)
            if not extra_pairs:
                extra_pairs = try_assignments(remaining_santas, available_recipients, False)
        else:
            extra_pairs = try_assignments(remaining_santas, available_recipients, False)

        if extra_pairs is None:
            error_message = 'Не удалось сформировать уникальные пары, попробуйте снова'
            if locked_assignments:
                error_message += ' Убедитесь, что закреплённые пары не блокируют распределение.'
            return jsonify({'success': False, 'error': error_message}), 500
    else:
        extra_pairs = []

    assignment_pairs = list(locked_assignments.items()) + same_country_pairs + extra_pairs

    # Проверяем, что все участники включены в распределение
    all_santa_ids = set(pair[0] for pair in assignment_pairs)
    all_recipient_ids = set(pair[1] for pair in assignment_pairs)
    all_participant_ids = set(user_ids)
    
    missing_santas = all_participant_ids - all_santa_ids
    missing_recipients = all_participant_ids - all_recipient_ids
    
    log_debug(f"admin_event_distribution_positive_generate: event_id={event_id}, total_participants={len(user_ids)}, "
              f"generated_pairs={len(assignment_pairs)}, locked={len(locked_assignments)}, "
              f"same_country={len(same_country_pairs)}, extra={len(extra_pairs)}")
    
    if missing_santas or missing_recipients:
        log_error(f"admin_event_distribution_positive_generate: Missing participants! "
                 f"Missing santas: {sorted(missing_santas)}, Missing recipients: {sorted(missing_recipients)}")
        return jsonify({
            'success': False, 
            'error': f'Не удалось создать распределение для всех участников. Отсутствует Дедов Морозов: {len(missing_santas)}, отсутствует получателей: {len(missing_recipients)}'
        }), 500

    if len(assignment_pairs) != len(user_ids):
        log_error(f"admin_event_distribution_positive_generate: Pair count mismatch! "
                 f"Expected: {len(user_ids)}, Got: {len(assignment_pairs)}")
        return jsonify({
            'success': False, 
            'error': f'Количество созданных пар ({len(assignment_pairs)}) не соответствует количеству участников ({len(user_ids)})'
        }), 500

    assignment_pairs.sort(key=lambda pair: participants_map[pair[0]]['name'] or '')

    # Загружаем существующие данные об отправке для сохранения при генерации
    # Учитываем как явные отметки (santa_sent_at), так и сообщения от Деда Мороза
    conn_existing = get_db_connection()
    existing_assignments = conn_existing.execute('''
        SELECT 
            ea.santa_user_id, 
            ea.recipient_user_id, 
            ea.santa_sent_at, 
            ea.santa_send_info, 
            ea.recipient_received_at,
            CASE 
                WHEN (ea.santa_sent_at IS NOT NULL AND ea.santa_sent_at != '') THEN 1
                WHEN EXISTS (
                    SELECT 1 FROM letter_messages lm 
                    WHERE lm.assignment_id = ea.id AND lm.sender = 'santa'
                ) THEN 1
                ELSE 0 
            END as has_sent_indicator
        FROM event_assignments ea
        WHERE ea.event_id = ?
    ''', (event_id,)).fetchall()
    conn_existing.close()
    
    existing_data_map = {}
    for row in existing_assignments:
        key = (row['santa_user_id'], row['recipient_user_id'])
        existing_data_map[key] = {
            'santa_sent_at': row['santa_sent_at'],
            'santa_send_info': row['santa_send_info'] if 'santa_send_info' in row.keys() else None,
            'recipient_received_at': row['recipient_received_at'],
            'has_sent_indicator': bool(row['has_sent_indicator']) if 'has_sent_indicator' in row.keys() else False
        }

    pairs = []
    for santa_id, recipient_id in assignment_pairs:
        santa_meta = participants_map.get(santa_id, {})
        recipient_meta = participants_map.get(recipient_id, {})
        
        # Проверяем, есть ли существующие данные об отправке для этой пары
        existing_key = (santa_id, recipient_id)
        existing_data = existing_data_map.get(existing_key, {})
        
        pairs.append({
            'santa_id': santa_id,
            'santa_name': santa_meta.get('name'),
            'santa_country': santa_meta.get('country'),
            'santa_city': santa_meta.get('city'),
            'recipient_id': recipient_id,
            'recipient_name': recipient_meta.get('name'),
            'recipient_country': recipient_meta.get('country'),
            'recipient_city': recipient_meta.get('city'),
            'santa_sent_at': existing_data.get('santa_sent_at') if (existing_data.get('santa_sent_at') and existing_data.get('santa_sent_at') != '') else None,
            'santa_send_info': existing_data.get('santa_send_info'),
            'recipient_received_at': existing_data.get('recipient_received_at'),
            'has_sent_indicator': existing_data.get('has_sent_indicator', False),
            'locked': santa_id in locked_assignments,
            'assignment_locked': santa_id in assignment_locked_santas
        })

    country_mode_applied = False
    if group_by_country:
        country_mode_applied = all(
            (participants_map.get(santa_id, {}).get('country') is None or
             participants_map.get(recipient_id, {}).get('country') is None or
             participants_map.get(santa_id, {}).get('country') == participants_map.get(recipient_id, {}).get('country'))
            for santa_id, recipient_id in assignment_pairs
        )

    log_debug(f"admin_event_distribution_positive_generate: Successfully generated {len(pairs)} pairs")
    return jsonify({'success': True, 'pairs': pairs, 'country_mode_applied': country_mode_applied})

@app.route('/admin/events/<int:event_id>/participants/add', methods=['POST'])
@require_role('admin')
def admin_event_participant_add(event_id):
    """Позволяет администратору добавить участника вручную"""
    identifier = request.form.get('user_identifier', '').strip()
    note = request.form.get('notes', '').strip()
    stage_choice = request.form.get('stage', 'main')

    if not identifier:
        flash('Укажите ID или имя пользователя', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        event = conn.execute('SELECT id, name FROM events WHERE id = ?', (event_id,)).fetchone()
        if not event:
            conn.close()
            flash('Мероприятие не найдено', 'error')
            return redirect(url_for('admin_events'))

        user = None
        if identifier.isdigit():
            user = conn.execute('SELECT * FROM users WHERE user_id = ?', (int(identifier),)).fetchone()
        if not user:
            user = conn.execute('SELECT * FROM users WHERE LOWER(username) = ?', (identifier.lower(),)).fetchone()

        if not user:
            conn.close()
            flash('Пользователь не найден', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        existing = conn.execute('''
            SELECT 1 FROM event_registrations WHERE event_id = ? AND user_id = ?
        ''', (event_id, user['user_id'])).fetchone()
        if existing:
            conn.close()
            flash('Этот пользователь уже участвует в мероприятии', 'info')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        pre_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'pre_registration'
        ''', (event_id,)).fetchone()
        main_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'main_registration'
        ''', (event_id,)).fetchone()

        stage_choice = stage_choice if stage_choice in ('pre', 'main') else 'main'

        target_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if stage_choice == 'pre':
            if pre_stage and pre_stage['start_datetime']:
                target_datetime = pre_stage['start_datetime']
        else:
            if main_stage and main_stage['start_datetime']:
                target_datetime = main_stage['start_datetime']

        conn.execute('''
            INSERT INTO event_registrations (event_id, user_id)
            VALUES (?, ?)
        ''', (event_id, user['user_id']))

        profile = {key: (user[key] or '').strip() if isinstance(user[key], str) else user[key]
                   for key in user.keys()}

        conn.execute('''
            INSERT INTO event_registration_details (
                event_id, user_id, last_name, first_name, middle_name,
                postal_code, country, city, street, house, building, apartment,
                phone, telegram, whatsapp, viber
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                last_name = excluded.last_name,
                first_name = excluded.first_name,
                middle_name = excluded.middle_name,
                postal_code = excluded.postal_code,
                country = excluded.country,
                city = excluded.city,
                street = excluded.street,
                house = excluded.house,
                building = excluded.building,
                apartment = excluded.apartment,
                phone = excluded.phone,
                telegram = excluded.telegram,
                whatsapp = excluded.whatsapp,
                viber = excluded.viber,
                updated_at = CURRENT_TIMESTAMP
        ''', (
            event_id,
            user['user_id'],
            profile.get('last_name'),
            profile.get('first_name'),
            profile.get('middle_name'),
            profile.get('postal_code'),
            profile.get('country'),
            profile.get('city'),
            profile.get('street'),
            profile.get('house'),
            profile.get('building'),
            profile.get('apartment'),
            profile.get('phone'),
            profile.get('telegram'),
            profile.get('whatsapp'),
            profile.get('viber')
        ))

        conn.execute('''
            UPDATE event_registrations
            SET registered_at = ?
            WHERE event_id = ? AND user_id = ?
        ''', (target_datetime, event_id, user['user_id']))

        approval_note = note or 'Добавлен администратором вручную'
        conn.execute('''
            INSERT INTO event_participant_approvals (event_id, user_id, approved, approved_at, approved_by, notes)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                approved = 1,
                approved_at = CURRENT_TIMESTAMP,
                approved_by = excluded.approved_by,
                notes = excluded.notes
        ''', (event_id, user['user_id'], session.get('user_id'), approval_note))

        conn.commit()
        conn.close()

        log_activity(
            'admin_event_add_participant',
            details=f'Пользователь {user["username"]} (ID {user["user_id"]}) добавлен в мероприятие {event["name"]}',
            metadata={'event_id': event_id, 'target_user_id': user['user_id'], 'notes': approval_note}
        )
        flash('Участник успешно добавлен', 'success')
    except sqlite3.IntegrityError:
        conn.rollback()
        conn.close()
        flash('Не удалось добавить участника: данные противоречат существующим записям', 'error')
    except Exception as exc:
        conn.rollback()
        conn.close()
        log_error(f"Ошибка ручного добавления участника: {exc}")
        flash('Не удалось добавить участника', 'error')

    return redirect(url_for('admin_event_participants', event_id=event_id))


@app.route('/admin/events/<int:event_id>/participants/upgrade', methods=['POST'])
@require_role('admin')
def admin_event_participant_upgrade(event_id):
    """Переводит участника из предварительной регистрации в основную"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        main_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'main_registration'
        ''', (event_id,)).fetchone()

        if not main_stage or not main_stage['start_datetime']:
            conn.close()
            flash('Этап основной регистрации не настроен', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        conn.execute('''
            UPDATE event_registrations
            SET registered_at = ?
            WHERE event_id = ? AND user_id = ?
        ''', (main_stage['start_datetime'], event_id, user_id))
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_upgrade_participant',
            details=f'Пользователь #{user_id} переведён в основную регистрацию мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id}
        )
        flash('Участник переведён в основную регистрацию', 'success')
    except Exception as exc:
        conn.rollback()
        conn.close()
        log_error(f"Ошибка перевода участника в основную регистрацию: {exc}")
        flash('Не удалось обновить участника', 'error')

    return redirect(url_for('admin_event_participants', event_id=event_id))
@app.route('/admin/events/<int:event_id>/participants/downgrade', methods=['POST'])
@require_role('admin')
def admin_event_participant_downgrade(event_id):
    """Переводит участника из основной регистрации в предварительную"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        pre_stage = conn.execute('''
            SELECT start_datetime
            FROM event_stages
            WHERE event_id = ? AND stage_type = 'pre_registration'
        ''', (event_id,)).fetchone()

        target_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if pre_stage and pre_stage['start_datetime']:
            target_datetime = pre_stage['start_datetime']

        conn.execute('''
            UPDATE event_registrations
            SET registered_at = ?
            WHERE event_id = ? AND user_id = ?
        ''', (target_datetime, event_id, user_id))
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_downgrade_participant',
            details=f'Пользователь #{user_id} переведён в предварительную регистрацию мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id}
        )
        flash('Участник переведён в предварительную регистрацию', 'success')
    except Exception as exc:
        conn.rollback()
        conn.close()
        log_error(f"Ошибка перевода участника в предварительную регистрацию: {exc}")
        flash('Не удалось обновить участника', 'error')

    return redirect(url_for('admin_event_participants', event_id=event_id))
@app.route('/admin/events/<int:event_id>/participants/remove', methods=['POST'])
@require_role('admin')
def admin_event_participant_remove(event_id):
    """Удаление участника из мероприятия"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        conn.execute('BEGIN')

        conn.execute('DELETE FROM event_registrations WHERE event_id = ? AND user_id = ?', (event_id, user_id))
        conn.execute('DELETE FROM event_registration_details WHERE event_id = ? AND user_id = ?', (event_id, user_id))
        conn.execute('DELETE FROM event_participant_approvals WHERE event_id = ? AND user_id = ?', (event_id, user_id))
        conn.execute('DELETE FROM event_assignments WHERE event_id = ? AND (santa_user_id = ? OR recipient_user_id = ?)', (event_id, user_id, user_id))

        conn.commit()
        conn.close()

        log_activity(
            'admin_event_remove_participant',
            details=f'Пользователь #{user_id} удалён из мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id}
        )
        flash('Участник удалён из мероприятия', 'success')
    except Exception as exc:
        try:
            conn.rollback()
        finally:
            conn.close()
        log_error(f"Ошибка удаления участника из мероприятия: {exc}")
        flash('Не удалось удалить участника', 'error')

    return redirect(url_for('admin_event_participants', event_id=event_id))


@app.route('/admin/events/<int:event_id>/participants/confirm', methods=['POST'])
@require_role('admin')
def admin_event_participant_confirm(event_id):
    """Подтверждение участия"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Некорректный участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id_int)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        stages = conn.execute('''
            SELECT stage_type, start_datetime
            FROM event_stages
            WHERE event_id = ?
        ''', (event_id,)).fetchall()

        def parse_dt(value):
            if not value:
                return None
            try:
                return datetime.fromisoformat(str(value))
            except ValueError:
                try:
                    return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return None

        pre_start = None
        main_start = None
        registration_closed_start = None
        for stage in stages:
            if stage['stage_type'] == 'pre_registration':
                pre_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'main_registration':
                main_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'registration_closed':
                registration_closed_start = parse_dt(stage['start_datetime'])

        registered_at_dt = parse_dt(registration['registered_at'])

        stage_label = 'main'
        if pre_start and main_start and registered_at_dt:
            if registered_at_dt >= pre_start and registered_at_dt < main_start:
                stage_label = 'pre'
        elif pre_start and registered_at_dt and not main_start:
            if registered_at_dt < pre_start:
                stage_label = 'pre'
        elif main_start and registered_at_dt:
            stage_label = 'pre' if registered_at_dt < main_start else 'main'

        if registration_closed_start and registered_at_dt and registered_at_dt >= registration_closed_start:
            stage_label = 'main'

        if stage_label != 'main' and main_start:
            conn.execute('''
                UPDATE event_registrations
                SET registered_at = ?
                WHERE event_id = ? AND user_id = ?
            ''', (main_start.strftime('%Y-%m-%d %H:%M:%S'), event_id, user_id_int))
            registered_at_dt = main_start
            stage_label = 'main'

        conn.execute('''
            INSERT INTO event_participant_approvals (event_id, user_id, approved, approved_at, approved_by, notes)
            VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?, NULL)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                approved = 1,
                approved_at = CURRENT_TIMESTAMP,
                approved_by = excluded.approved_by,
                notes = NULL
        ''', (event_id, user_id_int, session.get('user_id')))
        _set_review_penalty(conn, event_id, user_id_int, apply_penalty=False)
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_confirm_participant',
            details=f'Пользователь #{user_id_int} подтвержден для мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id_int}
        )
        flash('Участник подтвержден', 'success')
    except Exception as exc:
        try:
            conn.rollback()
        finally:
            conn.close()
        log_error(f"Ошибка подтверждения участника: {exc}")
        flash('Не удалось подтвердить участника', 'error')

    return redirect(url_for('admin_event_participants', event_id=event_id))
@app.route('/admin/events/<int:event_id>/participants/reject', methods=['POST'])
@require_role('admin')
def admin_event_participant_reject(event_id):
    """Отказ в участии"""
    user_id = request.form.get('user_id')
    if not user_id:
        flash('Не указан участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Некорректный участник', 'error')
        return redirect(url_for('admin_event_participants', event_id=event_id))

    reason = request.form.get('reason', '').strip()

    conn = get_db_connection()
    try:
        registration = conn.execute('''
            SELECT registered_at FROM event_registrations
            WHERE event_id = ? AND user_id = ?
        ''', (event_id, user_id_int)).fetchone()

        if not registration:
            conn.close()
            flash('Участник не найден в списке зарегистрированных', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        stages = conn.execute('''
            SELECT stage_type, start_datetime
            FROM event_stages
            WHERE event_id = ?
        ''', (event_id,)).fetchall()

        def parse_dt(value):
            if not value:
                return None
            try:
                return datetime.fromisoformat(str(value))
            except ValueError:
                try:
                    return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return None

        pre_start = None
        main_start = None
        registration_closed_start = None
        for stage in stages:
            if stage['stage_type'] == 'pre_registration':
                pre_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'main_registration':
                main_start = parse_dt(stage['start_datetime'])
            elif stage['stage_type'] == 'registration_closed':
                registration_closed_start = parse_dt(stage['start_datetime'])

        registered_at_dt = parse_dt(registration['registered_at'])

        stage_label = 'main'
        if pre_start and main_start and registered_at_dt:
            if registered_at_dt < main_start:
                stage_label = 'pre'
        elif pre_start and registered_at_dt and not main_start:
            if registered_at_dt < pre_start:
                stage_label = 'pre'
        elif main_start and registered_at_dt:
            stage_label = 'pre' if registered_at_dt < main_start else 'main'

        if registration_closed_start and registered_at_dt and registered_at_dt >= registration_closed_start:
            stage_label = 'main'

        if stage_label != 'main':
            conn.close()
            flash('Отказ возможен только для основной регистрации', 'error')
            return redirect(url_for('admin_event_participants', event_id=event_id))

        conn.execute('''
            INSERT INTO event_participant_approvals (event_id, user_id, approved, approved_at, approved_by, notes)
            VALUES (?, ?, 0, CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                approved = 0,
                approved_at = CURRENT_TIMESTAMP,
                approved_by = excluded.approved_by,
                notes = excluded.notes
        ''', (event_id, user_id_int, session.get('user_id'), reason or None))
        _set_review_penalty(conn, event_id, user_id_int, apply_penalty=True)
        conn.commit()
        conn.close()

        log_activity(
            'admin_event_reject_participant',
            details=f'Пользователь #{user_id_int} отклонен для мероприятия #{event_id}',
            metadata={'event_id': event_id, 'target_user_id': user_id_int, 'reason': reason}
        )
        flash('Участнику отказано в участии', 'success')
    except Exception as exc:
        try:
            conn.rollback()
        finally:
            conn.close()
        log_error(f"Ошибка отклонения участника: {exc}")
        flash('Не удалось отказать участнику', 'error')

    return redirect(url_for('admin_event_participants', event_id=event_id))

@app.route('/admin/events/<int:event_id>/distribution/positive/save', methods=['POST'])
@require_role('admin')
def admin_event_distribution_positive_save(event_id):
    data = request.get_json(silent=True) or {}
    pairs = data.get('pairs')
    
    log_debug(f"admin_event_distribution_positive_save: event_id={event_id}, pairs type={type(pairs)}, pairs length={len(pairs) if pairs else 0}")
    
    if not pairs or not isinstance(pairs, list):
        log_error(f"admin_event_distribution_positive_save: Invalid pairs data. Type: {type(pairs)}, Value: {pairs}")
        return jsonify({'success': False, 'error': 'Некорректные данные распределения'}), 400

    enforce_country = bool(data.get('enforce_country'))

    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходима авторизация'}), 403

    conn = get_db_connection()
    # Используем ту же логику, что и в генерации: только зарегистрированные И утвержденные участники
    approved_rows = conn.execute('''
        SELECT 
            er.user_id, 
            COALESCE(d.country, u.country) AS country
        FROM event_registrations er
        LEFT JOIN event_participant_approvals epa ON epa.event_id = er.event_id AND epa.user_id = er.user_id
        JOIN users u ON er.user_id = u.user_id
        LEFT JOIN event_registration_details d ON d.event_id = er.event_id AND d.user_id = er.user_id
        WHERE er.event_id = ?
          AND epa.approved = 1
    ''', (event_id,)).fetchall()
    approved_ids = {row['user_id'] for row in approved_rows}
    country_lookup = {row['user_id']: row['country'] for row in approved_rows}
    conn.close()

    log_debug(f"admin_event_distribution_positive_save: approved_ids count={len(approved_ids)}, approved_ids={sorted(approved_ids)}")

    if len(approved_ids) < 2:
        log_error(f"admin_event_distribution_positive_save: Not enough approved participants. Count: {len(approved_ids)}")
        return jsonify({'success': False, 'error': 'Недостаточно утверждённых участников для сохранения распределения'}), 400

    assignments = []
    santas_seen = set()
    recipients_seen = set()

    try:
        for idx, entry in enumerate(pairs):
            if not isinstance(entry, dict):
                log_error(f"admin_event_distribution_positive_save: Entry {idx} is not a dict: {entry}")
                return jsonify({'success': False, 'error': f'Некорректный формат пары #{idx + 1}'}), 400
            
            santa_id_raw = entry.get('santa_id')
            recipient_id_raw = entry.get('recipient_id')
            
            if santa_id_raw is None or recipient_id_raw is None:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} missing IDs: santa_id={santa_id_raw}, recipient_id={recipient_id_raw}")
                return jsonify({'success': False, 'error': f'Пара #{idx + 1} содержит некорректные идентификаторы'}), 400
            
            try:
                santa_id = int(santa_id_raw)
                recipient_id = int(recipient_id_raw)
            except (TypeError, ValueError) as e:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} invalid IDs: santa_id={santa_id_raw}, recipient_id={recipient_id_raw}, error={e}")
                return jsonify({'success': False, 'error': f'Пара #{idx + 1} содержит некорректные идентификаторы'}), 400
            
            if santa_id == recipient_id:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} santa equals recipient: {santa_id}")
                return jsonify({'success': False, 'error': f'Участник {santa_id} не может быть назначен самому себе'}), 400
            
            if santa_id not in approved_ids:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} santa_id {santa_id} not in approved_ids. Approved: {sorted(approved_ids)}")
                return jsonify({'success': False, 'error': f'Пользователь {santa_id} не входит в список утверждённых участников'}), 400
            
            if recipient_id not in approved_ids:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} recipient_id {recipient_id} not in approved_ids. Approved: {sorted(approved_ids)}")
                return jsonify({'success': False, 'error': f'Получатель {recipient_id} не входит в список утверждённых участников'}), 400
            
            santa_country = country_lookup.get(santa_id)
            recipient_country = country_lookup.get(recipient_id)
            if enforce_country and santa_country and recipient_country and santa_country != recipient_country:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} country mismatch: santa={santa_country}, recipient={recipient_country}")
                return jsonify({'success': False, 'error': f'При распределении по странам Дед Мороз ({santa_country}) и Внучок ({recipient_country}) должны быть из одной страны'}), 400
            
            if santa_id in santas_seen:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} duplicate santa_id: {santa_id}")
                return jsonify({'success': False, 'error': f'Дед Мороз {santa_id} встречается более одного раза'}), 400
            
            if recipient_id in recipients_seen:
                log_error(f"admin_event_distribution_positive_save: Entry {idx} duplicate recipient_id: {recipient_id}")
                return jsonify({'success': False, 'error': f'Получатель {recipient_id} встречается более одного раза'}), 400
            
            santas_seen.add(santa_id)
            recipients_seen.add(recipient_id)
            assignments.append((santa_id, recipient_id))
    except Exception as e:
        log_error(f"admin_event_distribution_positive_save: Unexpected error processing pairs: {e}")
        import traceback
        log_error(traceback.format_exc())
        return jsonify({'success': False, 'error': f'Ошибка обработки данных: {str(e)}'}), 400

    locked_pairs_raw = data.get('locked_pairs') or []
    locked_pairs_set = set()
    try:
        for entry in locked_pairs_raw:
            santa_id = int(entry.get('santa_id'))
            recipient_id = int(entry.get('recipient_id'))
            locked_pairs_set.add((santa_id, recipient_id))
    except (TypeError, ValueError, AttributeError):
        return jsonify({'success': False, 'error': 'Некорректные данные закреплённых пар'}), 400

    log_debug(f"admin_event_distribution_positive_save: assignments count={len(assignments)}, approved_ids count={len(approved_ids)}")
    log_debug(f"admin_event_distribution_positive_save: assignments santas={sorted(santas_seen)}, approved_ids={sorted(approved_ids)}")
    
    # Проверяем, что все участники из распределения есть в списке утвержденных
    all_santa_ids = set(pair[0] for pair in assignments)
    all_recipient_ids = set(pair[1] for pair in assignments)
    
    invalid_santas = all_santa_ids - approved_ids
    invalid_recipients = all_recipient_ids - approved_ids
    
    if invalid_santas or invalid_recipients:
        log_error(f"admin_event_distribution_positive_save: Invalid participants in distribution. Invalid santas: {sorted(invalid_santas)}, Invalid recipients: {sorted(invalid_recipients)}")
        return jsonify({
            'success': False, 
            'error': f'Распределение содержит участников, не входящих в список утверждённых. Некорректных Дедов Морозов: {len(invalid_santas)}, некорректных получателей: {len(invalid_recipients)}'
        }), 400
    
    # Проверяем, что количество пар соответствует количеству участников
    # Но не требуем строгого соответствия, так как некоторые участники могли быть удалены
    if len(assignments) != len(approved_ids):
        missing_santas = approved_ids - all_santa_ids
        missing_recipients = approved_ids - all_recipient_ids
        log_debug(f"admin_event_distribution_positive_save: Count mismatch (this is OK if participants were removed). "
                 f"Missing santas: {sorted(missing_santas)}, Missing recipients: {sorted(missing_recipients)}")
        # Это не ошибка - просто предупреждение в логах
        # Распределение может содержать меньше пар, если участники были удалены

    if locked_pairs_set:
        assignments_set = set(assignments)
        for santa_id, recipient_id in locked_pairs_set:
            if (santa_id, recipient_id) not in assignments_set:
                return jsonify({'success': False, 'error': 'Закреплённые пары должны соответствовать сохранённым значениям'}), 400

    success, result = save_event_assignments(
        event_id,
        assignments,
        user_id,
        locked_pairs=locked_pairs_set or None
    )
    if success:
        return jsonify({'success': True, 'message': f'Распределение сохранено ({result} пар).'})
    return jsonify({'success': False, 'error': result}), 500
@app.route('/admin/events/<int:event_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_event_edit(event_id):
    """Редактирование мероприятия"""
    conn = get_db_connection()
    event = conn.execute('SELECT * FROM events WHERE id = ?', (event_id,)).fetchone()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        conn.close()
        return redirect(url_for('admin_events'))
    
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    
    stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
    
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        award_id = request.form.get('award_id', '').strip()
        award_id = int(award_id) if award_id else None
        
        if not name:
            flash('Название мероприятия обязательно', 'error')
            awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
            conn.close()
            return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
        
        try:
            previous_end = None
            # Обновляем мероприятие
            conn.execute('''
                UPDATE events 
                SET name = ?, description = ?, updated_at = CURRENT_TIMESTAMP, award_id = ?
                WHERE id = ?
            ''', (name, description, award_id, event_id))
            
            # Обновляем этапы
            for stage in EVENT_STAGES:
                start_datetime = None
                end_datetime = None
                
                if stage['has_start']:
                    start_str = request.form.get(f"stage_{stage['type']}_start", '').strip()
                    if start_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in start_str:
                                if len(start_str) == 16:  # YYYY-MM-DDTHH:MM
                                    start_datetime = datetime.strptime(start_str, '%Y-%m-%dT%H:%M')
                                elif len(start_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    start_datetime = datetime.strptime(start_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                start_datetime = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты начала этапа {stage['type']}: {e}, строка: {start_str}")
                            pass
                
                if stage['has_end']:
                    end_str = request.form.get(f"stage_{stage['type']}_end", '').strip()
                    if end_str:
                        try:
                            # Пробуем разные форматы datetime-local
                            if 'T' in end_str:
                                if len(end_str) == 16:  # YYYY-MM-DDTHH:MM
                                    end_datetime = datetime.strptime(end_str, '%Y-%m-%dT%H:%M')
                                elif len(end_str) >= 19:  # YYYY-MM-DDTHH:MM:SS или больше
                                    end_datetime = datetime.strptime(end_str[:19], '%Y-%m-%dT%H:%M:%S')
                            else:
                                # Если нет T, пробуем как обычную дату
                                end_datetime = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            log_error(f"Ошибка парсинга даты окончания этапа {stage['type']}: {e}, строка: {end_str}")
                            pass
                
                # Проверяем обязательность
                if stage['required'] and stage['has_start'] and not start_datetime:
                    flash(f'Дата начала этапа "{stage["name"]}" обязательна', 'error')
                    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
                
                # Проверяем последовательность дат
                if start_datetime and previous_end and start_datetime < previous_end:
                    flash(f'Дата начала этапа "{stage["name"]}" не может быть раньше окончания предыдущего этапа', 'error')
                    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
                
                # Обновляем или создаем этап
                if stage['type'] in stages_dict:
                    # Форматируем datetime для сохранения в БД
                    start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
                    end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None
                    
                    log_debug(f"Обновление этапа {stage['type']}: start={start_datetime_str}, end={end_datetime_str}")
                    
                    conn.execute('''
                        UPDATE event_stages 
                        SET start_datetime = ?, end_datetime = ?
                        WHERE event_id = ? AND stage_type = ?
                    ''', (start_datetime_str, end_datetime_str, event_id, stage['type']))
                else:
                    stage_order = len(stages_dict) + 1
                    # Форматируем datetime для сохранения в БД
                    start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
                    end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None
                    
                    log_debug(f"Создание этапа {stage['type']}: start={start_datetime_str}, end={end_datetime_str}")
                    
                    conn.execute('''
                        INSERT INTO event_stages 
                        (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (event_id, stage['type'], stage_order, start_datetime_str, end_datetime_str,
                          1 if stage['required'] else 0, 1 if not stage['required'] else 0))
                
                previous_end = end_datetime or previous_end
            
            conn.commit()
            flash('Мероприятие успешно обновлено', 'success')
            conn.close()
            return redirect(url_for('admin_event_view', event_id=event_id))
        except Exception as e:
            log_error(f"Error updating event: {e}")
            flash(f'Ошибка обновления мероприятия: {str(e)}', 'error')
            conn.rollback()
            conn.close()
    
    # GET запрос - получаем список наград
    awards = conn.execute('SELECT id, title FROM awards ORDER BY sort_order, title').fetchall()
    conn.close()
    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict, awards=awards)
@app.route('/admin/events/<int:event_id>/delete', methods=['POST'])
@require_role('admin')
def admin_event_delete(event_id):
    """Удаление мероприятия"""
    conn = get_db_connection()
    event = conn.execute('SELECT * FROM events WHERE id = ?', (event_id,)).fetchone()
    
    if not event:
        flash('Мероприятие не найдено', 'error')
        conn.close()
        return redirect(url_for('admin_events'))
    
    try:
        conn.execute('DELETE FROM events WHERE id = ?', (event_id,))
        conn.commit()
        flash('Мероприятие успешно удалено', 'success')
    except Exception as e:
        log_error(f"Error deleting event: {e}")
        flash(f'Ошибка удаления мероприятия: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin_events'))


def _format_full_address(assignment):
    parts = []
    postal = assignment.get('recipient_postal_code')
    country = assignment.get('recipient_country')
    city = assignment.get('recipient_city')
    street = assignment.get('recipient_street')
    house = assignment.get('recipient_house')
    building = assignment.get('recipient_building')
    apartment = assignment.get('recipient_apartment')

    if postal:
        parts.append(str(postal))
    if country:
        parts.append(country)
    if city:
        parts.append(city)

    street_parts = []
    if street:
        street_parts.append(street)
    if house:
        street_parts.append(f"д. {house}")
    if building:
        street_parts.append(f"корп. {building}")
    if apartment:
        street_parts.append(f"кв. {apartment}")

    if street_parts:
        parts.append(', '.join(street_parts))

    if not parts:
        return 'адрес пока не указан'

    return ', '.join(parts)
@app.route('/letter', methods=['GET', 'POST'])
@require_login
def letter():
    """Страница с письмом получателя для Деда Мороза"""
    user_id = session.get('user_id')
    if not user_id:
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('login'))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Не удалось определить пользователя', 'error')
        return redirect(url_for('login'))

    assignment_id = request.args.get('assignment_id', type=int)
    if assignment_id is None and request.method == 'POST':
        assignment_id = request.form.get('assignment_id', type=int)

    is_admin = has_role(user_id_int, 'admin')
    admin_override = is_admin and (request.args.get('admin') == '1' or request.form.get('admin') == '1')

    if admin_override and request.method == 'POST':
        flash('Администраторы просматривают переписки только в режиме чтения.', 'error')
        return redirect(url_for('letter', assignment_id=assignment_id, admin=1) if assignment_id else url_for('admin_letters'))

    accessible_assignments = []
    if admin_override:
        accessible_assignments = get_admin_letter_assignments()
    else:
        user_assignments = get_user_assignments(user_id_int)
        for assignment in user_assignments:
            role = None
            if assignment.get('santa_user_id') == user_id_int:
                role = 'santa'
            elif assignment.get('recipient_user_id') == user_id_int:
                role = 'grandchild'
            if not role:
                continue
            assignment_copy = dict(assignment)
            assignment_copy['chat_role'] = role
            accessible_assignments.append(assignment_copy)

    if not accessible_assignments:
        if admin_override:
            flash('Пока нет переписок для отображения.', 'info')
            return redirect(url_for('admin_letters'))
        flash('У вас пока нет переписок для отображения.', 'info')
        return redirect(url_for('assignments'))

    selected_assignment = None
    if assignment_id:
        for assignment in accessible_assignments:
            if assignment.get('id') == assignment_id:
                selected_assignment = assignment
                break
        if not selected_assignment:
            flash('Выбранное задание не найдено. Показано первое доступное письмо.', 'warning')

    if not selected_assignment:
        selected_assignment = accessible_assignments[0]

    user_role = selected_assignment.get('chat_role', 'santa')

    event_finished = is_event_finished(selected_assignment.get('event_id'))

    if request.method == 'POST' and not admin_override:
        if event_finished:
            flash('Мероприятие завершено. Переписка доступна только для чтения.', 'error')
            return redirect(url_for('letter', assignment_id=selected_assignment.get('id')))

    if request.method == 'POST':
        message = _normalize_multiline_text(request.form.get('message'), max_length=2000)
        attachment_file = request.files.get('attachment')
        has_attachment = attachment_file and attachment_file.filename

        if not message and not has_attachment:
            flash('Введите сообщение или прикрепите изображение.', 'error')
            return redirect(url_for('letter', assignment_id=selected_assignment.get('id')))

        attachment_relative_path = None
        saved_filepath = None

        if has_attachment:
            filename = secure_filename(attachment_file.filename)
            _, ext = os.path.splitext(filename)
            ext = ext.lower()
            if ext not in ALLOWED_LETTER_IMAGE_EXTENSIONS:
                flash('Допускается загрузка только изображений (PNG, JPG, JPEG, GIF, WEBP).', 'error')
                return redirect(url_for('letter', assignment_id=selected_assignment.get('id')))

            unique_name = f"{selected_assignment.get('id')}_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}{ext}"
            saved_filepath = os.path.join(LETTER_UPLOAD_FOLDER, unique_name)
            try:
                attachment_file.save(saved_filepath)
            except Exception as exc:
                log_error(f"Failed to save letter attachment {unique_name}: {exc}")
                flash('Не удалось загрузить изображение.', 'error')
                return redirect(url_for('letter', assignment_id=selected_assignment.get('id')))

            attachment_relative_path = f"{LETTER_UPLOAD_RELATIVE}/{unique_name}"

        conn = get_db_connection()
        try:
            conn.execute('''
                INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
                VALUES (?, ?, ?, ?)
            ''', (selected_assignment.get('id'), user_role, message, attachment_relative_path))
            conn.commit()
            flash('Сообщение отправлено.', 'success')
        except Exception as exc:
            conn.rollback()
            if saved_filepath and os.path.exists(saved_filepath):
                try:
                    os.remove(saved_filepath)
                except OSError:
                    pass
            log_error(f"Error saving letter message for assignment {selected_assignment.get('id')}: {exc}")
            flash('Не удалось сохранить сообщение.', 'error')
        finally:
            conn.close()

        return redirect(url_for('letter', assignment_id=selected_assignment.get('id')))

    recipient_first_name = (selected_assignment.get('recipient_first_name')
                            or selected_assignment.get('recipient_username')
                            or '').strip()
    recipient_middle_name = (selected_assignment.get('recipient_middle_name') or '').strip()
    recipient_last_name = (selected_assignment.get('recipient_last_name') or '').strip()

    recipient_full_name_parts = [
        part for part in [recipient_last_name, recipient_first_name, recipient_middle_name] if part
    ]
    default_signature = 'Твой внучок'
    recipient_full_name = (
        ' '.join(recipient_full_name_parts)
        if recipient_full_name_parts else (recipient_first_name or recipient_last_name or default_signature)
    )

    recipient_address = _format_full_address(selected_assignment)

    recipient_bio = selected_assignment.get('recipient_bio')
    if recipient_bio:
        recipient_bio = recipient_bio.strip()
    if not recipient_bio:
        recipient_bio = 'Я пока не успел рассказать о себе, но обязательно сделаю это совсем скоро!'

    santa_country = selected_assignment.get('santa_country')
    santa_city = selected_assignment.get('santa_city')
    origin_parts = []
    if santa_country:
        origin_parts.append(santa_country)
    if santa_city:
        origin_parts.append(santa_city)
    if not origin_parts:
        origin_parts.append('Россия')
    santa_origin = ', '.join(origin_parts)

    letter_context = {
        'event_name': selected_assignment.get('event_name', 'Мероприятие'),
        'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
        'grandchild': {
            'first_name': recipient_first_name or recipient_full_name or default_signature,
            'full_name': recipient_full_name,
            'address': recipient_address,
            'bio': recipient_bio,
        },
        'santa': {
            'origin': santa_origin,
        }
    }

    available_letters = []
    for assignment in accessible_assignments:
        role = assignment.get('chat_role', 'santa')
        label = ''
        if role == 'santa':
            counterpart = assignment.get('recipient_first_name') or assignment.get('recipient_username') or assignment.get('recipient_last_name') or 'Получатель'
            label = f"Получатель: {counterpart}"
        elif role == 'grandchild':
            counterpart = assignment.get('santa_username') or 'Дед Мороз'
            label = f"Дед Мороз: {counterpart}"
        elif role == 'admin':
            santa_label = assignment.get('santa_full_name') or assignment.get('santa_username') or 'Дед Мороз'
            recipient_label = assignment.get('recipient_full_name') or assignment.get('recipient_username') or 'Внучок'
            label = f"Санта: {santa_label} → Внучок: {recipient_label}"
        available_letters.append({
            'assignment_id': assignment.get('id'),
            'event_name': assignment.get('event_name', 'Мероприятие'),
            'label': label,
            'role': role,
            'santa_label': assignment.get('santa_full_name') or assignment.get('santa_username'),
            'recipient_label': assignment.get('recipient_full_name') or assignment.get('recipient_username')
        })

    conn = get_db_connection()
    raw_messages = conn.execute('''
        SELECT id, sender, message, created_at, attachment_path
        FROM letter_messages
        WHERE assignment_id = ?
        ORDER BY created_at ASC, id ASC
    ''', (selected_assignment.get('id'),)).fetchall()
    chat_messages = []
    message_updates = []
    earliest_dt = None
    for row in raw_messages:
        message_text = row['message']
        if message_text:
            normalized = _normalize_multiline_text(message_text)
            if normalized != message_text:
                message_text = normalized
                message_updates.append((normalized, row['id']))
        created_raw = row['created_at']
        created_display = ''
        created_dt = None
        if created_raw:
            try:
                created_dt = datetime.fromisoformat(str(created_raw))
            except ValueError:
                try:
                    created_dt = datetime.strptime(str(created_raw), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    created_dt = None
            if created_dt:
                created_display = created_dt.strftime('%d.%m.%Y %H:%M')
                if earliest_dt is None or created_dt < earliest_dt:
                    earliest_dt = created_dt
        attachment_rel = row['attachment_path']
        attachment_url = url_for('static', filename=attachment_rel) if attachment_rel else None

        chat_messages.append({
            'sender': row['sender'],
            'message': message_text,
            'created_display': created_display,
            'created_iso': str(created_raw) if created_raw is not None else '',
            'attachment_url': attachment_url
        })

    if message_updates:
        conn.executemany('UPDATE letter_messages SET message = ? WHERE id = ?', message_updates)
        conn.commit()
    conn.close()

    if earliest_dt is None:
        candidate_dates = []
        for key in ('assigned_at', 'santa_sent_at', 'recipient_received_at'):
            value = selected_assignment.get(key)
            if value:
                parsed = parse_event_datetime(value)
                if parsed:
                    candidate_dates.append(parsed)
        if candidate_dates:
            earliest_dt = min(candidate_dates)
    if earliest_dt is None:
        earliest_dt = datetime.now()
    letter_context['date'] = earliest_dt.strftime('%d.%m.%Y %H:%M')

    return render_template(
        'letter.html',
        letter=letter_context,
        assignment=selected_assignment,
        available_letters=available_letters,
        user_role=user_role,
        chat_messages=chat_messages,
        admin_view=admin_override,
        admin_letters_url=url_for('admin_letters') if admin_override else None,
        event_finished=event_finished
    )


@app.route('/assignments')
@require_login
def assignments():
    """Страница заданий для пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('login'))
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        flash('Не удалось определить пользователя', 'error')
        return redirect(url_for('login'))
    
    user_assignments = get_user_assignments(user_id_int)
    
    # Группируем задания по мероприятиям
    assignments_by_event = {}
    for assignment in user_assignments:
        event_id = assignment.get('event_id')
        if not event_id:
            continue
        
        if event_id not in assignments_by_event:
            assignments_by_event[event_id] = {
                'event_name': assignment.get('event_name', 'Мероприятие'),
                'as_santa': None,  # Где пользователь Дед Мороз
                'as_recipient': None,  # Где пользователь Внучка
                'event_finished': is_event_finished(event_id)
            }
        
        # Проверяем, является ли пользователь Дедом Морозом в этом задании
        if assignment.get('santa_user_id') == user_id_int:
            assignments_by_event[event_id]['as_santa'] = assignment
        # Проверяем, является ли пользователь Внучкой в этом задании
        elif assignment.get('recipient_user_id') == user_id_int:
            assignments_by_event[event_id]['as_recipient'] = assignment
    
    return render_template('assignments.html', assignments_by_event=assignments_by_event)
@app.route('/admin/letters')
@require_role('admin')
def admin_letters():
    """Список всех переписок для администраторов"""
    assignments = get_admin_letter_assignments()
    return render_template('admin/letters.html', assignments=assignments)

@app.route('/titles/<int:title_id>')
def title_view(title_id):
    """Публичный список пользователей с конкретным званием"""
    conn = get_db_connection()
    title = conn.execute('SELECT * FROM titles WHERE id = ?', (title_id,)).fetchone()
    conn.close()

    if not title:
        flash('Звание не найдено', 'error')
        return redirect(url_for('participants'))

    users = get_users_with_title(title_id)
    return render_template('title_view.html', title=dict(title), users=users, get_avatar_url=get_avatar_url)


@app.route('/roles/<role_name>')
def role_view(role_name):
    """Публичный список пользователей с конкретной ролью"""
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE name = ?', (role_name,)).fetchone()

    if not role:
        conn.close()
        flash('Роль не найдена', 'error')
        return redirect(url_for('participants'))

    users = conn.execute('''
        SELECT 
            u.user_id,
            u.username,
            u.level,
            u.synd,
            u.avatar_seed,
            u.avatar_style,
            u.created_at,
            u.last_login
        FROM users u
        INNER JOIN user_roles ur ON u.user_id = ur.user_id
        INNER JOIN roles r ON ur.role_id = r.id
        WHERE r.name = ?
        ORDER BY LOWER(u.username)
    ''', (role_name,)).fetchall()
    conn.close()

    user_dicts = [dict(user) for user in users]

    return render_template(
        'role_view.html',
        role=dict(role),
        users=user_dicts,
        get_avatar_url=get_avatar_url
    )


def _normalize_contact_value(value):
    if not value:
        return ''
    value = str(value).strip()
    if not value:
        return ''
    lowered = value.lower()
    if lowered in {'не использую', 'нет', '-', 'none', 'no', 'n/a'}:
        return ''
    return value


_SNOWFLAKE_CONTACT_SOURCES = (
    ('telegram', 'Telegram', 'Заполнен Telegram'),
    ('whatsapp', 'WhatsApp', 'Заполнен WhatsApp'),
    ('viber', 'Viber', 'Заполнен Viber'),
)
_SNOWFLAKE_SOURCE_LABELS = {source: label for source, label, _ in _SNOWFLAKE_CONTACT_SOURCES}


def _normalize_multiline_text(value, max_length=None):
    if value is None:
        return ''
    text = str(value)
    text = text.replace('\r\n', '\n')
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = text.replace('\u2028', '\n').replace('\u2029', '\n')
    text = text.strip()
    text = re.sub(r'\n{3,}', '\n\n', text)
    if max_length and len(text) > max_length:
        text = text[:max_length]
    return text


def _sync_contact_snowflakes(conn, user_row):
    """Синхронизирует записи о снежинках с актуальными контактами пользователя."""
    if not isinstance(user_row, dict):
        user_row = dict(user_row)
    user_id = user_row.get('user_id')
    if not user_id:
        return

    existing = conn.execute(
        '''
        SELECT id, source, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ?
        ''',
        (user_id,)
    ).fetchall()
    existing_map = {row['source']: row for row in existing}

    for source, label, reason in _SNOWFLAKE_CONTACT_SOURCES:
        contact_value = _normalize_contact_value(user_row.get(source))
        event = existing_map.get(source)
        if contact_value:
            if not event:
                conn.execute(
                    '''
                    INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                    VALUES (?, ?, ?, 1, 1, 0)
                    ''',
                    (user_id, source, reason, 1)
                )
            elif not event['active'] and not event['manual_revoked']:
                conn.execute(
                    '''
                    UPDATE snowflake_events
                    SET active = 1,
                        points = 1,
                        revoked_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    ''',
                    (event['id'],)
                )
        else:
            if event and event['active'] and not event['manual_revoked']:
                conn.execute(
                    '''
                    UPDATE snowflake_events
                    SET active = 0,
                        revoked_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    ''',
                    (event['id'],)
                )


def _ensure_registration_bonus_event(conn, event_id, user_id):
    source = f'event:{event_id}:registration_bonus'
    reason = f'Регистрация закрыта: мероприятие #{event_id}'
    existing = conn.execute(
        '''
        SELECT id, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ? AND source = ?
        ''',
        (user_id, source)
    ).fetchone()
    if not existing:
        conn.execute(
            '''
            INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
            VALUES (?, ?, ?, 1, 1, 0)
            ''',
            (user_id, source, reason, 1)
        )
    elif not existing['active']:
        conn.execute(
            '''
            UPDATE snowflake_events
            SET active = 1,
                points = 1,
                manual_revoked = 0,
                revoked_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (existing['id'],)
        )


def _set_review_penalty(conn, event_id, user_id, apply_penalty=True):
    source = f'event:{event_id}:review_penalty'
    reason = f'Негативное ревью: мероприятие #{event_id}'
    existing = conn.execute(
        '''
        SELECT id, active, manual_revoked
        FROM snowflake_events
        WHERE user_id = ? AND source = ?
        ''',
        (user_id, source)
    ).fetchone()

    if apply_penalty:
        if not existing:
            conn.execute(
                '''
                INSERT INTO snowflake_events (user_id, source, reason, points, active, manual_revoked)
                VALUES (?, ?, ?, -2, 1, 0)
                ''',
                (user_id, source, reason, -2)
            )
        elif not existing['manual_revoked']:
            conn.execute(
                '''
                UPDATE snowflake_events
                SET active = 1,
                    points = -2,
                    manual_revoked = 0,
                    revoked_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                ''',
                (existing['id'],)
            )
    else:
        if existing and existing['active']:
            conn.execute(
                '''
                UPDATE snowflake_events
                SET active = 0,
                    revoked_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP,
                    manual_revoked = CASE WHEN manual_revoked THEN manual_revoked ELSE 0 END
                WHERE id = ?
                ''',
                (existing['id'],)
            )


def _get_snowflake_source_label(source):
    contact_map = {s: label for s, label, _ in _SNOWFLAKE_CONTACT_SOURCES}
    if source in contact_map:
        return contact_map[source]
    if source.startswith('event:'):
        parts = source.split(':')
        if len(parts) >= 3:
            try:
                event_part = parts[1]
                event_id = int(event_part)
            except (ValueError, TypeError):
                event_id = None
            suffix = parts[2]
            if suffix == 'registration_bonus':
                return f'Бонус за регистрацию (мероприятие #{event_id})' if event_id else 'Бонус за регистрацию'
            if suffix == 'review_penalty':
                return f'Негативное ревью (мероприятие #{event_id})' if event_id else 'Негативное ревью'
    return source



@app.route('/rating')
def user_rating():
    """Простая система рейтинга участников (прямая ссылка)."""
    roles = session.get('roles')
    if isinstance(roles, (list, tuple, set)):
        is_admin = 'admin' in roles
    elif isinstance(roles, str):
        is_admin = roles == 'admin'
    else:
        is_admin = False

    conn = get_db_connection()
    try:
        user_rows = [
            dict(row) for row in conn.execute('''
                SELECT user_id, username, telegram, whatsapp, viber
                FROM users
                ORDER BY LOWER(username)
            ''').fetchall()
        ]

        for user_row in user_rows:
            _sync_contact_snowflakes(conn, user_row)
        conn.commit()

        events = conn.execute('''
            SELECT id, user_id, source, reason, points, active, manual_revoked
            FROM snowflake_events
        ''').fetchall()
    finally:
        conn.close()

    events_by_user = defaultdict(lambda: {'points': 0})
    for event in events:
        if event['active']:
            events_by_user[event['user_id']]['points'] += event['points']

    rating_rows = []
    for user_row in user_rows:
        rating_rows.append({
            'user_id': user_row['user_id'],
            'username': user_row['username'],
            'rating': events_by_user[user_row['user_id']]['points'] if user_row['user_id'] in events_by_user else 0,
        })

    rating_rows.sort(key=lambda item: (-item['rating'], item['username'].lower() if item['username'] else ''))

    resp = make_response(render_template('rating.html', rating_rows=rating_rows, is_admin=is_admin))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


@app.route('/admin/rating/<int:user_id>')
@require_role('admin')
def admin_rating_detail(user_id):
    conn = get_db_connection()
    try:
        user_row = conn.execute('''
            SELECT user_id, username, telegram, whatsapp, viber
            FROM users
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        if not user_row:
            flash('Пользователь не найден', 'error')
            return redirect(url_for('user_rating'))

        user_dict = dict(user_row)
        _sync_contact_snowflakes(conn, user_dict)
        conn.commit()

        events = [
            dict(row) for row in conn.execute('''
                SELECT id, source, reason, points, active, manual_revoked, created_at, updated_at, revoked_at
                FROM snowflake_events
                WHERE user_id = ?
                ORDER BY created_at DESC, id DESC
            ''', (user_id,)).fetchall()
        ]
    finally:
        conn.close()

    for event in events:
        event['source_label'] = _get_snowflake_source_label(event['source'])

    active_count = sum(event['points'] for event in events if event['active'])
    return render_template(
        'admin/rating_detail.html',
        user=user_dict,
        events=events,
        active_count=active_count,
    )


@app.route('/admin/rating/events/<int:event_id>/annul', methods=['POST'])
@require_role('admin')
def admin_rating_event_annul(event_id):
    conn = get_db_connection()
    try:
        event = conn.execute('SELECT id, user_id, active, manual_revoked FROM snowflake_events WHERE id = ?', (event_id,)).fetchone()
        if not event:
            flash('Запись не найдена', 'error')
            return redirect(url_for('user_rating'))

        user_id = event['user_id']
        if event['manual_revoked'] and not event['active']:
            flash('Снежинка уже аннулирована.', 'info')
        else:
            conn.execute('''
                UPDATE snowflake_events
                SET active = 0,
                    manual_revoked = 1,
                    revoked_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (event_id,))
            conn.commit()
            log_activity('snowflake_annul', details=f'Аннулирована снежинка #{event_id}', metadata={'event_id': event_id, 'target_user_id': user_id})
            flash('Снежинка аннулирована.', 'success')
    finally:
        conn.close()

    return redirect(url_for('admin_rating_detail', user_id=user_id))


@app.route('/admin/rating/events/<int:event_id>/restore', methods=['POST'])
@require_role('admin')
def admin_rating_event_restore(event_id):
    conn = get_db_connection()
    try:
        event = conn.execute('SELECT id, user_id, manual_revoked FROM snowflake_events WHERE id = ?', (event_id,)).fetchone()
        if not event:
            flash('Запись не найдена', 'error')
            return redirect(url_for('user_rating'))

        user_id = event['user_id']
        conn.execute('''
            UPDATE snowflake_events
            SET active = 1,
                manual_revoked = 0,
                revoked_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (event_id,))
        conn.commit()
        log_activity('snowflake_restore', details=f'Восстановлена снежинка #{event_id}', metadata={'event_id': event_id, 'target_user_id': user_id})
        flash('Снежинка восстановлена.', 'success')
    finally:
        conn.close()

    return redirect(url_for('admin_rating_detail', user_id=user_id))


@app.route('/awards/<int:award_id>')
def award_view(award_id):
    """Публичный список пользователей с конкретной наградой"""
    conn = get_db_connection()
    award = conn.execute('SELECT * FROM awards WHERE id = ?', (award_id,)).fetchone()
    conn.close()

    if not award:
        flash('Награда не найдена', 'error')
        return redirect(url_for('participants'))

    users = get_users_with_award(award_id)
    return render_template('award_view.html', award=dict(award), users=users, get_avatar_url=get_avatar_url)

@app.route('/assignments/<int:assignment_id>/send', methods=['POST'])
@require_login
def assignment_mark_sent(assignment_id):
    """Обработчик отметки отправки подарка"""
    user_id = session.get('user_id')
    send_info = request.form.get('send_info', '').strip()
    
    success, message = mark_assignment_sent(assignment_id, user_id, send_info)
    flash(message, 'success' if success else 'error')
    
    return redirect(url_for('assignments'))

@app.route('/assignments/<int:assignment_id>/receive', methods=['POST'])
@require_login
def assignment_mark_received(assignment_id):
    """Обработчик подтверждения получения подарка"""
    user_id = session.get('user_id')
    thank_you_message = (request.form.get('thank_you_message') or '').strip()
    receipt_file = request.files.get('receipt_image')

    success, message = mark_assignment_received(
        assignment_id,
        user_id,
        thank_you_message,
        receipt_file
    )
    flash(message, 'success' if success else 'error')
    
    return redirect(url_for('assignments'))

# Инициализируем БД при импорте модуля (для WSGI)
try:
    init_db()
except Exception as e:
    log_error(f"Failed to initialize database on startup: {e}")

@app.errorhandler(404)
def handle_not_found(error):
    return render_template('errors/404.html', error=error), 404


@app.errorhandler(500)
def handle_server_error(error):
    log_error(f"Internal server error: {error}")
    return render_template('errors/500.html', error=error), 500


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    if isinstance(error, HTTPException):
        code = error.code or 500
        if code == 404:
            return handle_not_found(error)
        if code == 500:
            return handle_server_error(error)
        return render_template('errors/generic.html', error=error, status_code=code), code

    log_error(f"Unhandled exception: {error}\n{traceback.format_exc()}")
    return render_template('errors/500.html', error=error), 500



if __name__ == '__main__':
    app.run(debug=True)