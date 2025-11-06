from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, jsonify, send_from_directory
from urllib.parse import unquote, unquote_to_bytes, quote
import hashlib
import sqlite3
from datetime import datetime
import os
import logging
from functools import wraps
from version import __version__
import secrets
try:
    import requests
except ImportError:
    requests = None

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['VERSION'] = __version__

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
                if user and user.get('language') and user['language'] in app.config['LANGUAGES']:
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
ADMIN_USER_IDS = [283494, 240139, 90180]

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
            ('gwars_host', GWARS_HOST, 'Домен для GWars авторизации', 'gwars'),
            ('gwars_site_id', str(GWARS_SITE_ID), 'ID сайта в GWars', 'gwars'),
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
        ]
        
        for key, value, description, category in default_settings:
            c.execute('''
                INSERT OR IGNORE INTO settings (key, value, description, category)
                VALUES (?, ?, ?, ?)
            ''', (key, value, description, category))
            # Обновляем дефолтные значения для site_icon и site_logo, если они пустые
            if key in ('site_icon', 'site_logo'):
                c.execute('''
                    UPDATE settings SET value = ? WHERE key = ? AND (value = '' OR value IS NULL)
                ''', (value, key))
        
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
    # Используем комбинацию user_id + случайную строку для уникальности
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
        try:
            name_bytes = unquote_to_bytes(encoded_name)
            expected_sign_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8')
            ).hexdigest()
            variants.append(('bytes', expected_sign_bytes))
        except:
            pass
    
    # Вариант 2: декодированное имя через UTF-8
    expected_sign_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id)).encode('utf-8')
    ).hexdigest()
    variants.append(('decoded', expected_sign_decoded))
    
    # Вариант 3: закодированное имя (как пришло в URL)
    if encoded_name:
        expected_sign_encoded = hashlib.md5(
            (GWARS_PASSWORD + encoded_name + str(user_id)).encode('utf-8')
        ).hexdigest()
        variants.append(('encoded', expected_sign_encoded))
    
    # Вариант 4: декодированное через CP1251 (Windows-1251)
    if encoded_name:
        try:
            name_cp1251 = unquote(encoded_name, encoding='cp1251')
            expected_sign_cp1251 = hashlib.md5(
                (GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8')
            ).hexdigest()
            variants.append(('cp1251', expected_sign_cp1251))
        except:
            pass
        
        # Вариант 5: декодированное через latin1, затем байты
        try:
            name_latin1 = unquote(encoded_name, encoding='latin1')
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
        try:
            name_bytes = unquote_to_bytes(encoded_name)
            expected_sign3_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8') + 
                str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8')
            ).hexdigest()[:10]
            variants.append(('bytes', expected_sign3_bytes))
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
        ORDER BY e.created_at DESC
        LIMIT 6
    ''').fetchall()
    
    # Определяем текущий этап для каждого мероприятия
    events_with_stages = []
    for event in events_list:
        current_stage = get_current_event_stage(event['id'])
        events_with_stages.append({
            'event': event,
            'current_stage': current_stage
        })
    
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
    
    # Для пользователя 90180 автоматически назначаем звание "Автор идеи"
    if user_id == 90180:
        author_title = get_title_by_name('author')
        if author_title:
            user_titles = get_user_titles(user_id)
            user_title_ids = [t['id'] for t in user_titles]
            if author_title['id'] not in user_title_ids:
                assign_title(user_id, author_title['id'], assigned_by=user_id)
                log_debug(f"Title 'Автор идеи' automatically assigned to user_id {user_id}")
    
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
                name_cp1251 = unquote(name_encoded, encoding='cp1251')
                name = name_cp1251  # Используем CP1251 как основной вариант
            except:
                try:
                    name = unquote(name_encoded, encoding='utf-8')
                except:
                    try:
                        name = unquote(name_encoded, encoding='latin1')
                        name_latin1 = name
                    except:
                        name = name_encoded
                        name_latin1 = name_encoded
            
            # Если CP1251 декодирование не сработало, пробуем еще раз
            if not name_cp1251:
                try:
                    name_cp1251 = unquote(name_encoded, encoding='cp1251')
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
        
        # Для пользователя 90180 автоматически назначаем звание "Автор идеи"
        if int(user_id) == 90180:
            author_title = get_title_by_name('author')
            if author_title:
                user_titles = get_user_titles(user_id)
                user_title_ids = [t['id'] for t in user_titles]
                if author_title['id'] not in user_title_ids:
                    assign_title(user_id, author_title['id'], assigned_by=user_id)
                    log_debug(f"Title 'Автор идеи' automatically assigned to user_id {user_id}")
        
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
        
        return redirect(url_for('dashboard'))
    except Exception as e:
        log_error(f"Error in login route: {e}")
        import traceback
        log_error(f"Traceback: {traceback.format_exc()}")
        flash(f'Ошибка при входе: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/dashboard')
@require_login
def dashboard():
    # Получаем данные пользователя из БД
    conn = get_db_connection()
    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (session['user_id'],)
    ).fetchone()
    
    # Получаем роли пользователя
    user_roles = get_user_roles(session['user_id'])
    
    conn.close()
    
    return render_template('dashboard.html', user=user, user_roles=user_roles)


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
    
    conn.close()
    
    # Проверяем, является ли это профилем текущего пользователя (если авторизован)
    is_own_profile = session.get('user_id') == user_id if 'user_id' in session else False
    
    return render_template('view_profile.html', user=user, user_roles=user_roles, user_titles=user_titles, user_awards=user_awards, is_own_profile=is_own_profile)

@app.route('/participants')
def participants():
    """Страница со списком участников"""
    try:
        conn = get_db_connection()
        
        # Получаем всех пользователей с их ролями
        users = conn.execute('''
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
        
        return render_template('participants.html', 
                             participants=participants_data,
                             get_avatar_url=get_avatar_url)
    except Exception as e:
        log_error(f"Error in participants route: {e}")
        import traceback
        log_error(traceback.format_exc())
        try:
            conn.close()
        except:
            pass
        return f"Ошибка при загрузке участников: {str(e)}", 500

@app.route('/logout')
def logout():
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
    conn.close()
    
    return render_template('admin/users.html', users=users)

@app.route('/admin/users/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_create():
    """Создание нового пользователя"""
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
        
        if not user_id or not username:
            flash('ID и имя пользователя обязательны', 'error')
            return render_template('admin/user_form.html')
        
        try:
            user_id_int = int(user_id)
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = int(has_passport)
            has_mobile_int = int(has_mobile)
        except ValueError:
            flash('Неверный формат числовых полей', 'error')
            return render_template('admin/user_form.html')
        
        conn = get_db_connection()
        
        # Проверяем, существует ли пользователь
        existing = conn.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id_int,)).fetchone()
        if existing:
            flash('Пользователь с таким ID уже существует', 'error')
            conn.close()
            return render_template('admin/user_form.html')
        
        try:
            # Генерируем уникальный avatar_seed для нового пользователя
            avatar_seed = generate_unique_avatar_seed(user_id_int)
            avatar_style = 'avataaars'
            
            conn.execute('''
                INSERT INTO users 
                (user_id, username, level, synd, has_passport, has_mobile, usersex, 
                 avatar_seed, avatar_style, bio, contact_info, email, phone, telegram, whatsapp, viber)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id_int, username, level_int, synd_int, has_passport_int, has_mobile_int, 
                  usersex, avatar_seed, avatar_style, bio, contact_info, 
                  email, phone, telegram, whatsapp, viber))
            conn.commit()
            flash('Пользователь успешно создан', 'success')
            conn.close()
            return redirect(url_for('admin_users'))
        except Exception as e:
            log_error(f"Error creating user: {e}")
            flash(f'Ошибка создания пользователя: {str(e)}', 'error')
            conn.close()
            return render_template('admin/user_form.html')
    
    return render_template('admin/user_form.html')

@app.route('/admin/users/<int:user_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_user_edit(user_id):
    """Редактирование пользователя"""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    
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
        
        if not username:
            flash('Имя пользователя обязательно', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
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
                                 user_title_ids=user_title_ids)
        
        try:
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = int(has_passport)
            has_mobile_int = int(has_mobile)
        except ValueError:
            flash('Неверный формат числовых полей', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
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
                                 user_title_ids=user_title_ids)
        
        try:
            conn.execute('''
                UPDATE users SET
                    username = ?, level = ?, synd = ?, has_passport = ?, has_mobile = ?,
                    usersex = ?, bio = ?, contact_info = ?,
                    email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?
                WHERE user_id = ?
            ''', (username, level_int, synd_int, has_passport_int, has_mobile_int,
                  usersex, bio, contact_info, email, phone, 
                  telegram, whatsapp, viber, user_id))
            conn.commit()
            if not role_action and not title_action:
                flash('Пользователь успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('admin_user_edit', user_id=user_id))
        except Exception as e:
            log_error(f"Error updating user: {e}")
            flash(f'Ошибка обновления пользователя: {str(e)}', 'error')
            # Получаем данные для отображения ДО закрытия соединения
            all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
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
                                 user_title_ids=user_title_ids)
    
    # GET запрос - получаем данные для отображения
    all_roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    user_roles = get_user_roles(user_id)
    user_role_names = [r['name'] for r in user_roles]
    all_titles = get_all_titles()
    user_titles = get_user_titles(user_id)
    user_title_ids = [t['id'] for t in user_titles]
    
    conn.close()
    return render_template('admin/user_form.html', 
                         user=dict(user),
                         all_roles=all_roles,
                         user_roles=user_roles,
                         user_role_names=user_role_names,
                         all_titles=all_titles,
                         user_titles=user_titles,
                         user_title_ids=user_title_ids)

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
    """Управление ролями"""
    conn = get_db_connection()
    roles = conn.execute('SELECT * FROM roles ORDER BY is_system DESC, display_name').fetchall()
    
    # Для каждой роли получаем количество пользователей
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
    
    return render_template('admin/roles.html', roles=roles_with_counts)

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
    conn = get_db_connection()
    
    if request.method == 'POST':
        # Создаем папку для загрузок если её нет
        upload_dir = os.path.join(app.static_folder, 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        # Обработка загрузки файлов
        if 'site_icon' in request.files:
            icon_file = request.files['site_icon']
            if icon_file and icon_file.filename:
                # Проверяем расширение
                allowed_extensions = {'.ico', '.png', '.jpg', '.jpeg', '.svg'}
                file_ext = os.path.splitext(icon_file.filename)[1].lower()
                if file_ext in allowed_extensions:
                    # Сохраняем файл
                    filename = f"icon_{int(datetime.now().timestamp())}{file_ext}"
                    filepath = os.path.join(upload_dir, filename)
                    icon_file.save(filepath)
                    # Сохраняем путь в настройках
                    set_setting('site_icon', f'/static/uploads/{filename}', 'Иконка сайта (favicon)', 'general')
        
        if 'site_logo' in request.files:
            logo_file = request.files['site_logo']
            if logo_file and logo_file.filename:
                # Проверяем расширение
                allowed_extensions = {'.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp'}
                file_ext = os.path.splitext(logo_file.filename)[1].lower()
                if file_ext in allowed_extensions:
                    # Сохраняем файл
                    filename = f"logo_{int(datetime.now().timestamp())}{file_ext}"
                    filepath = os.path.join(upload_dir, filename)
                    logo_file.save(filepath)
                    # Сохраняем путь в настройках
                    set_setting('site_logo', f'/static/uploads/{filename}', 'Логотип сайта', 'general')
        
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
                
                conn.execute('''
                    UPDATE settings 
                    SET value = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                    WHERE key = ?
                ''', (value, session.get('user_id'), key))
            except Exception as e:
                log_error(f"Error updating setting {key}: {e}")
        
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
    
    conn.close()
    
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
                         all_titles=all_titles)

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

@app.route('/admin/faq')
@require_role('admin')
def admin_faq():
    """Управление FAQ"""
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
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        answer = request.form.get('answer', '').strip()
        category = request.form.get('category', 'general').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not question or not answer:
            flash('Вопрос и ответ обязательны для заполнения', 'error')
            return render_template('admin/faq_form.html')
        
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
    
    return render_template('admin/faq_form.html')

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

@app.route('/admin/rules')
@require_role('admin')
def admin_rules():
    """Управление правилами"""
    try:
        rules_content = get_setting('rules_content', '')
        return render_template('admin/rules.html', rules_content=rules_content)
    except Exception as e:
        log_error(f"Error in admin_rules route: {e}")
        flash(f'Ошибка загрузки правил: {str(e)}', 'error')
        return render_template('admin/rules.html', rules_content='')

@app.route('/admin/rules/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_rules_edit():
    """Редактирование правил"""
    try:
        if request.method == 'POST':
            rules_content = request.form.get('rules_content', '').strip()
            
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
                    ''', (rules_content, datetime.now(), user_id, 'rules_content'))
                else:
                    # Создаем новую настройку
                    conn.execute('''
                        INSERT INTO settings (key, value, category, created_at, created_by, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', ('rules_content', rules_content, 'general', datetime.now(), user_id, datetime.now(), user_id))
                
                conn.commit()
                flash('Правила успешно сохранены', 'success')
            except Exception as e:
                log_error(f"Error saving rules: {e}")
                flash(f'Ошибка сохранения правил: {str(e)}', 'error')
            finally:
                conn.close()
            
            return redirect(url_for('admin_rules'))
        
        rules_content = get_setting('rules_content', '')
        return render_template('admin/rules_edit.html', rules_content=rules_content)
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
    {'type': 'celebration_date', 'name': 'Дата праздника', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'after_party', 'name': 'Послепраздничное настроение', 'required': True, 'has_start': False, 'has_end': True},
]

def is_event_finished(event_id):
    """Проверяет, закончилось ли мероприятие полностью"""
    conn = get_db_connection()
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()
    
    if not stages:
        return False
    
    now = datetime.now()
    
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

def distribute_event_awards(event_id):
    """Выдает награды всем участникам завершенного мероприятия"""
    conn = get_db_connection()
    
    # Проверяем, есть ли награда для мероприятия
    event = conn.execute('SELECT award_id FROM events WHERE id = ?', (event_id,)).fetchone()
    if not event or not event['award_id']:
        conn.close()
        return False
    
    award_id = event['award_id']
    
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
        log_debug(f"Distributed {awarded_count} awards for event {event_id}")
    
    conn.close()
    return awarded_count > 0

def get_current_event_stage(event_id):
    """Определяет текущий этап мероприятия на основе текущей даты"""
    conn = get_db_connection()
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()
    
    if not stages:
        return None
    
    now = datetime.now()
    
    # Создаем словарь этапов с их информацией
    stages_dict = {stage['stage_type']: stage for stage in stages}
    stages_info_dict = {stage['type']: stage for stage in EVENT_STAGES}
    
    # Ищем текущий этап
    current_stage = None
    
    for stage_info in EVENT_STAGES:
        stage_type = stage_info['type']
        if stage_type not in stages_dict:
            continue
        
        stage = stages_dict[stage_type]
        
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

@app.route('/events')
def events():
    """Публичная страница со списком всех мероприятий"""
    conn = get_db_connection()
    events_list = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        ORDER BY e.created_at DESC
    ''').fetchall()
    conn.close()
    
    # Определяем текущий этап для каждого мероприятия
    events_with_stages = []
    for event in events_list:
        current_stage = get_current_event_stage(event['id'])
        events_with_stages.append({
            'event': event,
            'current_stage': current_stage
        })
    
    # Добавляем информацию о регистрации для каждого мероприятия
    user_id = session.get('user_id')
    for item in events_with_stages:
        event = item['event']
        item['is_registered'] = is_user_registered(event['id'], user_id) if user_id else False
        item['registrations_count'] = get_event_registrations_count(event['id'])
        item['registration_open'] = is_registration_open(event['id'])
    
    return render_template('events.html', events_with_stages=events_with_stages)

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
    is_registered = is_user_registered(event_id, user_id)
    registrations_count = get_event_registrations_count(event_id)
    registrations = get_event_registrations(event_id)
    
    # Проверяем, закончилось ли мероприятие, и выдаем награды если нужно
    if is_event_finished(event_id):
        distribute_event_awards(event_id)
    
    # Получаем все этапы мероприятия
    conn = get_db_connection()
    stages = conn.execute('''
        SELECT * FROM event_stages 
        WHERE event_id = ? 
        ORDER BY stage_order
    ''', (event_id,)).fetchall()
    conn.close()
    
    # Определяем статус каждого этапа (past, current, future)
    now = datetime.now()
    current_stage_type = current_stage['info']['type'] if current_stage else None
    
    stages_with_info = []
    stages_dict = {stage['stage_type']: stage for stage in stages}
    
    for stage_info in EVENT_STAGES:
        stage_type = stage_info['type']
        stage_data = stages_dict.get(stage_type, None)
        
        # Определяем статус этапа
        stage_status = 'future'  # по умолчанию будущий
        if stage_data:
            # Проверяем, является ли это текущим этапом
            if current_stage_type == stage_type:
                stage_status = 'current'
            else:
                # Проверяем, прошел ли этап
                if stage_data['start_datetime']:
                    try:
                        start_dt = datetime.strptime(stage_data['start_datetime'], '%Y-%m-%d %H:%M:%S')
                    except:
                        try:
                            start_dt = datetime.strptime(stage_data['start_datetime'], '%Y-%m-%dT%H:%M')
                        except:
                            start_dt = None
                    
                    if start_dt:
                        # Проверяем, не начался ли следующий этап
                        stage_order = stage_data['stage_order']
                        next_stage_started = False
                        for next_stage in stages:
                            if next_stage['stage_order'] > stage_order and next_stage['start_datetime']:
                                try:
                                    next_start_dt = datetime.strptime(next_stage['start_datetime'], '%Y-%m-%d %H:%M:%S')
                                except:
                                    try:
                                        next_start_dt = datetime.strptime(next_stage['start_datetime'], '%Y-%m-%dT%H:%M')
                                    except:
                                        continue
                                if now >= next_start_dt:
                                    next_stage_started = True
                                    break
                        
                        if next_stage_started or (now < start_dt):
                            # Этап еще не начался или уже закончился
                            if now < start_dt:
                                stage_status = 'future'
                            else:
                                stage_status = 'past'
                        else:
                            # Этап должен быть текущим, но не определен как текущий
                            # Это может быть ошибка в логике, но оставим как есть
                            pass
        
        stages_with_info.append({
            'info': stage_info,
            'data': stage_data,
            'status': stage_status
        })
    
    return render_template('event_view.html', 
                         event=event,
                         current_stage=current_stage,
                         registration_open=registration_open,
                         is_registered=is_registered,
                         registrations_count=registrations_count,
                         registrations=registrations,
                         stages_with_info=stages_with_info)

def has_required_contacts(user_id):
    """Проверяет, заполнены ли обязательные контактные данные пользователя"""
    conn = get_db_connection()
    try:
        user = conn.execute('''
            SELECT email, phone, telegram, whatsapp, viber,
                   last_name, first_name, middle_name,
                   postal_code, country, city, street, house, building, apartment
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
                   postal_code, country, city, street, house, building, apartment
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
        request.headers.get('Content-Type') == 'application/json' or 
        request.headers.get('Accept') == 'application/json' or
        request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
        request.is_json
    )
    
    if not user_id:
        if is_json_request:
            return jsonify({'success': False, 'error': 'Необходимо авторизоваться'}), 401
        flash('Необходимо авторизоваться', 'error')
        return redirect(url_for('login'))
    
    # Проверяем, открыта ли регистрация
    if not is_registration_open(event_id):
        if is_json_request:
            return jsonify({'success': False, 'error': 'Регистрация на это мероприятие закрыта'}), 400
        flash('Регистрация на это мероприятие закрыта', 'error')
        return redirect(url_for('event_view', event_id=event_id))
    
    # Проверяем, не зарегистрирован ли уже
    if is_user_registered(event_id, user_id):
        if is_json_request:
            return jsonify({'success': False, 'error': 'Вы уже зарегистрированы на это мероприятие'}), 400
        flash('Вы уже зарегистрированы на это мероприятие', 'info')
        return redirect(url_for('event_view', event_id=event_id))
    
    # Проверяем заполненность обязательных полей
    missing_fields = get_missing_required_fields(user_id)
    has_all_required = (
        missing_fields['has_personal_data'] and 
        missing_fields['has_address'] and 
        missing_fields['has_contact']
    )
    
    # Проверяем, является ли это финальным запросом после подтверждения в модальном окне
    is_final_registration = False
    if request.is_json and request.json:
        is_final_registration = request.json.get('final_registration', False)
    
    # Для AJAX запросов: показываем модальное окно для проверки данных
    # Исключение: если это финальный запрос после подтверждения в модальном окне - регистрируем сразу
    if is_json_request or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        if not is_final_registration:
            # Это первый запрос - показываем модальное окно для проверки/заполнения данных
            log_debug(f"AJAX запрос на регистрацию от пользователя {user_id}. Показываем модальное окно для проверки данных")
            return jsonify({
                'success': False,
                'needs_filling': True,
                'missing_fields': missing_fields
            }), 200
        # Это финальный запрос после подтверждения - проверяем, все ли заполнено
        if not has_all_required:
            log_debug(f"Финальный AJAX запрос от пользователя {user_id}, но данные не заполнены")
            return jsonify({
                'success': False,
                'error': 'Пожалуйста, заполните все обязательные поля',
                'missing_fields': missing_fields
            }), 400
        # Все данные заполнены - продолжаем регистрацию ниже
    
    # Для обычных запросов проверяем заполненность контактных данных
    if not has_required_contacts(user_id):
        missing_fields = get_missing_required_fields(user_id)
        log_debug(f"Пользователь {user_id} пытается зарегистрироваться, но не заполнены обязательные поля")
        
        # Если это не AJAX запрос, показываем flash сообщение
        flash('Для регистрации на мероприятие необходимо заполнить все обязательные поля в разделе "Контакты" вашего профиля. Пожалуйста, перейдите в <a href="' + url_for('dashboard') + '#contacts" style="text-decoration: underline;">профиль</a> и заполните:<br><br><strong>Личные данные:</strong> Фамилия, Имя, Отчество<br><strong>Адрес:</strong> Индекс, Страна, Город, Улица, Дом, Корпус/Строение, Квартира<br><strong>Контактные данные:</strong> хотя бы одно из полей (Email, Телефон, Telegram, WhatsApp или Viber)', 'error')
        return redirect(url_for('event_view', event_id=event_id))
    
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT INTO event_registrations (event_id, user_id)
            VALUES (?, ?)
        ''', (event_id, user_id))
        conn.commit()
        if is_json_request:
            return jsonify({'success': True, 'message': 'Вы успешно зарегистрированы на мероприятие!'}), 200
        flash('Вы успешно зарегистрированы на мероприятие!', 'success')
    except sqlite3.IntegrityError:
        if is_json_request:
            return jsonify({'success': False, 'error': 'Вы уже зарегистрированы на это мероприятие'}), 400
        flash('Вы уже зарегистрированы на это мероприятие', 'info')
    except Exception as e:
        log_error(f"Ошибка регистрации на мероприятие: {e}")
        if is_json_request:
            return jsonify({'success': False, 'error': 'Ошибка при регистрации'}), 500
        flash('Ошибка при регистрации', 'error')
    finally:
        conn.close()
    
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
                   postal_code, country, city, street, house, building, apartment
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
                'apartment': user['apartment'] or ''
            }
        })
    except Exception as e:
        log_error(f"Error getting profile data for user_id={user_id}: {e}")
        import traceback
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
        
        if not update_fields:
            return jsonify({'success': False, 'error': 'Нет полей для обновления'}), 400
        
        update_values.append(user_id)
        update_query = f'''
            UPDATE users 
            SET {', '.join(update_fields)}
            WHERE user_id = ?
        '''
        conn.execute(update_query, update_values)
        conn.commit()
        
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
    """Страница с часто задаваемыми вопросами (всегда показывает статический контент)"""
    # Всегда используем статический контент (дефолтный)
    return render_template('faq.html', faq_by_category=None)

@app.route('/rules')
def rules():
    """Страница с правилами"""
    try:
        # Получаем правила из настроек, если они есть
        rules_content = get_setting('rules_content', '')
        return render_template('rules.html', rules_content=rules_content)
    except Exception as e:
        log_error(f"Error in rules route: {e}")
        return render_template('rules.html', rules_content='')

@app.route('/contacts')
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
    return render_template('admin/events.html', events=events)

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
    stages_dict = {stage['stage_type']: stage for stage in stages}
    stages_with_info = []
    for stage_info in EVENT_STAGES:
        stage_data = stages_dict.get(stage_info['type'], None)
        stages_with_info.append({
            'info': stage_info,
            'data': stage_data
        })
    
    return render_template('admin/event_view.html', event=event, stages_with_info=stages_with_info)

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
    
    stages_dict = {stage['stage_type']: stage for stage in stages}
    
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

# Инициализируем БД при импорте модуля (для WSGI)
try:
    init_db()
except Exception as e:
    log_error(f"Failed to initialize database on startup: {e}")

if __name__ == '__main__':
    app.run(debug=True)

