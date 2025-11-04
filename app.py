from flask import Flask, render_template, request, redirect, url_for, session, flash, abort, jsonify, send_from_directory
from urllib.parse import unquote, unquote_to_bytes
import hashlib
import sqlite3
from datetime import datetime
import os
import logging
from functools import wraps
from version import __version__
import secrets

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['VERSION'] = __version__

# Настройка логирования
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
        
        # Добавляем пользовательские поля для редактирования профиля (миграция)
        user_editable_fields = ['bio', 'contact_info', 'avatar_style']
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
                FOREIGN KEY (created_by) REFERENCES users(user_id)
            )
        ''')
        
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
        
        # Инициализация настроек по умолчанию
        default_settings = [
            ('gwars_host', GWARS_HOST, 'Домен для GWars авторизации', 'gwars'),
            ('gwars_site_id', str(GWARS_SITE_ID), 'ID сайта в GWars', 'gwars'),
            ('admin_user_ids', ','.join(map(str, ADMIN_USER_IDS)), 'ID администраторов по умолчанию (через запятую)', 'system'),
            ('project_name', 'Анонимные Деды Морозы', 'Название проекта', 'general'),
            ('default_theme', 'light', 'Тема по умолчанию (light или dark)', 'general'),
            ('site_icon', '', 'Иконка сайта (favicon)', 'general'),
            ('site_logo', '', 'Логотип сайта', 'general'),
        ]
        
        for key, value, description, category in default_settings:
            c.execute('''
                INSERT OR IGNORE INTO settings (key, value, description, category)
                VALUES (?, ?, ?, ?)
            ''', (key, value, description, category))
        
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
    """Получает список всех прав, сгруппированных по категориям"""
    conn = get_db_connection()
    permissions = conn.execute('''
        SELECT * FROM permissions ORDER BY category, display_name
    ''').fetchall()
    conn.close()
    
    # Группируем по категориям
    grouped = {}
    for perm in permissions:
        category = perm['category'] or 'general'
        if category not in grouped:
            grouped[category] = []
        grouped[category].append(dict(perm))
    
    return grouped

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
    today = datetime.now().strftime("%Y-%m-%d")
    expected_sign4 = hashlib.md5(
        (today + sign3 + GWARS_PASSWORD).encode('utf-8')
    ).hexdigest()[:10]
    return expected_sign4 == sign4

@app.context_processor
def inject_default_theme():
    """Добавляет настройку темы по умолчанию и функции во все шаблоны"""
    default_theme = get_setting('default_theme', 'light')
    # Получаем аватар текущего пользователя для хэдера
    current_user_avatar_seed = None
    current_user_avatar_style = None
    if 'user_id' in session:
        conn = get_db_connection()
        user = conn.execute('SELECT avatar_seed, avatar_style FROM users WHERE user_id = ?', (session['user_id'],)).fetchone()
        if user:
            current_user_avatar_seed = user['avatar_seed']
            current_user_avatar_style = user['avatar_style']
        conn.close()
    return dict(
        default_theme=default_theme, 
        get_avatar_url=get_avatar_url,
        current_user_avatar_seed=current_user_avatar_seed,
        current_user_avatar_style=current_user_avatar_style,
        get_role_permissions=get_role_permissions,
        get_setting=get_setting
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
        existing_user = conn.execute('SELECT avatar_seed, avatar_style, bio, contact_info FROM users WHERE user_id = ?', (user_id,)).fetchone()
        
        # Если пользователь новый, генерируем уникальный avatar_seed
        avatar_seed = None
        avatar_style = None
        bio = None
        contact_info = None
        
        if not existing_user:
            # Новый пользователь - генерируем рандомный аватар
            avatar_seed = generate_unique_avatar_seed(user_id)
            avatar_style = 'avataaars'  # Стиль по умолчанию
        elif existing_user and not existing_user['avatar_seed']:
            # Если у существующего пользователя нет seed, генерируем
            avatar_seed = generate_unique_avatar_seed(user_id)
            avatar_style = existing_user['avatar_style'] or 'avataaars'
            bio = existing_user['bio']
            contact_info = existing_user['contact_info']
        else:
            # Используем существующий seed и сохраняем все пользовательские данные
            avatar_seed = existing_user['avatar_seed']
            avatar_style = existing_user['avatar_style']
            bio = existing_user['bio']
            contact_info = existing_user['contact_info']
        
        # Обновляем данные пользователя, сохраняя пользовательские поля (avatar, bio, contact_info)
        conn.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, last_login)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, name, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, datetime.now()))
        conn.commit()
        log_debug(f"Dev user saved successfully: user_id={user_id}, username={name}, avatar_seed={avatar_seed}")
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
    
    flash('Тестовая авторизация выполнена успешно!', 'success')
    return redirect(url_for('dashboard'))

@app.route('/login')
def login():
    # Получаем параметры от GWars
    sign = request.args.get('sign', '')
    user_id = request.args.get('user_id', '')
    
    # ВАЖНО: Flask автоматически декодирует URL параметры, но нам нужен оригинальный закодированный вариант
    # Получаем оригинальное значение из query string напрямую
    try:
        query_string_raw = request.query_string
        query_string = query_string_raw.decode('utf-8', errors='replace')
    except:
        query_string = request.query_string.decode('utf-8')
    
    name_encoded = None
    # Пробуем извлечь name из query string
    for param in query_string.split('&'):
        if param.startswith('name='):
            name_encoded = param.split('=', 1)[1]  # Берем все после первого =
            break
    
    # Если не получилось получить из query_string, пробуем через request.args (но это уже декодированное)
    if not name_encoded or name_encoded == '':
        name_encoded = request.args.get('name', '')
        # Если получили через args, значит оно уже декодировано, нужно закодировать обратно для проверки
        if name_encoded:
            from urllib.parse import quote
            name_encoded_for_comparison = quote(name_encoded, safe='')
        else:
            name_encoded_for_comparison = ''
    else:
        name_encoded_for_comparison = name_encoded
    
    # Пробуем декодировать разными способами
    # ВАЖНО: GWars использует CP1251 (Windows-1251) для кодирования русских символов!
    name = name_encoded
    name_latin1 = None
    name_cp1251 = None
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
    
    # Если нет параметров, редиректим на GWars для авторизации
    if not sign or not user_id:
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
        
        gwars_url = f"https://www.gwars.io/cross-server-login.php?site_id={GWARS_SITE_ID}&url={callback_url}"
        logger.debug(f"Redirecting to GWars: {gwars_url}")
        logger.debug(f"Callback URL: {callback_url}")
        return redirect(gwars_url)
    
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
        flash('Ошибка проверки подписи sign4 (устаревшая подпись)', 'error')
        return redirect(url_for('index'))
    
    # Сохраняем пользователя в БД
    conn = get_db_connection()
    try:
        # Проверяем, существует ли пользователь и получаем все его данные
        existing_user = conn.execute('SELECT avatar_seed, avatar_style, bio, contact_info FROM users WHERE user_id = ?', (user_id,)).fetchone()
        
        # Если пользователь новый, генерируем уникальный avatar_seed
        avatar_seed = None
        avatar_style = None
        bio = None
        contact_info = None
        
        if not existing_user:
            # Новый пользователь - генерируем рандомный аватар
            avatar_seed = generate_unique_avatar_seed(user_id)
            avatar_style = 'avataaars'  # Стиль по умолчанию
        elif existing_user and not existing_user['avatar_seed']:
            # Если у существующего пользователя нет seed, генерируем
            avatar_seed = generate_unique_avatar_seed(user_id)
            avatar_style = existing_user['avatar_style'] or 'avataaars'
            bio = existing_user['bio']
            contact_info = existing_user['contact_info']
        else:
            # Используем существующий seed и сохраняем все пользовательские данные
            avatar_seed = existing_user['avatar_seed']
            avatar_style = existing_user['avatar_style']
            bio = existing_user['bio']
            contact_info = existing_user['contact_info']
        
        # Обновляем данные пользователя, сохраняя пользовательские поля (avatar, bio, contact_info)
        conn.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, last_login)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, name, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, datetime.now()))
        conn.commit()
        log_debug(f"User saved successfully: user_id={user_id}, username={name}, avatar_seed={avatar_seed}")
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
                return redirect(url_for('index'))
        else:
            flash(f'Ошибка сохранения пользователя: {str(e)}', 'error')
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
    
    return redirect(url_for('dashboard'))

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
                # Обновляем с новым аватаром
                conn.execute('''
                    UPDATE users 
                    SET bio = ?, contact_info = ?, avatar_style = ?, avatar_seed = ?
                    WHERE user_id = ?
                ''', (bio, contact_info, avatar_style, avatar_seed, session['user_id']))
            else:
                # Обновляем только bio и contact_info
                conn.execute('''
                    UPDATE users 
                    SET bio = ?, contact_info = ?
                    WHERE user_id = ?
                ''', (bio, contact_info, session['user_id']))
            
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
@require_login
def view_profile(user_id):
    """Просмотр профиля другого пользователя"""
    conn = get_db_connection()
    
    # Получаем данные пользователя
    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (user_id,)
    ).fetchone()
    
    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('participants'))
    
    # Получаем роли пользователя
    user_roles = get_user_roles(user_id)
    
    conn.close()
    
    # Проверяем, является ли это профилем текущего пользователя
    is_own_profile = session.get('user_id') == user_id
    
    return render_template('view_profile.html', user=user, user_roles=user_roles, is_own_profile=is_own_profile)

@app.route('/participants')
def participants():
    """Страница со списком участников"""
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
        ORDER BY u.created_at DESC
    ''').fetchall()
    
    # Для каждого пользователя определяем статус
    participants_data = []
    for user in users:
        # Определяем статус: онлайн (если был вход сегодня) или оффлайн
        last_login = user['last_login']
        status = 'Оффлайн'
        if last_login:
            try:
                last_login_date = datetime.strptime(last_login.split('.')[0], '%Y-%m-%d %H:%M:%S')
                now = datetime.now()
                if (now - last_login_date).total_seconds() < 3600:  # Меньше часа
                    status = 'Онлайн'
                elif (now - last_login_date).days == 0:  # Сегодня
                    status = 'Был сегодня'
            except:
                pass
        
        participants_data.append({
            'user_id': user['user_id'],
            'username': user['username'],
            'avatar_seed': user['avatar_seed'],
            'avatar_style': user['avatar_style'],
            'status': status,
            'roles': user['roles'] or 'Пользователь',
            'created_at': user['created_at']
        })
    
    conn.close()
    
    return render_template('participants.html', participants=participants_data)

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
    return render_template('admin/role_form.html', permissions=permissions)

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
    conn.close()
    return render_template('admin/role_form.html', role=role, permissions=permissions)

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
        
        # Обновляем настройки
        settings_dict = {}
        for key in request.form:
            if key.startswith('setting_'):
                setting_key = key.replace('setting_', '')
                setting_value = request.form.get(key)
                settings_dict[setting_key] = setting_value
        
        # Сохраняем настройки
        for key, value in settings_dict.items():
            try:
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
        SELECT * FROM settings ORDER BY category, key
    ''').fetchall()
    
    # Группируем по категориям
    settings_by_category = {}
    for setting in settings:
        category = setting['category'] or 'general'
        if category not in settings_by_category:
            settings_by_category[category] = []
        settings_by_category[category].append(dict(setting))
    
    conn.close()
    
    return render_template('admin/settings.html', settings_by_category=settings_by_category)

def get_setting(key, default=None):
    """Получает значение настройки из БД"""
    conn = get_db_connection()
    setting = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    conn.close()
    return setting['value'] if setting and setting['value'] else default

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
    {'type': 'gift_sending', 'name': 'Отправка подарков', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'celebration_date', 'name': 'Дата праздника', 'required': True, 'has_start': True, 'has_end': False},
    {'type': 'after_party', 'name': 'Послепраздничное настроение', 'required': True, 'has_start': False, 'has_end': True},
]

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
                    continue
            
            # Если этап еще не начался, пропускаем
            if now < start_dt:
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
        
        # Если этап не имеет даты начала, но есть следующий этап с датой начала
        # Проверяем, не начался ли следующий этап
        if not stage['start_datetime']:
            # Ищем следующий этап с датой начала
            next_stage_started = False
            current_order = stage['stage_order']
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
                        break
            if next_stage_started:
                continue
        
        # Этот этап активен
        current_stage = {
            'data': stage,
            'info': stage_info
        }
        break
    
    return current_stage

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
    
    return render_template('events.html', events_with_stages=events_with_stages)

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
        
        if not name:
            flash('Название мероприятия обязательно', 'error')
            return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES)
        
        conn = get_db_connection()
        try:
            # Создаем мероприятие
            cursor = conn.execute('''
                INSERT INTO events (name, description, created_by)
                VALUES (?, ?, ?)
            ''', (name, description, session.get('user_id')))
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
                            start_datetime = datetime.strptime(start_str, '%Y-%m-%dT%H:%M')
                        except:
                            pass
                
                if stage['has_end']:
                    end_str = request.form.get(f"stage_{stage['type']}_end", '').strip()
                    if end_str:
                        try:
                            end_datetime = datetime.strptime(end_str, '%Y-%m-%dT%H:%M')
                        except:
                            pass
                
                # Проверяем обязательность
                is_required = 1 if stage['required'] else 0
                is_optional = 1 if not stage['required'] else 0
                
                # Для обязательных этапов проверяем наличие даты начала
                if stage['required'] and stage['has_start'] and not start_datetime:
                    flash(f'Дата начала этапа "{stage["name"]}" обязательна', 'error')
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES)
                
                conn.execute('''
                    INSERT INTO event_stages 
                    (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (event_id, stage['type'], stage_order, start_datetime, end_datetime, is_required, is_optional))
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
    
    return render_template('admin/event_form.html', event=None, stages=EVENT_STAGES)

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
        
        if not name:
            flash('Название мероприятия обязательно', 'error')
            conn.close()
            return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict)
        
        try:
            # Обновляем мероприятие
            conn.execute('''
                UPDATE events 
                SET name = ?, description = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (name, description, event_id))
            
            # Обновляем этапы
            for stage in EVENT_STAGES:
                start_datetime = None
                end_datetime = None
                
                if stage['has_start']:
                    start_str = request.form.get(f"stage_{stage['type']}_start", '').strip()
                    if start_str:
                        try:
                            start_datetime = datetime.strptime(start_str, '%Y-%m-%dT%H:%M')
                        except:
                            pass
                
                if stage['has_end']:
                    end_str = request.form.get(f"stage_{stage['type']}_end", '').strip()
                    if end_str:
                        try:
                            end_datetime = datetime.strptime(end_str, '%Y-%m-%dT%H:%M')
                        except:
                            pass
                
                # Проверяем обязательность
                if stage['required'] and stage['has_start'] and not start_datetime:
                    flash(f'Дата начала этапа "{stage["name"]}" обязательна', 'error')
                    conn.rollback()
                    conn.close()
                    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict)
                
                # Обновляем или создаем этап
                if stage['type'] in stages_dict:
                    conn.execute('''
                        UPDATE event_stages 
                        SET start_datetime = ?, end_datetime = ?
                        WHERE event_id = ? AND stage_type = ?
                    ''', (start_datetime, end_datetime, event_id, stage['type']))
                else:
                    stage_order = len(stages_dict) + 1
                    conn.execute('''
                        INSERT INTO event_stages 
                        (event_id, stage_type, stage_order, start_datetime, end_datetime, is_required, is_optional)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (event_id, stage['type'], stage_order, start_datetime, end_datetime, 
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
    
    conn.close()
    return render_template('admin/event_form.html', event=event, stages=EVENT_STAGES, existing_stages=stages_dict)

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

