"""SQLite database connection and schema initialization."""

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from gwadm.services.gwars_domains import DEFAULT_GWARS_DOMAIN_MAP

from gwadm.config import ADMIN_USER_IDS, DATABASE_PATH
from gwadm.logging_config import log_debug, log_error

_db_initialized = False
_db_path = None
_db_init_lock = threading.Lock()

def get_db_path():
    """Return path to the SQLite database file."""
    global _db_path
    if _db_path is None:
        _db_path = DATABASE_PATH
    return _db_path

def _database_is_ready():
    """Проверяет, что БД уже инициализирована и доступна для чтения."""
    conn = None
    try:
        conn = sqlite3.connect(get_db_path(), timeout=30)
        conn.execute('SELECT 1 FROM settings LIMIT 1')
        return True
    except Exception:
        return False
    finally:
        if conn:
            conn.close()


def init_db():
    """Инициализирует базу данных, создавая таблицы если их нет"""
    global _db_initialized
    if _db_initialized:
        return

    conn = None
    try:
        db_path = get_db_path()
        log_debug(f"Initializing database at: {db_path}")
        
        # Создаем директорию если её нет
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        conn = sqlite3.connect(db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
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
        # Миграция: добавляем поля для индивидуальных настроек рейтинга мероприятия
        try:
            c.execute('ALTER TABLE events ADD COLUMN rating_registration INTEGER')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
        try:
            c.execute('ALTER TABLE events ADD COLUMN rating_gift_not_sent INTEGER')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
        try:
            c.execute('ALTER TABLE events ADD COLUMN rating_gift_sent INTEGER')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
        try:
            c.execute('ALTER TABLE events ADD COLUMN rating_order_coefficient REAL')
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
        
        # Таблица начислений «бубенчиков»
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
        # Миграция: изменяем тип points на REAL для поддержки десятичных значений
        # В SQLite нельзя напрямую изменить тип колонки, но можно создать новую таблицу
        try:
            # Проверяем, есть ли уже колонка points и какой у неё тип
            c.execute('PRAGMA table_info(snowflake_events)')
            columns = c.fetchall()
            points_col = next((col for col in columns if col[1] == 'points'), None)
            if points_col:
                col_type = points_col[2].upper()
                if col_type == 'INTEGER':
                    log_debug("Migrating points column from INTEGER to REAL")
                    # Создаем временную таблицу с REAL для points
                    c.execute('''
                        CREATE TABLE snowflake_events_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER NOT NULL,
                            source TEXT NOT NULL,
                            reason TEXT NOT NULL,
                            points REAL NOT NULL DEFAULT 1,
                            active INTEGER DEFAULT 1,
                            manual_revoked INTEGER DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            revoked_at TIMESTAMP,
                            FOREIGN KEY (user_id) REFERENCES users(user_id),
                            UNIQUE(user_id, source)
                        )
                    ''')
                    # Копируем данные
                    c.execute('''
                        INSERT INTO snowflake_events_new 
                        SELECT id, user_id, source, reason, CAST(points AS REAL) as points, active, manual_revoked, created_at, updated_at, revoked_at
                        FROM snowflake_events
                    ''')
                    # Удаляем старую таблицу
                    c.execute('DROP TABLE snowflake_events')
                    # Переименовываем новую таблицу
                    c.execute('ALTER TABLE snowflake_events_new RENAME TO snowflake_events')
                    log_debug("Points column migration completed successfully")
        except sqlite3.OperationalError as e:
            # Если миграция не удалась, продолжаем работу
            log_debug(f"Points column migration skipped: {e}")
            pass
        try:
            c.execute('ALTER TABLE snowflake_events ADD COLUMN manual_revoked INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        
        # Добавляем индексы для ускорения запросов рейтинга
        try:
            c.execute('CREATE INDEX IF NOT EXISTS idx_snowflake_events_user_id ON snowflake_events(user_id)')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('CREATE INDEX IF NOT EXISTS idx_snowflake_events_active ON snowflake_events(active)')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('CREATE INDEX IF NOT EXISTS idx_snowflake_events_manual_revoked ON snowflake_events(manual_revoked)')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('CREATE INDEX IF NOT EXISTS idx_snowflake_events_rating ON snowflake_events(active, manual_revoked, user_id)')
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
        
        # Таблица для архивных чатов (расформированных пар)
        c.execute('''
            CREATE TABLE IF NOT EXISTS assignment_chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_assignment_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                santa_user_id INTEGER NOT NULL,
                recipient_user_id INTEGER NOT NULL,
                archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                archived_by INTEGER,
                notes TEXT,
                FOREIGN KEY (event_id) REFERENCES events(id),
                FOREIGN KEY (santa_user_id) REFERENCES users(user_id),
                FOREIGN KEY (recipient_user_id) REFERENCES users(user_id),
                FOREIGN KEY (archived_by) REFERENCES users(user_id)
            )
        ''')
        
        # Добавляем поле is_archived в event_assignments для пометки расформированных пар
        try:
            c.execute('ALTER TABLE event_assignments ADD COLUMN is_archived INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        
        # Таблица комментариев администраторов к профилям пользователей
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_admin_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                admin_user_id INTEGER,
                comment TEXT NOT NULL,
                is_admin_only INTEGER DEFAULT 0,
                is_thanks_from_recipient INTEGER DEFAULT 0,
                assignment_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (admin_user_id) REFERENCES users(user_id),
                FOREIGN KEY (assignment_id) REFERENCES event_assignments(id)
            )
        ''')
        
        # Миграция: добавляем новые поля в существующую таблицу
        try:
            c.execute('ALTER TABLE user_admin_comments ADD COLUMN is_admin_only INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE user_admin_comments ADD COLUMN is_thanks_from_recipient INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass
        try:
            c.execute('ALTER TABLE user_admin_comments ADD COLUMN assignment_id INTEGER')
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
            ('gwars_domain_map', json.dumps(DEFAULT_GWARS_DOMAIN_MAP, ensure_ascii=False), 'Карта доменов-зеркал GWars (JSON)', 'integrations'),
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
        
        # Миграция устаревших настроек GWars в gwars_domain_map
        existing_map = c.execute(
            'SELECT value FROM settings WHERE key = ?', ('gwars_domain_map',)
        ).fetchone()
        legacy_host = c.execute(
            'SELECT value FROM settings WHERE key = ?', ('gwars_host',)
        ).fetchone()
        legacy_site_id = c.execute(
            'SELECT value FROM settings WHERE key = ?', ('gwars_site_id',)
        ).fetchone()

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
        
        # Инициализируем настройки рейтинга по умолчанию
        try:
            rating_defaults = [
                ('rating_contact_telegram', '1', 'Очки за заполненный Telegram', 'rating'),
                ('rating_contact_whatsapp', '1', 'Очки за заполненный WhatsApp', 'rating'),
                ('rating_contact_viber', '1', 'Очки за заполненный Viber', 'rating'),
                ('rating_event_registration', '1', 'Очки за регистрацию на мероприятие (начисляются автоматически всем зарегистрированным участникам при закрытии регистрации администратором)', 'rating'),
                ('rating_event_gift_not_sent', '0', 'Очки за неотправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации не отправили подарок)', 'rating'),
                ('rating_event_gift_sent', '0', 'Очки за отправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации отправили подарок)', 'rating'),
            ]
            for key, value, description, category in rating_defaults:
                existing = c.execute('SELECT key FROM settings WHERE key = ?', (key,)).fetchone()
                if not existing:
                    c.execute('''
                        INSERT INTO settings (key, value, description, category)
                        VALUES (?, ?, ?, ?)
                    ''', (key, value, description, category))
        except Exception as e:
            log_error(f"Error initializing rating settings: {e}")
        
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
        
        # Инициализируем настройки рейтинга по умолчанию
        try:
            rating_defaults = [
                ('rating_contact_telegram', '1', 'Очки за заполненный Telegram', 'rating'),
                ('rating_contact_whatsapp', '1', 'Очки за заполненный WhatsApp', 'rating'),
                ('rating_contact_viber', '1', 'Очки за заполненный Viber', 'rating'),
                ('rating_event_registration', '1', 'Очки за регистрацию на мероприятие (начисляются автоматически всем зарегистрированным участникам при закрытии регистрации администратором)', 'rating'),
                ('rating_event_gift_not_sent', '0', 'Очки за неотправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации не отправили подарок)', 'rating'),
                ('rating_event_gift_sent', '0', 'Очки за отправленный подарок (начисляются автоматически участникам, которые на момент закрытия регистрации отправили подарок)', 'rating'),
            ]
            for key, value, description, category in rating_defaults:
                existing = c.execute('SELECT key FROM settings WHERE key = ?', (key,)).fetchone()
                if not existing:
                    c.execute('''
                        INSERT INTO settings (key, value, description, category)
                        VALUES (?, ?, ?, ?)
                    ''', (key, value, description, category))
            conn.commit()
        except Exception as e:
            log_error(f"Error initializing rating settings: {e}")
        
        conn.commit()
        _db_initialized = True
        log_debug(f"Database initialized successfully at: {db_path}")
    except sqlite3.OperationalError as e:
        if 'locked' in str(e).lower() and _database_is_ready():
            _db_initialized = True
            log_debug("Database already initialized by another worker")
        else:
            log_error(f"Error initializing database: {e}")
            raise
    except Exception as e:
        log_error(f"Error initializing database: {e}")
        raise
    finally:
        if conn:
            conn.close()
def ensure_db():
    """Убеждается, что база данных инициализирована"""
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
        for attempt in range(5):
            try:
                init_db()
                return
            except sqlite3.OperationalError as e:
                if 'locked' not in str(e).lower():
                    raise
                if _database_is_ready():
                    _db_initialized = True
                    return
                if attempt < 4:
                    time.sleep(0.25 * (attempt + 1))
                    continue
                raise


def is_database_initialized() -> bool:
    """Return True after ensure_db() has completed in this process."""
    return _db_initialized


def get_db_connection():
    """Получает соединение с базой данных"""
    if not _db_initialized:
        raise RuntimeError(
            'Database is not initialized. Call ensure_db() at application or cron startup.'
        )
    db_path = get_db_path()
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

@contextmanager
def get_db():
    """Context manager for a database connection (caller commits explicitly)."""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()
