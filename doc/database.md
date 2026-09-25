# Расположение базы данных

## Обзор

Проект использует **SQLite** базу данных для хранения всех данных. Путь к базе данных определяется автоматически в зависимости от окружения (локальное или PythonAnywhere).

## Функция определения пути

### `get_db_path()`

Функция находится в `app.py` (строки 273-284) и определяет путь к базе данных:

```python
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
```

## Логика определения пути

### 1. Проверка окружения

Функция проверяет наличие директории `/home/gwadm`:
- **Если существует** → окружение PythonAnywhere
- **Если не существует** → локальное окружение

### 2. Пути в зависимости от окружения

#### На PythonAnywhere (продакшн)
```
/home/gwadm/gwadm/database.db
```

**Полный путь:** `/home/gwadm/gwadm/database.db`

**Где:**
- `/home/gwadm/` - домашняя директория пользователя на PythonAnywhere
- `gwadm/` - папка проекта (не `gwadmpaw`!)
- `database.db` - файл базы данных

#### Локально (разработка)
```
<директория_проекта>/database.db
```

**Относительный путь:** `database.db` (в корне проекта)

**Абсолютный путь (пример):**
- Windows: `C:\Users\TBG\Documents\gwadmpaw\database.db`
- Linux/Mac: `/home/user/gwadmpaw/database.db`

## Текущее расположение

### Локально (ваш компьютер)
```
C:\Users\TBG\Documents\gwadmpaw\database.db
```

**Размер:** ~368 KB (376,832 байт)  
**Последнее изменение:** 01.12.2025 1:26:38

### На PythonAnywhere (продакшн)
```
/home/gwadm/gwadm/database.db
```

## Создание базы данных

### Автоматическое создание

База данных создаётся автоматически при первом запуске приложения:

```python
def init_db():
    """Инициализирует базу данных, создавая таблицы если их нет"""
    db_path = get_db_path()
    
    # Создаем директорию если её нет
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    # ... создание таблиц ...
```

### Инициализация при старте

База данных инициализируется при импорте модуля:

```python
# В конце app.py (при импорте WSGI)
try:
    ensure_db()
except Exception as e:
    log_error(f"Failed to initialize database on startup: {e}")
```

Скрипт `cron_tasks.py` также вызывает `ensure_db()` в начале `main()`.

## Подключение к базе данных

### Функция `get_db_connection()` ([`gwadm/db.py`](gwadm/db.py))

```python
def get_db_connection():
    """Получает соединение с базой данных"""
    if not _db_initialized:
        raise RuntimeError('Call ensure_db() at startup')
    db_path = get_db_path()
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn
```

**Особенности:**
- Требует предварительный `ensure_db()` при старте процесса
- Использует `sqlite3.Row` для удобного доступа к данным
- WAL mode и timeout=30 для многопоточного gunicorn

## Структура базы данных

### Основные таблицы

1. **users** - пользователи
2. **events** - мероприятия
3. **event_registrations** - регистрации на мероприятия
4. **event_assignments** - назначения (кто кому дарит)
5. **roles** - роли
6. **permissions** - права доступа
7. **user_roles** - связь пользователей и ролей
8. **titles** - звания
9. **user_titles** - связь пользователей и званий
10. **awards** - награды
11. **user_awards** - связь пользователей и наград
12. **settings** - настройки системы
13. **activity_logs** - логи действий
14. **faq_items** - FAQ
15. **letter_messages** - сообщения/письма
16. И другие...

## Резервное копирование

### Важно!

База данных **НЕ включена** в Git (см. `.gitignore`):

```
*.db
*.sqlite
*.sqlite3
```

### Резервное копирование

На production (gwadm.ru) бэкапы создаются **ежедневно** через user systemd timer `gwadm-backup.timer`.

| Параметр | Значение |
|----------|----------|
| Скрипт | [`scripts/backup_db.py`](../scripts/backup_db.py) → `backup_database()` в [`cron_tasks.py`](../cron_tasks.py) |
| Каталог | `~/gwadm/backups/` (или `BACKUP_DIR` в `.env`) |
| Имя файла | `database_YYYYMMDD_HHMMSS.db` |
| Ротация | последние **7** копий |

**Установка timer** (один раз на сервере):

```bash
mkdir -p ~/.config/systemd/user
cp ~/gwadm/deploy/gwadm-backup.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gwadm-backup.timer
```

Ручной прогон: `systemctl --user start gwadm-backup.service` или `python scripts/backup_db.py`.

Альтернатива: HTTP `GET /cron/run?token=...&backup=1` (тот же `backup_database()`).

Каталог `backups/` в [`.gitignore`](../.gitignore). Старые `database.db.backup_*` в корне репо можно удалить вручную после перехода на timer.

#### Локально

```bash
python scripts/backup_db.py
```

```powershell
Copy-Item database.db "database_$(Get-Date -Format 'yyyyMMdd').db"
```

## Миграции

Схема БД версионируется через каталог [`migrations/`](migrations/) и таблицу `schema_version`. При старте приложения [`gwadm/migrations/runner.py`](gwadm/migrations/runner.py) вызывается из `init_db()`.

| Файл | Назначение |
|------|------------|
| `001_schema_version.sql` | Таблица `schema_version` |
| `002_initial_schema.sql` | `CREATE TABLE IF NOT EXISTS` для всех таблиц |
| `003_legacy_columns.py` | `ALTER TABLE`, индексы, миграция `snowflake_events.points` |
| `004_seed_data.py` | Роли, права, настройки по умолчанию, seed-данные |

**Существующая БД (прод):** если есть таблица `users`, но `schema_version` пуста — runner **штампует** текущую версию без повторного прогона legacy-ALTER (bootstrap).

**Новая миграция:** добавить `005_description.sql` (или `.py` с `upgrade(conn)`) и увеличить `CURRENT_VERSION` в `gwadm/migrations/runner.py`.

```bash
python -m pytest tests/test_migrations.py -q
```

## Проверка базы данных

### Локально

```powershell
# Проверить существование
Test-Path database.db

# Получить информацию
Get-Item database.db | Select-Object FullName, Length, LastWriteTime
```

### На PythonAnywhere

```bash
# Проверить существование
ls -lh ~/gwadm/database.db

# Получить размер
du -h ~/gwadm/database.db

# Проверить права доступа
ls -la ~/gwadm/database.db
```

### Через SQLite CLI

```bash
# Локально
sqlite3 database.db ".tables"

# На PythonAnywhere
sqlite3 ~/gwadm/database.db ".tables"
```

## Права доступа

### На PythonAnywhere

Убедитесь, что у базы данных правильные права:

```bash
chmod 644 ~/gwadm/database.db
chmod 755 ~/gwadm
```

### Локально

Обычно права устанавливаются автоматически, но можно проверить:

```powershell
# Windows
icacls database.db
```

## Перемещение базы данных

### Изменение пути

Если нужно изменить путь к базе данных, отредактируйте функцию `get_db_path()`:

```python
def get_db_path():
    global _db_path
    if _db_path is None:
        # Ваш кастомный путь
        _db_path = '/custom/path/to/database.db'
    return _db_path
```

### Перемещение существующей БД

1. **Остановите приложение**
2. **Скопируйте файл:**
   ```bash
   cp /old/path/database.db /new/path/database.db
   ```
3. **Обновите `get_db_path()`**
4. **Запустите приложение**

## Отладка

### Логи инициализации

При инициализации БД выводятся логи:

```python
log_debug(f"Initializing database at: {db_path}")
log_debug(f"Database initialized successfully at: {db_path}")
```

### Проверка пути в коде

Можно добавить временную отладку:

```python
def get_db_path():
    global _db_path
    if _db_path is None:
        # ... логика определения пути ...
    print(f"Database path: {_db_path}")  # Отладка
    return _db_path
```

## Безопасность

### Важные моменты

1. **База данных НЕ в Git** - файлы `.db` игнорируются
2. **Путь определяется автоматически** - не нужно настраивать вручную
3. **Автоматическое создание директорий** - если директории нет, она создаётся
4. **Защита от потери данных** - при сохранении назначений сохраняется старое состояние

### Рекомендации

- ✅ Регулярно делайте резервные копии
- ✅ Храните бэкапы в безопасном месте
- ✅ Не коммитьте базу данных в Git
- ✅ Проверяйте права доступа на сервере

## Заключение

База данных хранится:
- **Локально:** в корне проекта как `database.db`
- **На PythonAnywhere:** в `/home/gwadm/gwadm/database.db`

Путь определяется автоматически функцией `get_db_path()`, которая проверяет окружение и возвращает соответствующий путь.

