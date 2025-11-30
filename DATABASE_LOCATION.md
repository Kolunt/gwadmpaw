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
# В конце app.py
try:
    init_db()
except Exception as e:
    log_error(f"Failed to initialize database on startup: {e}")
```

## Подключение к базе данных

### Функция `get_db_connection()`

```python
def get_db_connection():
    """Получает соединение с базой данных"""
    ensure_db()  # Убеждаемся, что БД инициализирована
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Возвращает строки как словари
    return conn
```

**Особенности:**
- Автоматически инициализирует БД, если её нет
- Использует `sqlite3.Row` для удобного доступа к данным
- Возвращает готовое соединение

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

### Рекомендации по резервному копированию

#### На PythonAnywhere

1. **Через консоль:**
```bash
cd ~/gwadm
cp database.db database.db.backup
```

2. **Через веб-интерфейс:**
- Files → `~/gwadm/database.db` → Download

3. **Автоматическое резервное копирование:**
```bash
# Создать скрипт для ежедневного бэкапа
cd ~/gwadm
cp database.db backups/database_$(date +%Y%m%d).db
```

#### Локально

```powershell
# Windows PowerShell
Copy-Item database.db database.db.backup

# Или с датой
Copy-Item database.db "database_$(Get-Date -Format 'yyyyMMdd').db"
```

## Миграции

База данных поддерживает автоматические миграции через `ALTER TABLE`:

```python
# Пример миграции (добавление колонки)
try:
    c.execute('ALTER TABLE users ADD COLUMN avatar_seed TEXT')
except sqlite3.OperationalError:
    # Колонка уже существует, это нормально
    pass
```

**Особенности:**
- Миграции выполняются при инициализации БД
- Безопасные (не падают, если колонка уже существует)
- Автоматические (не требуют ручного вмешательства)

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

