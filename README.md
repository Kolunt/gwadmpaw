# Анонимные Деды Морозы

**Версия: 1.20.0**

Тестовый проект для авторизации через GWars на PythonAnywhere.

## Технологии

- **Backend**: Flask (Python)
- **Frontend**: HTML/CSS/JavaScript с мобильной адаптацией и dark/light mode
- **База данных**: SQLite

## Установка локально

```bash
pip install -r requirements.txt
python app.py
```

Приложение будет доступно по адресу `http://localhost:5000`

### Проверка перед деплоем

```bash
python scripts/smoke_check.py
python scripts/verify_gwars_domains.py
```

## Конфигурация

Настройки загружаются из переменных окружения (модуль [`gwadm/config.py`](gwadm/config.py)). Шаблон для `.env` на сервере: [`.env.example`](.env.example).

| Переменная | Назначение |
|------------|------------|
| `SECRET_KEY` | Ключ сессий Flask (обязательно на проде) |
| `GWARS_PASSWORD` | Пароль подписей GWars |
| `DATABASE_PATH` | Путь к файлу SQLite |
| `CRON_SECRET_TOKEN` | Защита endpoint `/cron/run` |
| `FLASK_ENV` / `FLASK_DEBUG` | Режим prod/dev и уровень логов |
| `EVENT_TIME_OFFSET_HOURS` | Смещение «сейчас» для этапов мероприятий |

Слой приложения: [`gwadm/factory.py`](gwadm/factory.py) (`create_app()`), [`gwadm/extensions.py`](gwadm/extensions.py), [`gwadm/i18n.py`](gwadm/i18n.py), [`gwadm/db.py`](gwadm/db.py), [`gwadm/logging_config.py`](gwadm/logging_config.py). Entry point для gunicorn: `app:app` (корневой [`app.py`](app.py) вызывает `create_app()` и регистрирует маршруты).

## Развертывание на PythonAnywhere

📖 **Подробная инструкция по развертыванию на gwadm.pythonanywhere.com** находится в файле [DEPLOYMENT.md](DEPLOYMENT.md)

### ⚠️ Важно: названия репозитория и папки

- **Репозиторий GitHub**: `gwadmpaw`
- **Папка на PythonAnywhere**: `gwadm`

### Краткая инструкция:

1. **Клонируйте репозиторий** на PythonAnywhere с указанием имени папки:
```bash
cd ~
git clone https://github.com/Kolunt/gwadmpaw.git gwadm
```

2. **Установите зависимости**:
```bash
cd ~/gwadm
pip3.10 install --user -r requirements.txt
```

3. **Настройте WSGI файл** (см. [DEPLOYMENT.md](DEPLOYMENT.md) для подробностей)

4. **Настройте Static files** в разделе Web панели управления

5. **Перезагрузите веб-приложение** через панель управления

**Важно**: Карта доменов GWars настраивается в админке (**Настройки → Интеграции → GWars**) или в `settings.gwars_domain_map`. Подробнее: [GWARS_DOMAINS.md](GWARS_DOMAINS.md).

## Обновление проекта

Для обновления кода на сервере:

```bash
cd ~/gwadm
git pull origin main
# Если появились новые зависимости:
pip3.10 install --user -r requirements.txt
# Затем перезагрузите веб-приложение через панель управления
```

📖 **Подробная инструкция по обновлению** находится в [DEPLOYMENT.md](DEPLOYMENT.md#обновление-проекта)

## Структура проекта

```
gwadmpaw/
├── app.py              # Маршруты; entry point gunicorn app:app
├── gwadm/              # Пакет приложения
│   ├── factory.py      # create_app()
│   ├── extensions.py   # Babel, hook для blueprints
│   ├── i18n.py         # Локализация
│   ├── config.py       # Настройки из env
│   ├── db.py           # SQLite, ensure_db, get_db_connection
│   ├── logging_config.py
│   ├── decorators.py   # require_login, require_role
│   ├── services/       # avatars, awards, events_stages, profile_comments, gwars_auth, roles, settings, titles, activity
│   └── blueprints/     # auth, meta, public, profile (dashboard, edit, view)
├── gwars_domains.py    # Shim → gwadm.services.gwars_domains
├── scripts/
│   ├── smoke_check.py
│   └── verify_gwars_domains.py
├── requirements.txt
├── database.db         # SQLite (создаётся автоматически)
├── templates/
└── static/
```

## Авторизация через GWars

### Как это работает:

1. Пользователь нажимает "Войти через GWars"
2. Происходит редирект на `https://www.gwars.io/cross-server-login.php` с параметрами:
   - `site_id` — по текущему домену (для `gwadm.ru` это `3`, см. [GWARS_DOMAINS.md](GWARS_DOMAINS.md))
   - `url=https://{текущий-домен}/login`
3. GWars проверяет авторизацию пользователя
4. Если пользователь авторизован, GWars перенаправляет на `/login` с параметрами:
   - `sign` - подпись (md5(password + username + user_id))
   - `name` - имя пользователя
   - `user_id` - ID пользователя
   - `level` - уровень бойца
   - `synd` - синдикат
   - `sign2` - вторая подпись (md5(password + level + synd + user_id))
   - `has_passport`, `has_mobile`, `old_passport` - флаги
   - `sign3` - третья подпись (первые 10 символов md5)
   - `usersex` - пол пользователя
   - `sign4` - подпись даты (первые 10 символов md5)
5. Приложение проверяет все подписи для безопасности
6. Если подписи верны, пользователь авторизуется и данные сохраняются в БД

## Особенности

- ✅ Полная проверка всех подписей (sign, sign2, sign3, sign4)
- ✅ Защита от подделки данных через проверку подписей
- ✅ Сохранение пользователей в SQLite базе данных
- ✅ Адаптивный дизайн для мобильных устройств
- ✅ Dark/Light mode с сохранением выбора в localStorage
- ✅ Современный и чистый UI

## Безопасность

- Все подписи проверяются на сервере
- Пароль `deadmoroz` используется только для проверки подписей
- Данные пользователя сохраняются в БД после успешной проверки
- Сессии используются для управления авторизацией

Маршруты вынесены в blueprints: `events`, `assignments`, `admin`, `integrations`; точка входа — `app.py` + `gwadm.create_app()`.
