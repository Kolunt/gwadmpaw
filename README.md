# GWars Auth Test Project

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

## Развертывание на PythonAnywhere

### 1. Загрузите файлы на PythonAnywhere

Используйте Git или загрузите файлы через веб-интерфейс:
- `app.py`
- `requirements.txt`
- папку `templates/`
- папку `static/`

### 2. Установите зависимости

В консоли PythonAnywhere:
```bash
pip3.10 install --user -r requirements.txt
```

### 3. Настройте WSGI файл

В файле `wsgi.py` (обычно находится в `/var/www/yourusername_pythonanywhere_com_wsgi.py`):

```python
import sys
import os

path = '/home/yourusername/gwadmpaw'
if path not in sys.path:
    sys.path.insert(0, path)

from app import app as application

if __name__ == "__main__":
    application.run()
```

### 4. Настройте базу данных

При первом запуске приложение автоматически создаст `database.db`. Убедитесь, что у файла есть права на запись.

### 5. Настройте домен в GWars

В файле `app.py` измените `GWARS_HOST` на ваш домен PythonAnywhere:
```python
GWARS_HOST = "yourusername.pythonanywhere.com"
```

### 6. Перезагрузите веб-приложение

Нажмите кнопку "Reload" в панели управления PythonAnywhere.

## Структура проекта

```
gwadmpaw/
├── app.py              # Основное Flask приложение
├── requirements.txt    # Зависимости Python
├── README.md          # Документация
├── .gitignore         # Игнорируемые файлы
├── database.db        # SQLite база данных (создается автоматически)
├── templates/         # HTML шаблоны
│   ├── base.html
│   ├── index.html
│   └── dashboard.html
└── static/            # Статические файлы
    ├── css/
    │   └── style.css
    └── js/
        └── theme.js
```

## Авторизация через GWars

### Как это работает:

1. Пользователь нажимает "Войти через GWars"
2. Происходит редирект на `https://www.gwars.io/cross-server-login.php` с параметрами:
   - `site_id=4`
   - `url=https://yourdomain.pythonanywhere.com/login`
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

