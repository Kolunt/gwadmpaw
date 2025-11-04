# Инструкция по развертыванию на PythonAnywhere

## Развертывание на gwadm.pythonanywhere.com

### Шаг 1: Подготовка файлов

1. **Клонируйте репозиторий** на PythonAnywhere через консоль:
```bash
cd ~
git clone https://github.com/Kolunt/gwadmpaw.git
```

Или загрузите файлы через **Files** → **Upload a file** в панели управления PythonAnywhere.

### Шаг 2: Установка зависимостей

1. Откройте консоль **Bash** в панели управления PythonAnywhere
2. Перейдите в директорию проекта:
```bash
cd ~/gwadmpaw
```

3. Установите зависимости (для Python 3.10):
```bash
pip3.10 install --user -r requirements.txt
```

Если используете другую версию Python, замените `3.10` на нужную версию (например, `3.9`, `3.11`).

### Шаг 3: Настройка WSGI файла

1. В панели управления PythonAnywhere перейдите в раздел **Web**
2. Нажмите на ссылку для редактирования WSGI файла (обычно находится по пути `/var/www/gwadm_pythonanywhere_com_wsgi.py`)

3. Замените содержимое файла на:
```python
import sys
import os

# Добавляем путь к проекту
path = '/home/gwadm/gwadmpaw'
if path not in sys.path:
    sys.path.insert(0, path)

# Импортируем приложение Flask
from app import app as application

# Эта переменная нужна для WSGI
if __name__ == "__main__":
    application.run()
```

**Важно**: Если ваше имя пользователя на PythonAnywhere не `gwadm`, замените `/home/gwadm/` на правильный путь (например, `/home/ваш_username/`).

### Шаг 4: Настройка статических файлов

1. В разделе **Web** → **Static files** найдите секцию **Static files**
2. Добавьте следующие записи:

| URL | Directory |
|-----|-----------|
| `/static/` | `/home/gwadm/gwadmpaw/static/` |

**Важно**: Замените `gwadm` на ваше имя пользователя, если оно отличается.

### Шаг 5: Проверка конфигурации

Убедитесь, что в файле `app.py` правильно указан домен:
```python
GWARS_HOST = "gwadm.pythonanywhere.com"
```

Это должно быть уже настроено, но проверьте на всякий случай.

### Шаг 6: Создание базы данных

База данных `database.db` создастся автоматически при первом запуске приложения. Убедитесь, что у вашей директории есть права на запись:

```bash
chmod 755 ~/gwadmpaw
```

### Шаг 7: Запуск приложения

1. В разделе **Web** найдите секцию **Reload**
2. Нажмите зеленую кнопку **Reload** для перезагрузки веб-приложения
3. Дождитесь сообщения об успешной перезагрузке

### Шаг 8: Проверка работы

1. Откройте в браузере: `https://gwadm.pythonanywhere.com`
2. Вы должны увидеть главную страницу с кнопкой "Войти через GWars"
3. При нажатии на кнопку произойдет редирект на GWars для авторизации

### Шаг 9: Настройка в GWars (если требуется)

Если нужно обновить URL в настройках GWars, используйте:
```
https://www.gwars.io/cross-server-login.php?site_id=4&url=https://gwadm.pythonanywhere.com/login
```

## Проверка логов при ошибках

Если что-то не работает:

1. **Проверьте логи ошибок**:
   - В разделе **Web** → **Error log** найдите последние ошибки
   - Или в консоли: `tail -n 50 ~/logs/gwadm.pythonanywhere.com.error.log`

2. **Проверьте логи сервера**:
   - В консоли: `tail -n 50 ~/logs/gwadm.pythonanywhere.com.server.log`

3. **Проверьте права доступа**:
```bash
ls -la ~/gwadmpaw
```

4. **Проверьте, что Python находит Flask**:
```bash
python3.10 -c "import flask; print(flask.__version__)"
```

## Обновление проекта

Если нужно обновить код из репозитория:

```bash
cd ~/gwadmpaw
git pull origin main
```

Затем перезагрузите веб-приложение через панель управления.

## Структура файлов на сервере

После развертывания структура должна выглядеть так:

```
/home/gwadm/gwadmpaw/
├── app.py
├── requirements.txt
├── README.md
├── DEPLOYMENT.md
├── .gitignore
├── database.db          # Создается автоматически
├── templates/
│   ├── base.html
│   ├── index.html
│   └── dashboard.html
└── static/
    ├── css/
    │   └── style.css
    └── js/
        └── theme.js
```

## Частые проблемы и решения

### Проблема: "ModuleNotFoundError: No module named 'flask'"

**Решение**: Установите зависимости еще раз:
```bash
pip3.10 install --user -r requirements.txt
```

### Проблема: "Permission denied" при создании database.db

**Решение**: Проверьте права доступа:
```bash
chmod 755 ~/gwadmpaw
chmod 644 ~/gwadmpaw/*.py
```

### Проблема: "Internal Server Error"

**Решение**: 
1. Проверьте логи ошибок (см. выше)
2. Убедитесь, что путь в WSGI файле правильный
3. Проверьте, что все файлы загружены

### Проблема: Статические файлы не загружаются

**Решение**: 
1. Проверьте настройки Static files в разделе Web
2. Убедитесь, что путь указан правильно: `/home/gwadm/gwadmpaw/static/`

## Дополнительная информация

- **Документация PythonAnywhere**: https://help.pythonanywhere.com/
- **Flask документация**: https://flask.palletsprojects.com/
- **Репозиторий проекта**: https://github.com/Kolunt/gwadmpaw

## Контакты и поддержка

Если возникли проблемы, проверьте:
1. Логи ошибок
2. Правильность путей в WSGI файле
3. Установлены ли все зависимости
4. Правильно ли настроены Static files

