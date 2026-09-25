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

