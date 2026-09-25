# Развёртывание gwadm.ru (production)

Краткий чеклист на сервере — [deploy/FINISH_SETUP.md](../deploy/FINISH_SETUP.md).
Ниже — полное описание окружения и процедур.

## Окружение

| Параметр | Значение |
|----------|----------|
| Домен | https://gwadm.ru |
| Сервер | VPS, пользователь `deploy` |
| Приложение | `/home/deploy/gwadm` |
| systemd | user unit `gwadm.service` (`deploy/gwadm-user.service`) |
| БД | `~/gwadm/database.db` (см. [database.md](database.md)) |
| GWars site_id | `3` (см. [gwars_domains.md](gwars_domains.md)) |

## Локальная разработка

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
python app.py
```

Проверка: `python -m pytest -q`, `python scripts/smoke_check.py`.

## Деплой на прод

```bash
git push
ssh gwadmpaw-prod "cd ~/gwadm && git pull && systemctl --user restart gwadm"
bash ~/gwadm/scripts/post_deploy_smoke.sh
journalctl --user -u gwadm -n 20
```

Nginx reload нужен только после изменения `deploy/nginx-gwadm.conf`.

## Резервное копирование БД

Ежедневный backup через user systemd timer (см. [database.md](database.md)):

```bash
cp ~/gwadm/deploy/gwadm-backup.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gwadm-backup.timer
systemctl --user start gwadm-backup.service
ls -la ~/gwadm/backups/
```

Опционально в `~/gwadm/.env`: `BACKUP_DIR=/path/to/backups`.

## PWA / Service Worker

При релизе с изменениями в `static/` обновите `CACHE_NAME` в [`static/sw.js`](../static/sw.js) — синхронно с версией в [`version.py`](../version.py) (например `gwadmpaw-v1.28.0`). Иначе клиенты могут кэшировать старый CSS/JS.

---

# Завершение настройки gwadm.ru

Приложение и данные уже на сервере. Остался **один шаг с sudo** (nginx + HTTPS).

## 1. DNS (сделайте в панели регистратора домена)

| Тип | Имя | Значение |
|-----|-----|----------|
| A | `@` | `194.226.124.37` |
| A | `www` | `194.226.124.37` |

Подождите 5–30 минут после сохранения.

## 2. Nginx + SSL (на сервере)

Подключитесь и выполните (введёте пароль `deploy` один раз):

```bash
ssh gwadmpaw-prod
sudo bash ~/gwadm/deploy/setup-nginx-only.sh
```

Скрипт:
- проксирует `gwadm.ru` → gunicorn на порту 8000;
- выпустит сертификат Let's Encrypt (если DNS уже указывает на сервер).

## 3. GWars

В настройках GWars для **site_id=3** укажите callback:

```
https://gwadm.ru/login
```

При необходимости добавьте также `https://www.gwadm.ru/login`.

Карта доменов в приложении: админка → **Настройки → Интеграции → GWars** или см. [gwars_domains.md](gwars_domains.md).

**GWARS_PASSWORD** (подписи callback `/login`) — обязательно в `~/gwadm/.env` на проде:

```bash
cd ~/gwadm
grep '^GWARS_PASSWORD=' .env || echo 'GWARS_PASSWORD=MISSING'
# если MISSING — допишите строку (значение из настроек GWars для site_id=3), не перезаписывайте весь .env:
# GWARS_PASSWORD=ваш_пароль
# ENABLE_DEV_LOGIN=0
systemctl --user restart gwadm
journalctl --user -u gwadm -n 20 | grep -i GWARS || echo 'OK: no GWARS_PASSWORD warning'
```

Без `GWARS_PASSWORD` в логах gunicorn будет предупреждение, а callback отклоняется на production.

## 4. SECRET_KEY (сессии Flask)

Без фиксированного `SECRET_KEY` при нескольких воркерах gunicorn авторизация «вылетает» между запросами.

```bash
cd ~/gwadm
cp .env.example .env
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))" >> .env
# или отредактируйте .env вручную
cp deploy/gwadm-user.service ~/.config/systemd/user/gwadm.service
systemctl --user daemon-reload
systemctl --user restart gwadm
```

## 5. Telegram и cron

В админке: **Настройки → Интеграции** — «Проверить подключение» у бота (обновит webhook).

**CRON_SECRET_TOKEN** обязателен в `~/gwadm/.env` (больше не генерируется в БД):

```bash
cd ~/gwadm
# если токен был только в settings:
sqlite3 database.db "SELECT value FROM settings WHERE key='cron_secret_token';"
# добавьте в .env:
# CRON_SECRET_TOKEN=скопированное_значение
python3 -c "import secrets; print('CRON_SECRET_TOKEN=' + secrets.token_urlsafe(32))"  # или новый токен
systemctl --user restart gwadm
```

В cron-job.org:

```
https://gwadm.ru/cron/run?token=ВАШ_ТОКЕН
```

## 6. Nginx: rate limit и security headers

После обновления репозитория скопируйте конфиг и перезагрузите nginx:

```bash
sudo cp ~/gwadm/deploy/nginx-gwadm.conf /etc/nginx/sites-available/gwadm
sudo nginx -t && sudo systemctl reload nginx
```

Проверка заголовков:

```bash
curl -sI https://gwadm.ru/ | grep -i x-frame
```

## 7. Проверка

```bash
systemctl --user status gwadm
curl -s http://127.0.0.1:8000/health
bash ~/gwadm/scripts/post_deploy_smoke.sh
curl -I https://gwadm.ru/
curl -sI https://gwadm.ru/login | grep -i location    # site_id=3
curl -sI https://www.gwadm.ru/login | grep -i location
```
