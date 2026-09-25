# Завершение настройки gwadm.ru

Полный гайд: [doc/deployment.md](../doc/deployment.md).

Приложение и данные уже на сервере. Остался **один шаг с sudo** (nginx + HTTPS).

## 1. DNS (сделайте в панели регистратора домена)

| Тип | Имя | Значение |
|-----|-----|----------|
| A | `@` | `194.226.124.37` |
| A | `www` | `194.226.124.37` |

Подождите 5–30 минут после сохранения.

## 2. Linger для user systemd (обязательно)

Без linger сервис `gwadm` останавливается, когда закрывается SSH-сессия → **502 Bad Gateway** в nginx.

```bash
sudo loginctl enable-linger deploy
loginctl show-user deploy -p Linger   # Linger=yes
systemctl --user enable --now gwadm
```

## 3. Nginx + SSL (на сервере)

Подключитесь и выполните (введёте пароль `deploy` один раз):

```bash
ssh gwadmpaw-prod
sudo bash ~/gwadm/deploy/setup-nginx-only.sh
```

Скрипт:
- проксирует `gwadm.ru` → gunicorn на порту 8000;
- выпустит сертификат Let's Encrypt (если DNS уже указывает на сервер).

## 4. GWars

В настройках GWars для **site_id=3** укажите callback:

```
https://gwadm.ru/login
```

При необходимости добавьте также `https://www.gwadm.ru/login`.

Карта доменов в приложении: админка → **Настройки → Интеграции → GWars** или см. [doc/gwars_domains.md](../doc/gwars_domains.md).

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

## 5. SECRET_KEY (сессии Flask)

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

## 6. Telegram и cron

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

## 7. Nginx: rate limit и security headers

После обновления репозитория скопируйте конфиг и перезагрузите nginx:

```bash
sudo cp ~/gwadm/deploy/nginx-gwadm.conf /etc/nginx/sites-available/gwadm
sudo nginx -t && sudo systemctl reload nginx
```

Проверка заголовков:

```bash
curl -sI https://gwadm.ru/ | grep -i x-frame
```

## 8. Backup БД (systemd timer)

```bash
mkdir -p ~/.config/systemd/user
cp ~/gwadm/deploy/gwadm-backup.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gwadm-backup.timer
systemctl --user start gwadm-backup.service
ls -la ~/gwadm/backups/
```

Подробнее: [doc/database.md](../doc/database.md).

## 9. Мониторинг (systemd timer)

```bash
cp ~/gwadm/deploy/gwadm-monitor.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gwadm-monitor.timer
systemctl --user start gwadm-monitor.service
journalctl --user -u gwadm-monitor.service -n 30
```

Подробнее: [doc/deployment.md](../doc/deployment.md#мониторинг).

## 10. Фоновые задачи (rating, рассылки, audit, avatar cache)

```bash
cp ~/gwadm/deploy/gwadm-rating-cache.{service,timer} ~/.config/systemd/user/
cp ~/gwadm/deploy/gwadm-broadcast-queue.{service,timer} ~/.config/systemd/user/
cp ~/gwadm/deploy/gwadm-avatar-cleanup.{service,timer} ~/.config/systemd/user/
cp ~/gwadm/deploy/gwadm-sqlite-audit.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now gwadm-rating-cache.timer gwadm-broadcast-queue.timer gwadm-avatar-cleanup.timer gwadm-sqlite-audit.timer
systemctl --user start gwadm-rating-cache.service
systemctl --user restart gwadm
```

Подробнее: [doc/deployment.md](../doc/deployment.md#фоновые-задачи-фаза-8).

## 11. Проверка после деплоя (обязательно)

```bash
systemctl --user status gwadm
bash ~/gwadm/scripts/post_deploy_smoke.sh   # gunicorn + nginx + version
curl -s http://127.0.0.1/health           # через nginx :80
curl -sI http://gwadm.ru/login | grep -i location    # site_id=3
```

`post_deploy_smoke.sh` должен завершиться с `OK`. Версия в `/health` = `version.py` в git.
