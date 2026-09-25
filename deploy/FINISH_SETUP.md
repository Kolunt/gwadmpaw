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

Карта доменов в приложении: админка → **Настройки → Интеграции → GWars** или см. [GWARS_DOMAINS.md](../GWARS_DOMAINS.md).

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

В cron-job.org замените URL на:

```
https://gwadm.ru/cron/run?token=ВАШ_ТОКЕН
```

## 6. Проверка

```bash
systemctl --user status gwadm
curl -I http://127.0.0.1:8000/
curl -I https://gwadm.ru/
curl -sI https://gwadm.ru/login | grep -i location    # site_id=3
curl -sI https://www.gwadm.ru/login | grep -i location
```
