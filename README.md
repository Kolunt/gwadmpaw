# Анонимные Деды Морозы

**Версия: 1.32.6** · production: [gwadm.ru](https://gwadm.ru)

Веб-приложение для организации «Анонимных Дедов Морозов» с авторизацией через GWars.

## Быстрый старт (локально)

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
python app.py
```

Проверка: `python -m pytest -q`, `python scripts/smoke_check.py`.

## Документация

| Документ | Описание |
|----------|----------|
| [doc/deployment.md](doc/deployment.md) | Развёртывание и обновление на gwadm.ru |
| [doc/gwars_domains.md](doc/gwars_domains.md) | Карта доменов GWars, site_id |
| [doc/database.md](doc/database.md) | SQLite, миграции, бэкапы |
| [doc/telegram.md](doc/telegram.md) | Telegram-бот и webhook |
| [doc/lottery.md](doc/lottery.md) | Механизм жеребьёвки |
| [doc/admin.md](doc/admin.md) | Админ-layout, тема, дашборд |
| [doc/layout.md](doc/layout.md) | Full-width layout, токены `--layout-*` |
| [doc/responsive.md](doc/responsive.md) | Breakpoints, drawer, адаптивные таблицы |
| [doc/refactoring_backlog.md](doc/refactoring_backlog.md) | Бэклог рефакторинга |
| [deploy/FINISH_SETUP.md](deploy/FINISH_SETUP.md) | Чеклист настройки на сервере |

Устаревший гайд PythonAnywhere: [doc/_OLD_deployment_pythonanywhere.md](doc/_OLD_deployment_pythonanywhere.md) (неактуален с 2025-09-25).

## Конфигурация

Переменные окружения — [`gwadm/config.py`](gwadm/config.py), шаблон [`.env.example`](.env.example).

| Переменная | Назначение |
|------------|------------|
| `SECRET_KEY` | Ключ сессий Flask (обязательно на проде) |
| `GWARS_PASSWORD` | Пароль подписей GWars (обязательно на проде) |
| `CRON_SECRET_TOKEN` | Защита `/cron/run` (обязательно на проде) |
| `ENABLE_DEV_LOGIN` | `0` на проде — отключить `/login/dev` |
| `DATABASE_PATH` | Путь к SQLite |

## Структура

```
gwadmpaw/
├── gwadm/              # Пакет приложения (factory, blueprints, services)
├── migrations/         # Версионированные миграции SQLite
├── templates/macros/   # Jinja macros (avatar, badge, csrf, …)
├── static/             # CSS, JS, PWA (sw.js)
├── tests/              # pytest
├── doc/                # Документация
├── scripts/            # smoke_check, post_deploy_smoke, verify_*
└── app.py              # Entry point: gunicorn app:app
```

## Обновление на проде

```bash
cd ~/gwadm && git pull && systemctl --user restart gwadm
bash ~/gwadm/scripts/post_deploy_smoke.sh
```

Подробнее: [doc/deployment.md](doc/deployment.md).
