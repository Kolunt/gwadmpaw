# Бэклог рефакторинга gwadm

Приоритет: **архитектура и сопровождаемость**, затем безопасность и тесты.  
Оценки — ориентировочные; задачи можно брать по одной ветке / PR.

Статусы: `todo` | `in_progress` | `done` | `cancelled`

---

## Фаза 0 — Подготовка (1–2 дня)

Цель: безопасно рефакторить, не ломая прод.

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| R-000 | Зафиксировать workflow: локально → тест → git → деплой → проверка на проде | done | `.cursor/rules/development-workflow.mdc` |
| R-001 | Добавить `scripts/smoke_check.py` (импорт app, `/`, `/login` redirect, parse gwars map) | done | `scripts/smoke_check.py` |
| R-002 | Описать целевую структуру пакетов в README или здесь — не менять код, только договориться | done | README + целевая структура в бэклоге |
| R-003 | Включить на проде только то, что нужно: `SECRET_KEY` в `.env` | done | |
| R-004 | Убрать из git чувствительное/лишнее: `database.db.backup*`, `pa_export/` | done | Отдельный PR, без функциональных изменений |

---

## Фаза 1 — Фундамент (3–5 дней)

Цель: вынести инфраструктурный слой из `app.py`, не трогая пока маршруты.

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-101 | `config.py` — `SECRET_KEY`, `GWARS_PASSWORD`, `DATABASE_PATH`, `CRON_SECRET_TOKEN`, флаги prod/dev | done | `gwadm/config.py` |
| R-102 | `db.py` — `get_db_path()`, `get_connection()` как context manager, WAL, timeout | done | `gwadm/db.py`, `get_db()` |
| R-103 | Перевести `init_db()` на вызов только при старте / явной миграции; убрать `ensure_db()` из каждого `get_db_connection()` | done | `ensure_db()` при импорте app и в cron |
| R-104 | `migrations/` — вынести ALTER/INSERT из `init_db` в версионированные скрипты (хотя бы `001_initial.sql`, `002_*.sql` + таблица `schema_version`) | done | R-103 |
| R-105 | `logging_config.py` — уровни log debug/info по `FLASK_ENV`; убрать `log_error` для штатного login debug | done | `gwadm/logging_config.py` |
| R-106 | `extensions.py` — создание `app`, ProxyFix, Babel, регистрация blueprints | done | `gwadm/extensions.py`, Babel 4.x |

**Критерий готовности:** `app.py` импортирует `create_app()` из пакета; поведение на проде не изменилось; smoke проходит.

---

## Фаза 2 — Разбиение монолита на Blueprints (1–2 недели)

Цель: `app.py` < 500 строк, только `create_app()` и регистрация модулей.

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-201 | Пакет `gwadm/` (или `app/`) + `create_app()` factory | done | `gwadm/factory.py`, `app = create_app()` |
| R-202 | Blueprint `auth` — `/login`, `/logout`, `/login/dev`, `/gwars-required`, verify_sign* | done | R-201 |
| R-203 | Blueprint `public` — `/`, `/participants`, `/faq`, `/rules`, `/rating`, `/contacts` | done | R-201 |
| R-204 | Blueprint `profile` — `/dashboard`, `/profile/*`, edit profile | done | R-201 |
| R-205 | Blueprint `events` — события, регистрация, просмотр | done | R-201 |
| R-206 | Blueprint `assignments` — задания, письма, чаты | done | R-201 |
| R-207 | Blueprint `admin` — все `/admin/*` | done | R-201 |
| R-208 | Blueprint `integrations` — Telegram webhook, Dadata verify, cron | done | R-201 |
| R-209 | Blueprint `meta` — `/avatars/image`, titles/awards/roles public views | done | R-201 |
| R-210 | Сервисный слой: `services/gwars_auth.py`, `services/avatars.py`, `services/telegram.py` | done | R-202, R-209 |
| R-211 | Удалить дубли debug-логики sign/sign3; один модуль `gwars_signatures.py` | done | `gwadm/services/gwars_signatures.py`, `scripts/verify_gwars_signatures.py` |

**Критерий готовности:** нет маршрутов в корневом `app.py`; импорты циклически не ломаются; `cron_tasks.py` импортирует из `db.py`, не из монолита.

---

## Фаза 3 — Авторизация и мобильный вход (3–5 дней)

Цель: предсказуемый login-flow, меньше боли на телефоне с приложением GWars.

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-301 | Разделить `/login` → `/login` (landing), `/login/go` (redirect GWars), callback как есть | done | `templates/login.html`, `/login/go` |
| R-302 | Мобильный interstitial: детект UA, инструкция «открыть в браузере», кнопка без мгновенного редиректа | done | `templates/login_mobile.html` |
| R-303 | Заменить `gwars_auth_attempt` на `login_state` + `return_url` в сессии | done | `gwadm/services/login_flow.py` |
| R-304 | `debug.html` / `debug_sign3.html` — только `FLASK_DEBUG` или роль admin | done | `can_view_auth_debug` |
| R-305 | `GWARS_PASSWORD` только из env (`config.py`), убрать из кода и debug-шаблонов | done | пароль скрыт в debug, блок callback на проде |
| R-306 | `/login/dev` — отключение через `ENABLE_DEV_LOGIN=0` на проде (дополнительно к проверке host) | done | `is_dev_login_enabled()` |

---

## Фаза 4 — Безопасность форм и API (2–4 дня)

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-401 | CSRF для POST в админке и профиле (`Flask-WTF` или свой токен) | done | R-207 |
| R-402 | Аудит загрузки файлов (awards, letter attachments) — расширения, размер, путь | done | R-207 |
| R-403 | `/cron/run` — токен только из env, без автогенерации в БД на проде | done | R-101 |
| R-404 | Rate limit на `/login`, `/telegram/webhook` (Flask-Limiter или nginx) | done | R-301 |
| R-405 | Security headers в nginx (X-Frame-Options, CSP базовый) | done | — |

---

## Фаза 5 — Тесты и CI (3–5 дней)

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-501 | `pytest` + `tests/conftest.py` (temp SQLite, test client) | done | R-102 |
| R-502 | Тесты `gwars_domains.py` (уже есть скрипт — перенести в pytest) | done | R-501 |
| R-503 | Тесты `gwars_signatures.py` — sign, sign2, sign3, sign4, cp1251 | done | R-211 |
| R-504 | Тесты auth flow: `/login/go` redirect URL, callback с фикстурными sign | done | R-301 |
| R-505 | GitHub Actions: lint (ruff/flake8) + pytest на push/PR | done | R-501 |
| R-506 | Smoke после деплоя (опционально): curl site_id, /health | done | R-001 |

---

## Фаза 6 — Frontend и шаблоны (по мере сил)

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-601 | Jinja macros: `avatar.html`, `user_row.html`, `badge.html` | todo | R-209 |
| R-602 | Вынести JS из `admin/settings.html` → `static/js/admin-settings.js` | todo | R-207 |
| R-603 | Убрать дубли inline-стилей в dashboard / view_profile / title_view | todo | R-601 |
| R-604 | PWA: стратегия кэша only same-origin; документировать bump `CACHE_NAME` | todo | частично done |
| R-605 | Разбить `style.css` на модули или секции с комментариями-якорями | todo | — |

---

## Фаза 7 — Операции и документация (параллельно)

| ID | Задача | Статус | Зависит от |
|----|--------|--------|------------|
| R-701 | Обновить `README.md` под gwadm.ru, актуальная версия, ссылки на GWARS_DOMAINS | todo | — |
| R-702 | Объединить `DEPLOYMENT.md` и `deploy/FINISH_SETUP.md` или явно пометить legacy PA | todo | — |
| R-703 | `GET /health` — 200 + версия, без БД или с лёгкой проверкой | done | R-106 |
| R-704 | systemd timer для backup БД + ротация (вместо только cron-job.org) | todo | R-102 |
| R-705 | Мониторинг: алерт при 5xx, место на диске (аватары-кэш растёт) | todo | R-703 |

---

## Фаза 8 — Долгосрочно (когда упрётесь в SQLite)

| ID | Задача | Статус | Триггер |
|----|--------|--------|---------|
| R-801 | Оценка миграции на PostgreSQL | todo | частые lock / >1k одновременных |
| R-802 | Фоновые задачи (Celery/RQ/systemd) для рассылок и пересчёта рейтинга | todo | таймауты gunicorn |
| R-803 | Кэш рейтинга (материализованная таблица / Redis) | todo | медленный `/rating` |

---

## Целевая структура каталогов

```
gwadmpaw/
├── gwadm/                    # пакет приложения (имя на выбор)
│   ├── __init__.py           # create_app()
│   ├── config.py
│   ├── extensions.py
│   ├── db.py
│   ├── blueprints/
│   │   ├── auth.py
│   │   ├── public.py
│   │   ├── profile.py
│   │   ├── events.py
│   │   ├── assignments.py
│   │   ├── admin/
│   │   └── integrations.py
│   ├── services/
│   │   ├── gwars_auth.py
│   │   ├── gwars_signatures.py
│   │   ├── gwars_domains.py  # перенос из корня
│   │   ├── avatars.py
│   │   └── telegram.py
│   └── models/               # опционально: тонкие репозитории
├── migrations/
├── tests/
├── templates/                # без изменений путей на первом этапе
├── static/
├── scripts/
│   ├── verify_gwars_domains.py
│   └── smoke_check.py
├── app.py                    # тонкая обёртка: from gwadm import create_app; app = create_app()
├── cron_tasks.py             # импорт из gwadm.db
└── wsgi.py / gunicorn app:app
```

---

## Рекомендуемый порядок PR (рефакторинг)

1. **R-101 + R-102 + R-105** — config, db, logging (один PR, без смены URL)
2. **R-106 + R-201** — factory `create_app()`, старый `app.py` реэкспортирует app
3. **R-209 + R-210** — avatars + gwars_domains в services (малый PR)
4. **R-202** — auth blueprint (самый чувствительный — отдельный PR + smoke)
5. **R-203 … R-208** — по одному blueprint за PR
6. **R-301 … R-306** — login/mobile после стабилизации auth
7. **R-501 … R-505** — тесты параллельно с фазой 2, начиная с domains/signatures

---

## Не в scope рефакторинга (отдельные задачи)

- Новые фичи без техдолга
- Смена дизайна / UX
- Миграция на другой фреймворк

---

## Журнал

| Дата | Изменение |
|------|-----------|
| 2025-09-25 | Создан бэклог; R-000, R-003 отмечены done по факту уже сделанного |
| 2025-09-25 | R-001, R-101, R-102, R-105: пакет `gwadm/` (config, db, logging), smoke_check, документация |
| 2025-09-25 | R-103: `ensure_db()` только при старте (app import, cron), guard в `get_db_connection()` |
| 2025-09-25 | R-106, R-201, R-002: `create_app()` factory, extensions, i18n; Babel 4.x fix |
| 2025-09-25 | R-211: `gwars_signatures.py` — compute/verify sign*, debug helpers; auth.py без inline hashlib |
| 2025-09-25 | R-301–R-306: login landing/go, mobile interstitial, login_state+return_url, debug gate, GWARS_PASSWORD, ENABLE_DEV_LOGIN |
| 2025-09-25 | R-401–R-405, R-501–R-506, R-703: CSRF, uploads audit, cron env-only, nginx rate limit/headers, pytest+CI, `/health` |
| 2025-09-25 | R-004: `database.db.backup*` убраны из git, расширен `.gitignore` (pytest cache, `_download_pa_export.py`) |
| 2025-09-25 | R-104: `migrations/` + `schema_version`, runner в `gwadm/migrations/runner.py`, legacy bootstrap для прода |
