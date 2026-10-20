# Адаптивная вёрстка

Единые breakpoints и поведение layout для публичного сайта и админки.

Токены и классы full-width layout (`layout-main`, `--layout-inner`) — в [layout.md](layout.md).

## Breakpoints

| Зона | Ширина | Сайдбар |
|------|--------|---------|
| Mobile | ≤767px | Drawer (гамбургер + overlay) |
| Tablet | 768–1023px | Drawer (как на телефоне) |
| Desktop | ≥1024px | Фиксированный сайдбар 250px |

Токены в [`static/css/style.css`](../static/css/style.css): `--bp-sm` (480), `--bp-md` (768), `--bp-lg` (1024).

Порог drawer синхронизирован с [`static/js/sidebar.js`](../static/js/sidebar.js) (`MOBILE_MAX = 1024`).

## Таблицы

- **Desktop (≥1024):** `.admin-table` / `.participants-table` с горизонтальным scroll в `.table-container` или `.table-scroll-desktop`.
- **≤1023:** card-layout — `thead` скрыт, строки как карточки, подписи через `data-label` на `<td>`.

## Сетки

На tablet (768–1023) основные сетки (`stats-grid`, `events-grid`, `admin-grid`, `contacts-grid`, `assignments-grid`) — **2 колонки**.

На mobile (≤767) — **1 колонка** (кроме `features-grid` на главной — всегда **2×2**, на desktop **4 в ряд**).

`features-grid` (главная): **2 колонки** (&lt;1024px), **4 колонки** (≥1024px).

## Многострочный пользовательский текст

Поля вроде «О себе», сообщений в чате `/letter`, информации об отправке и благодарностей на `/assignments` хранятся в БД с переводами строк (`\n`). Legacy-значения с текстом `<br>` при чтении/сохранении приводятся к `\n` через `normalize_multiline_text` в [`gwadm/services/multiline_text.py`](../gwadm/services/multiline_text.py).

**Вывод в шаблонах:** только фильтр Jinja `format_multiline` (регистрация в [`gwadm/jinja_filters.py`](../gwadm/jinja_filters.py)). Не смешивать с `white-space: pre-wrap` на том же узле и не дублировать `| e | replace('\n', '<br>') | safe`.

При добавлении новых многострочных полей:

1. Нормализовать при записи (`normalize_multiline_text`).
2. При необходимости — ленивая нормализация при чтении (как для `recipient_thanks_message`).
3. В шаблоне: `{{ value | format_multiline }}`, при необходимости класс `.multiline-text`.

Тесты: [`tests/test_multiline_text.py`](../tests/test_multiline_text.py).

## Переключатель чатов `/letter`

Логика группировки: [`gwadm/services/letter_navigation.py`](../gwadm/services/letter_navigation.py).

- Несколько мероприятий — `<select class="letter-event-select">`; смена мероприятия открывает чат **Деда Мороза** в выбранном событии.
- Одно мероприятие — только чипы **Дед Мороз** / **Внучок** (`letter-role-switcher`).
- Заход на `/letter` без `assignment_id` — последнее мероприятие (по `assigned_at`) и роль Деда.
- Админ (`admin=1`): select мероприятия + при нескольких парах в событии — `letter-chat-select` с подписью пары.

Пункт сайдбара **Задания** пульсирует (класс `sidebar-link-assignments-pulse`), если пользователь зарегистрирован на мероприятие в этапе **Обмен подарками** (`celebration_date`). Логика: `user_in_celebration_exchange_stage` в [`events_stages.py`](../gwadm/services/events_stages.py).

Кнопка **Задания** в шапке мероприятия (`.event-header-actions`, `event_view.html`): логика [`event_assignments_button_state`](../gwadm/services/events_stages.py).

| Условие | Кнопка | Пульс (`letter-button-animated`) |
|---------|--------|----------------------------------|
| Не зарегистрирован | скрыта | — |
| До жеребьёвки, нет задания | скрыта | — |
| До жеребьёвки, есть задание (раннее распределение) | видна | нет |
| Этап «Обмен подарками» | видна | да |
| Этап «Мероприятие завершено» | видна | нет |

«После жеребьёвки» по календарю: `celebration_date` или `after_party`. Иконки админа и кнопка — в один горизонтальный ряд.

Тесты: [`tests/test_letter_navigation.py`](../tests/test_letter_navigation.py), [`tests/test_celebration_sidebar.py`](../tests/test_celebration_sidebar.py), [`tests/test_event_assignments_button.py`](../tests/test_event_assignments_button.py).

## Стили страниц

Page-specific CSS вынесен в `style.css` (секция «Responsive pages»):

- чат писем (`letter.html`)
- контакты (`contacts.html`)
- правила (public + admin)
- рассылки (`broadcasts.html`, `broadcasts_templates.html`)
- распределение участников (классы `distribution-*`)
- логи действий (`admin/logs.html`, `.admin-logs-page`, `.logs-table`)
- канбан прогресса обмена (`admin/event_progress.html`, `.progress-kanban` — всегда 4 колонки в ряд, при нехватке ширины горизонтальный скролл)

## Ручная проверка

DevTools, ширины: **390**, **768**, **1024**, **1280** px.

| Страница | Что проверить |
|----------|----------------|
| `/` | hero, stats-grid, нет горизонтального scroll body |
| `/assignments` | карточки, фильтры |
| `/letter` | пузыри чата, форма, select мероприятий и чипы Дед/Внучок |
| `/contacts` | карточки контактов |
| `/admin/` | дашборд, drawer на tablet |
| `/admin/users` | таблица → карточки на ≤1023 |
| `/admin/settings` | табы, таблица доменов |
| `/admin/events/{id}/distribution/positive` | табы, поиск, таблица |

## Локальные тесты

```bash
python -m pytest tests/test_multiline_text.py tests/test_letter_navigation.py tests/test_responsive_layout.py -v
python -m pytest -q
python scripts/smoke_check.py
python scripts/verify_web_routes.py
```
