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

## Стили страниц

Page-specific CSS вынесен в `style.css` (секция «Responsive pages»):

- чат писем (`letter.html`)
- контакты (`contacts.html`)
- правила (public + admin)
- рассылки (`broadcasts.html`, `broadcasts_templates.html`)
- распределение участников (классы `distribution-*`)

## Ручная проверка

DevTools, ширины: **390**, **768**, **1024**, **1280** px.

| Страница | Что проверить |
|----------|----------------|
| `/` | hero, stats-grid, нет горизонтального scroll body |
| `/assignments` | карточки, фильтры |
| `/letter` | пузыри чата, форма |
| `/contacts` | карточки контактов |
| `/admin/` | дашборд, drawer на tablet |
| `/admin/users` | таблица → карточки на ≤1023 |
| `/admin/settings` | табы, таблица доменов |
| `/admin/events/{id}/distribution/positive` | табы, поиск, таблица |

## Локальные тесты

```bash
python -m pytest tests/test_responsive_layout.py -v
python -m pytest -q
python scripts/smoke_check.py
python scripts/verify_web_routes.py
```
