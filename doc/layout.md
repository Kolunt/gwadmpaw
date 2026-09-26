# Layout-система

Единая сетка для публичного сайта и `/admin/*`: контент на всю ширину области справа от сайдбара, без `max-width` у основного каркаса.

## Токены

В [`static/css/style.css`](../static/css/style.css):

| Токен | Значение | Назначение |
|-------|----------|------------|
| `--layout-gap` | `0` | Зазор между секциями и ячейками сеток |
| `--layout-padding-x` | `0` | Горизонтальный padding обёртки страницы |
| `--layout-padding-y` | `0` | Вертикальный padding обёртки страницы |
| `--layout-inner` | `0.75rem` (12px) | Внутренний padding карточек, заголовков секций |
| `--layout-section-gap` | `1.5rem` (24px) | Равный зазор между соседними секциями в `.layout-main` |
| `--layout-narrow` | `48rem` | Узкий контент (формы, письма, login) |

## Классы

| Класс | Назначение |
|-------|------------|
| `.layout-main` | Корневая обёртка контента в `base.html` / `admin/base.html` — `width: 100%`, без `max-width` |
| `.layout-section` | Секция страницы без внешних отступов (hero, stats, events на главной) |
| `.layout-inner` | Узкая колонка внутри full-width layout (login, формы) |
| `.container` | Alias `.layout-main` для обратной совместимости |
| `.dashboard-container`, `.admin-container` | Семантические обёртки страниц — прозрачные (100% ширины) |

## Правила

- Межсекционные gap в сетках — **0**; разделение ячеек — `border` на карточках.
- Между соседними `<section>` в `.layout-main` — `margin-top: var(--layout-section-gap)` (главная: hero → events → features → CTA).
- `.main-content`, `.layout-main`, page shells — `padding-block: 0` (включая mobile breakpoints).
- Navbar (60px) и sidebar (250px на desktop) не меняются; отступ под navbar задаёт `.content-wrapper { margin-top: 60px }`.
- Карточки и плитки: `border-radius: 0`, `padding: var(--layout-inner)`.
- Узкий контент только точечно: `.form-container`, `.layout-inner`, `.profile-card`, `.letter-page-card`.
- Hero на главной (`.hero-section`) — на всю высоту области контента: `min-height: calc(100dvh - 60px)`.

## Features (главная)

`.features-grid` — 4 карточки: **2×2** на узких экранах (`repeat(2, 1fr)`), **4 в ряд** на desktop (≥1024px).

## Блок «Перейти к мероприятиям»

`.section-footer` в секции events — `padding-top: var(--layout-section-gap)`, снизу `--layout-inner`, `border-top` отделяет от сетки карточек.

## Статистика (главная)

Три метрики (участники, онлайн, мероприятия) — блок `.stats-bar.stats-bar-hero` внизу `.hero-section` (виден без прокрутки). Три `.stats-bar-item`, разделённые `border`. На mobile (≤768px) — колонка.

## Hero (главная)

Секция `.hero-section` на `/`:

| Свойство | Значение |
|----------|----------|
| Высота | `min-height: calc(100dvh - 60px)` — вся область под navbar |
| Градиент | 4 остановки на базе `--accent-color` / `--accent-hover` + `color-mix` |
| Анимация | `@keyframes hero-gradient-shift`, 20s, `ease`, `infinite` |
| Механика | `background-size: 400% 400%` + сдвиг `background-position` |
| Доступность | при `prefers-reduced-motion: reduce` — статичный градиент, без анимации |

Цвета accent подставляются из настроек сайта (`base.html` → `:root`).

### Hero whispers (шёпоты на фоне)

Декоративные фразы в стиле боевого лога GWars — случайно появляются на фоне hero и плавно исчезают.

| Элемент | Назначение |
|---------|------------|
| `.hero-whispers` | Абсолютный слой под контентом (`z-index: 1`), `pointer-events: none`, `aria-hidden` |
| `.hero-whisper` | Одна фраза: monospace, полупрозрачный белый, `@keyframes hero-whisper-life` |
| `.hero-content`, `.stats-bar-hero` | `z-index: 2` — текст и статистика поверх шёпотов |

Данные: таблица `hero_whispers`, настройка `hero_whispers_enabled` в `settings`. Управление — [`/admin/hero-whispers`](../gwadm/blueprints/admin/hero_whispers.py). На главной фразы передаются в `window.HERO_WHISPERS` и обрабатываются [`static/js/hero-whispers.js`](../static/js/hero-whispers.js).

При `prefers-reduced-motion: reduce` слой `.hero-whispers` скрыт (как и анимация градиента hero).

## Карточки мероприятий (главная)

Футер карточки (`.event-card-footer-link`) — ссылка на страницу мероприятия (`/events/{id}`), без отдельной кнопки «Подробнее».

В `.event-card-home` статус мероприятия — иконка справа от названия:

| Состояние | Класс | Иконка | Условие |
|-----------|-------|--------|---------|
| Идёт | `.event-status-active` | зелёный `●` | `current_stage` есть, `is_event_finished()` — false |
| Завершено | `.event-status-finished` | красный `✕` | `is_event_finished()` — true |

Название текущего этапа — в атрибуте `title` у зелёного индикатора. Мероприятия без начавшихся этапов индикатор не показывают.

Логика: [`gwadm/services/events.py`](../gwadm/services/events.py) (`is_event_finished`), данные на главной — [`gwadm/blueprints/public.py`](../gwadm/blueprints/public.py).

## Связанные документы

- [responsive.md](responsive.md) — breakpoints, drawer, адаптивные таблицы
- [admin.md](admin.md) — админ-тема и дашборд
