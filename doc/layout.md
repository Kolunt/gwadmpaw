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

- Межсекционные gap и margin — **0**; разделение блоков — `border` на карточках.
- `.main-content`, `.layout-main`, page shells — `padding-block: 0` (включая mobile breakpoints).
- Navbar (60px) и sidebar (250px на desktop) не меняются; отступ под navbar задаёт `.content-wrapper { margin-top: 60px }`.
- Карточки и плитки: `border-radius: 0`, `padding: var(--layout-inner)`.
- Узкий контент только точечно: `.form-container`, `.layout-inner`, `.profile-card`, `.letter-page-card`.
- Hero на главной (`.hero-section`) — на всю высоту области контента: `min-height: calc(100dvh - 60px)`.

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

## Связанные документы

- [responsive.md](responsive.md) — breakpoints, drawer, адаптивные таблицы
- [admin.md](admin.md) — админ-тема и дашборд
