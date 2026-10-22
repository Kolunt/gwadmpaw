# Прогресс обмена подарками (админ)

Экран: `/admin/events/<event_id>/progress` (кнопка 📊 в списке мероприятий).

Логика этапов: [`gwadm/services/assignment_progress.py`](../gwadm/services/assignment_progress.py).

## Просмотр задания участником

При первом заходе на `/assignments` или `/letter` (не в режиме `admin=1`) выставляются:

- `santa_viewed_at` — если пользователь Дед в паре;
- `recipient_viewed_at` — если пользователь Внучок.

Миграция: `migrations/008_assignment_viewed.py`.

## Колонки канбана

Отправка (`is_sent`): `santa_sent_at` или сообщение от Деда в `letter_messages`.

Подтверждение: `recipient_received_at`.

Подробная таблица этапов для ролей Дед / Внучок — в плане и в docstrings сервиса.

## Workspace администратора на карточке канбана

На каждой карточке пары кнопка **«Работа»** открывает модалку с **единой формой** и **лентой записей** (хронология по `created_at`, новые сверху). Данные привязаны к `event_assignments.id` (одинаковы на вкладках «Дед Мороз» и «Внучок»).

| Условие | Хранение | В ленте |
|---------|----------|---------|
| Срок **не** указан | `assignment_admin_notes` | текст, автор, дата; без чекбокса |
| Срок указан | `assignment_admin_tasks` | текст, срок, чекбокс «выполнено», автор, дата |

Поле срока в форме — нативный `datetime-local` в одной строке с кнопкой «календарь» (📅), как даты этапов на форме мероприятия: `showPicker()` в поддерживаемых браузерах, иначе фокус на поле. Разметка `.datetime-field-wrap`, стили и поведение этапов — [`static/js/admin-event-form.js`](../static/js/admin-event-form.js), [`doc/admin.md`](admin.md).

В UI нет кнопок «Изменить» / «Удалить» (эндпоинты PATCH/DELETE в API остаются для возможного будущего использования). Отметка задачи выполненной — `PATCH` … `/workspace/tasks/<id>` с `completed`.

**Пульс** карточки и счётчик на «Работа» — только невыполненные задачи с заданным `due_at` (`completed_at IS NULL` и `due_at IS NOT NULL`).

Ответ `GET workspace` включает массив `entries` (поля `kind`: `note` | `task`, `author_username`, для задачи с сроком — `show_checkbox`, `due_at`, `is_completed`) и `open_tasks_count`.

Сервис: [`gwadm/services/assignment_admin_workspace.py`](../gwadm/services/assignment_admin_workspace.py). Таблицы: `assignment_admin_notes`, `assignment_admin_tasks` (миграция `012_assignment_admin_workspace.py`).

JSON API (роль `admin`, CSRF):

- `GET /admin/events/<event_id>/assignments/<assignment_id>/workspace`
- `POST` … `/workspace/notes` (текст без срока) или `POST` … `/workspace/tasks` (текст + опциональный `due_at`)
- `PATCH/DELETE` … `/workspace/notes` и … `/workspace/tasks` (без UI)

Фронт: [`static/js/admin-event-progress.js`](../static/js/admin-event-progress.js).
