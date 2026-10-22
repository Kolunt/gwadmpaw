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

На каждой карточке пары кнопка **«Работа»** открывает модалку с заметками и задачами. Данные привязаны к `event_assignments.id` (одинаковы на вкладках «Дед Мороз» и «Внучок»).

| Сущность | Возможности |
|----------|-------------|
| Заметки | создать, изменить, удалить (текст) |
| Задачи | создать, изменить, удалить; опциональный срок (`due_at`); отметка «выполнена» |

Если есть невыполненные задачи, карточка подсвечивается пульсирующей рамкой; на кнопке «Работа» показывается счётчик.

Сервис: [`gwadm/services/assignment_admin_workspace.py`](../gwadm/services/assignment_admin_workspace.py). Таблицы: `assignment_admin_notes`, `assignment_admin_tasks` (миграция `012_assignment_admin_workspace.py`).

JSON API (роль `admin`, CSRF):

- `GET /admin/events/<event_id>/assignments/<assignment_id>/workspace`
- `POST/PATCH/DELETE` … `/workspace/notes` и … `/workspace/tasks`

Фронт: [`static/js/admin-event-progress.js`](../static/js/admin-event-progress.js).
