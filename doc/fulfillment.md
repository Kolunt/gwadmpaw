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
