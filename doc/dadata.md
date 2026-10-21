# DaData (подсказки адреса)

Интеграция для автозаполнения почтового адреса при регистрации на мероприятие и в [редактировании профиля](/profile/edit).

## Настройка в админке

1. `/admin/settings` → вкладка **Интеграции** → **DaData**.
2. Указать **API-ключ** (Token) и **Secret**, нажать «Проверить ключи».
3. Включить чекбокс **DaData интеграция включена** (доступен только после успешной проверки).

В БД (`settings`):

| Ключ | Назначение |
|------|------------|
| `dadata_api_key` | Token |
| `dadata_secret_key` | Secret |
| `dadata_verified` | `1` после успешной проверки |
| `dadata_enabled` | `1` — подсказки включены на сайте |

## Поведение на сайте

При `dadata_enabled` и `dadata_verified` на шаге адреса модалки регистрации и в блоке «Адрес» профиля показывается поле **«Поиск адреса»**. Пользователь выбирает подсказку — заполняются поля индекс, страна, город, улица, дом, корпус/строение, квартира. Поля остаются редактируемыми.

Подсказки ориентированы на **адреса РФ**. Для других стран ввод вручную.

## API (прокси)

Браузер **не** получает Secret. Запросы идут на сервер:

- `POST /dadata/suggest/address`
- Требуется авторизация (`session`) и CSRF (`X-CSRF-Token` или поле `_csrf_token` в JSON).
- Body: `{"query": "москва ленина"}` (до 200 символов).
- Ответ: `{ "success": true, "suggestions": [{ "value", "label", "fields": { ... } }] }`.

Сервер вызывает `https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address`.

Реализация: [`gwadm/services/dadata.py`](../gwadm/services/dadata.py), маршрут в [`gwadm/blueprints/integrations.py`](../gwadm/blueprints/integrations.py).

Фронт: [`static/js/dadata-address.js`](../static/js/dadata-address.js).

## Маппинг полей DaData → форма

| Поле формы | DaData `data` |
|------------|----------------|
| `postal_code` | `postal_code` |
| `country` | `country` |
| `city` | `city` или `settlement` |
| `street` | `street_with_type` или `street` |
| `house` | `house` |
| `building` | `block` |
| `apartment` | `flat` |

## Проверка ключей (админ)

`POST /admin/settings/verify-dadata` — отдельный от подсказок; использует Profile Balance API (см. [`verify_dadata_api`](../gwadm/services/telegram.py)).
