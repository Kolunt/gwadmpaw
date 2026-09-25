# GWars: домены-зеркала и site_id

Приложение может работать на нескольких доменах (зеркалах). Для каждого домена GWars использует свой `site_id` и свой callback URL `https://{host}/login`.

## Настройка

Карта доменов хранится в таблице `settings` под ключом `gwars_domain_map` (категория `integrations`).

Редактирование: **Админ-панель → Настройки → Интеграции → GWars**.

### Формат JSON

```json
[
  {"host": "gwadm.ru", "site_id": 3, "primary": true},
  {"host": "www.gwadm.ru", "site_id": 3},
  {"host": "gwadm.pythonanywhere.com", "site_id": 4}
]
```

| Поле | Описание |
|------|----------|
| `host` | Домен без схемы и без порта (например `gwadm.ru`) |
| `site_id` | Положительное целое — ID сайта в кабинете GWars |
| `primary` | Ровно одна запись должна иметь `primary: true` — используется как fallback для неизвестных доменов и для локальной разработки |

## Текущее соответствие

| Домен | site_id | Примечание |
|-------|---------|------------|
| `gwadm.ru` | 3 | primary, основной сервер |
| `www.gwadm.ru` | 3 | зеркало основного домена |
| `gwadm.pythonanywhere.com` | 4 | legacy PythonAnywhere |

## Как добавить зеркало

1. В кабинете GWars для нужного `site_id` разрешите callback `https://{новый-домен}/login`.
2. В админке добавьте строку: домен, `site_id`, при необходимости отметьте primary.
3. Сохраните настройки — перезапуск приложения не требуется.
4. Проверьте редирект (см. ниже).

Если домен не найден в карте, используется `site_id` primary-зеркала.

## Как это работает

1. Пользователь открывает `/login` (landing) на текущем домене.
2. Кнопка «Войти через GWars» ведёт на `/login/go` (на мобильных — interstitial с подсказкой).
3. Приложение определяет `site_id` по заголовку `Host` (через `ProxyFix` за nginx).
4. Callback URL всегда строится как `https://{текущий-host}/login` (на проде).
5. `/login/go` редиректит на `https://www.gwars.io/cross-server-login.php?site_id=...&url=...`.

Логика в модуле `gwadm/services/gwars_domains.py` (корневой `gwars_domains.py` — обратносовместимый re-export).

## Telegram и site_url

**Базовый URL сайта** (`site_url` в настройках) — канонический адрес для ссылок в Telegram-боте (например `https://gwadm.ru`).

Зеркала GWars на работу бота не влияют: webhook и ссылки используют `site_url`, а не текущий домен входа.

## Проверка после изменений

Локально:

```bash
python scripts/verify_gwars_domains.py
```

На сервере (после деплоя):

```bash
curl -sI https://gwadm.ru/login | head -1               # 200 landing
curl -sI https://gwadm.ru/login/go | grep -i location   # site_id=3
curl -sI https://www.gwadm.ru/login/go | grep -i location  # site_id=3
```

В заголовке `Location` должен быть URL вида `cross-server-login.php?site_id=3&url=https%3A%2F%2F...%2Flogin`.

Ручная проверка: вход через GWars на `https://gwadm.ru`, страница `/gwars-required` — та же ссылка с корректным `site_id`.
