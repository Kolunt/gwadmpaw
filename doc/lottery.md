# Механизм проведения жеребьёвки

## Обзор

Жеребьёвка (lottery) - это этап мероприятия, на котором происходит случайное распределение участников на пары "Дед Мороз → Получатель подарка". Это ключевой этап в системе "Анонимные Деды Морозы".

## Этапы мероприятия

Жеребьёвка является частью жизненного цикла мероприятия:

1. **Предварительная регистрация** (pre_registration) - ранний этап
2. **Основная регистрация** (main_registration) - основной период
3. **Закрытие регистрации** (registration_closed) - регистрация завершена
4. **Жеребьёвка** (lottery) - **распределение пар** ⭐
5. **Обмен подарками** (celebration_date) - день вручения
6. **Мероприятие завершено** (after_party) - послевкусие

## Участники жеребьёвки

### Кто участвует?

В жеребьёвке участвуют только **утверждённые участники** (participants with `approved = 1` в таблице `event_participant_approvals`).

Функция получения участников:
```python
def get_approved_participants(event_id):
    """Получает список утвержденных участников"""
    # Выбирает участников из event_participant_approvals
    # где approved = 1
```

### Минимальные требования

- Минимум **2 участника** для проведения жеребьёвки
- Все участники должны быть **утверждены администратором**

## Два способа проведения жеребьёвки

### 1. Простая случайная жеребьёвка (`create_random_assignments`)

**Функция:** `create_random_assignments(event_id, assigned_by)`

**Алгоритм:**
1. Получает список утверждённых участников
2. Создаёт список их ID
3. **Перемешивает** список случайным образом (`random.shuffle`)
4. Создаёт **циклическое распределение**:
   - Участник `i` дарит подарок участнику `(i + 1) % n`
   - Последний участник дарит первому
   - Гарантирует, что каждый участник получает подарок

**Пример:**
```
Участники: [A, B, C, D]
После перемешивания: [C, A, D, B]

Распределение:
C → A  (C дарит A)
A → D  (A дарит D)
D → B  (D дарит B)
B → C  (B дарит C)  ← циклическое замыкание
```

**Код:**
```python
participant_ids = [p['user_id'] for p in participants]
random.shuffle(participant_ids)

assignments = []
for i in range(len(participant_ids)):
    santa_id = participant_ids[i]
    recipient_id = participant_ids[(i + 1) % len(participant_ids)]
    assignments.append((santa_id, recipient_id))
```

### 2. Продвинутая жеребьёвка с опциями (`admin_event_distribution_positive_generate`)

**Маршрут:** `POST /admin/events/<event_id>/distribution/positive/random`

**Особенности:**
- Поддержка **распределения по странам** (опция `group_by_country`)
- Поддержка **закреплённых пар** (locked_pairs)
- Поддержка **заблокированных участников** (assignment_locked_santas)
- Множественные попытки генерации (до 3000 попыток)

**Алгоритм:**

#### Шаг 1: Подготовка данных
```python
# Получает участников с их странами и городами
participants = conn.execute('''
    SELECT er.user_id, u.username, 
           COALESCE(d.country, u.country) AS country,
           COALESCE(d.city, u.city) AS city
    FROM event_registrations er
    LEFT JOIN event_participant_approvals epa ON ...
    WHERE epa.approved = 1
''')
```

#### Шаг 2: Обработка закреплённых пар
```python
# Закреплённые пары (locked_pairs) - пары, которые нельзя менять
locked_assignments = {}
for entry in locked_pairs_raw:
    santa_id = entry['santa_id']
    recipient_id = entry['recipient_id']
    # Проверки:
    # - santa_id != recipient_id (нельзя дарить себе)
    # - Оба участника в списке утверждённых
    # - Если group_by_country: страны должны совпадать
    locked_assignments[santa_id] = recipient_id
```

#### Шаг 3: Распределение по странам (если включено)
```python
if group_by_country:
    # Группирует получателей по странам
    recipients_by_country = defaultdict(list)
    for rid in user_ids:
        country = participants_map[rid]['country']
        recipients_by_country[country].append(rid)
    
    # Для каждого Деда Мороза ищет получателя из той же страны
    for santa_id in remaining_candidate_santas:
        country = participants_map[santa_id]['country']
        candidates = recipients_by_country.get(country)
        if candidates:
            recipient_id = candidates.pop(0)
            same_country_pairs.append((santa_id, recipient_id))
```

#### Шаг 4: Распределение оставшихся участников
```python
def try_assignments(rem_santas, rem_recipients, require_same_country, attempts=3000):
    """Пытается создать валидные пары за attempts попыток"""
    for _ in range(attempts):
        random.shuffle(rem_santas)
        random.shuffle(rem_recipients)
        
        # Проверяет, что все пары валидны
        valid = True
        for santa_id, recipient_id in zip(rem_santas, rem_recipients):
            if not is_valid_pair(santa_id, recipient_id, require_same_country):
                valid = False
                break
        
        if valid:
            return list(zip(rem_santas, rem_recipients))
    
    return None  # Не удалось создать валидные пары
```

**Проверка валидности пары:**
```python
def is_valid_pair(santa_id, recipient_id, require_same_country):
    # 1. Нельзя дарить себе
    if santa_id == recipient_id:
        return False
    
    # 2. Если требуется распределение по странам
    if require_same_country:
        santa_country = participants_map[santa_id]['country']
        recipient_country = participants_map[recipient_id]['country']
        if santa_country and recipient_country and santa_country != recipient_country:
            return False
    
    return True
```

#### Шаг 5: Объединение результатов
```python
# Объединяет закреплённые пары, пары по странам и оставшиеся пары
assignment_pairs = (
    list(locked_assignments.items()) +  # Закреплённые
    same_country_pairs +                 # По странам
    extra_pairs                          # Оставшиеся
)
```

## Сохранение распределения

### Функция `save_event_assignments`

**Параметры:**
- `event_id` - ID мероприятия
- `assignments` - список кортежей `(santa_id, recipient_id)`
- `assigned_by` - ID администратора, создавшего распределение
- `locked_pairs` - закреплённые пары (опционально)
- `assignment_locked` - флаг блокировки всего распределения

**Процесс:**
1. **Сохраняет текущее состояние** (для возможности отката)
2. **Удаляет старые назначения** для мероприятия
3. **Вставляет новые назначения** в таблицу `event_assignments`
4. **Логирует действие** в `activity_logs`

**Структура таблицы `event_assignments`:**
```sql
CREATE TABLE event_assignments (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    santa_user_id INTEGER,      -- Кто дарит (Дед Мороз)
    recipient_user_id INTEGER,  -- Кому дарят (Внучка)
    assigned_at TIMESTAMP,
    assigned_by INTEGER,        -- Администратор
    locked INTEGER,             -- Закреплена ли пара
    assignment_locked INTEGER,   -- Заблокировано ли всё распределение
    santa_sent_at TIMESTAMP,     -- Когда отправлен подарок
    santa_send_info TEXT,        -- Информация об отправке
    recipient_received_at TIMESTAMP,  -- Когда получен подарок
    recipient_thanks_message TEXT,    -- Благодарность
    recipient_receipt_image TEXT      -- Фото получения
)
```

**Защита от потери данных:**
```python
# Сохраняет текущее состояние перед удалением
existing_rows = conn.execute('SELECT ... FROM event_assignments WHERE event_id = ?')

# Удаляет старые назначения
conn.execute('DELETE FROM event_assignments WHERE event_id = ?')

try:
    # Вставляет новые назначения
    conn.executemany('INSERT INTO event_assignments ...')
    conn.commit()
except Exception as e:
    # В случае ошибки восстанавливает старое состояние
    conn.rollback()
    conn.executemany('INSERT INTO event_assignments ...', existing_rows)
```

## Маршруты и интерфейс

### 1. Просмотр распределения
**Маршрут:** `GET /admin/events/<event_id>/distribution/positive`

**Функция:** `admin_event_distribution_positive_view`

**Что делает:**
- Показывает список утверждённых участников
- Отображает сохранённые пары (если есть)
- Позволяет генерировать новое распределение
- Позволяет редактировать пары вручную

### 2. Генерация распределения
**Маршрут:** `POST /admin/events/<event_id>/distribution/positive/random`

**Функция:** `admin_event_distribution_positive_generate`

**Параметры запроса (JSON):**
```json
{
    "group_by_country": true,  // Распределение по странам
    "locked_pairs": [           // Закреплённые пары
        {"santa_id": 123, "recipient_id": 456}
    ],
    "assignment_locked_santas": [789]  // Заблокированные участники
}
```

**Ответ:**
```json
{
    "success": true,
    "pairs": [
        {
            "santa_id": 123,
            "santa_name": "User1",
            "santa_country": "Россия",
            "recipient_id": 456,
            "recipient_name": "User2",
            "recipient_country": "Россия",
            "locked": false,
            "assignment_locked": false
        }
    ],
    "country_mode_applied": true
}
```

### 3. Сохранение распределения
**Маршрут:** `POST /admin/events/<event_id>/distribution/positive/save`

**Функция:** `admin_event_distribution_positive_save`

**Параметры запроса (JSON):**
```json
{
    "pairs": [...],  // Список пар для сохранения
    "locked_pairs": [...],  // Закреплённые пары
    "enforce_country": true  // Проверять соответствие стран
}
```

**Валидация:**
- Каждый участник должен быть в списке утверждённых
- Каждый Дед Мороз должен встречаться ровно один раз
- Каждый получатель должен встречаться ровно один раз
- Участник не может быть назначен самому себе
- Если `enforce_country=true`: страны должны совпадать

### 4. Создание заданий (финальная блокировка)
**Маршрут:** `POST /admin/events/<event_id>/distribution/positive/assignments`

**Функция:** `admin_event_distribution_positive_create_assignments`

**Что делает:**
- Блокирует распределение (`assignment_locked = 1`)
- После этого распределение нельзя изменить
- Участники получают доступ к информации о получателе

**Требования:**
- Все пары должны быть закреплены (`locked = 1`)

### 5. Отмена назначения
**Маршрут:** `POST /admin/events/<event_id>/distribution/positive/unassign`

**Функция:** `admin_event_distribution_positive_unassign`

**Что делает:**
- Удаляет назначение для конкретного Деда Мороза
- Позволяет перераспределить этого участника

## Особенности и ограничения

### Гарантии
1. **Каждый участник получает подарок** - циклическое распределение гарантирует это
2. **Никто не дарит себе** - проверка `santa_id != recipient_id`
3. **Каждый участник дарит ровно один раз** - уникальность в БД
4. **Каждый участник получает ровно один раз** - проверка при сохранении

### Ограничения
1. **Минимум 2 участника** - иначе распределение невозможно
2. **Все участники должны быть утверждены** - только `approved = 1`
3. **Распределение по странам может быть невозможно** - если не хватает участников из одной страны
4. **Максимум 3000 попыток** - если не удаётся создать валидные пары, возвращается ошибка

### Обработка ошибок
```python
# Недостаточно участников
if len(participants) < 2:
    return jsonify({'success': False, 'error': 'Недостаточно участников'}), 400

# Не удалось создать валидные пары
if extra_pairs is None:
    error_message = 'Не удалось сформировать уникальные пары, попробуйте снова'
    if locked_assignments:
        error_message += ' Убедитесь, что закреплённые пары не блокируют распределение.'
    return jsonify({'success': False, 'error': error_message}), 500
```

## Пример использования

### Сценарий 1: Простая жеребьёвка
```python
# Администратор нажимает "Сгенерировать случайное распределение"
success, message = create_random_assignments(event_id=1, assigned_by=admin_id)
# Результат: создано циклическое распределение для всех участников
```

### Сценарий 2: Жеребьёвка с распределением по странам
```python
# Администратор включает опцию "По странам" и генерирует распределение
POST /admin/events/1/distribution/positive/random
{
    "group_by_country": true
}
# Результат: пары формируются так, чтобы Дед Мороз и получатель были из одной страны
```

### Сценарий 3: Ручная корректировка
```python
# 1. Генерируется автоматическое распределение
# 2. Администратор видит пары и может их редактировать
# 3. Некоторые пары закрепляются (locked = true)
# 4. Сохраняется финальное распределение
POST /admin/events/1/distribution/positive/save
{
    "pairs": [...],
    "locked_pairs": [{"santa_id": 123, "recipient_id": 456}]
}
```

### Сценарий 4: Финальная блокировка
```python
# После проверки и корректировки администратор блокирует распределение
POST /admin/events/1/distribution/positive/assignments
# Теперь участники могут видеть, кому они дарят подарок
```

## Логирование

Все действия жеребьёвки логируются в таблицу `activity_logs`:

```python
log_activity(
    'assignments_saved',
    details=f'Сохранено распределение для мероприятия #{event_id}',
    metadata={
        'event_id': event_id,
        'pairs_count': len(assignments),
        'assigned_by': assigned_by,
    },
    user_id=assigned_by
)
```

## Безопасность

1. **Только администраторы** могут проводить жеребьёвку (`@require_role('admin')`)
2. **Проверка валидности** всех пар перед сохранением
3. **Защита от потери данных** - сохранение состояния перед удалением
4. **Логирование действий** для аудита

## Заключение

Механизм жеребьёвки обеспечивает:
- ✅ Случайное и справедливое распределение
- ✅ Гибкость (ручная корректировка, распределение по странам)
- ✅ Безопасность (валидация, логирование)
- ✅ Надёжность (защита от потери данных)

Система поддерживает как простую автоматическую жеребьёвку, так и продвинутое управление с ручной корректировкой и дополнительными опциями.

