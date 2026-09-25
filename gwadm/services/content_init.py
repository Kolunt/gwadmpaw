"""Default content and settings helpers."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.settings import get_setting

def get_faq_categories():
    """Получает список активных категорий FAQ"""
    conn = get_db_connection()
    categories = conn.execute('''
        SELECT * FROM faq_categories 
        WHERE is_active = 1 
        ORDER BY sort_order, display_name
    ''').fetchall()
    conn.close()
    return [dict(c) for c in categories]



def set_setting(key, value, description=None, category='general'):
    """Устанавливает значение настройки"""
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR REPLACE INTO settings (key, value, description, category, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (key, value, description or '', category))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error setting {key}: {e}")
        conn.close()
        return False


def init_default_rules():
    """Инициализирует дефолтные правила для тестирования"""
    import json
    conn = get_db_connection()
    try:
        # Проверяем, есть ли уже правила с непустым содержимым
        existing = conn.execute('SELECT * FROM settings WHERE key = ?', ('rules_content',)).fetchone()
        if existing:
            # Проверяем, есть ли реальное содержимое (не пустая строка и не только пробелы)
            existing_value = existing.get('value', '').strip() if existing.get('value') else ''
            if existing_value:
                # Пытаемся распарсить как JSON, чтобы убедиться, что это валидные правила
                try:
                    parsed = json.loads(existing_value)
                    if isinstance(parsed, list) and len(parsed) > 0:
                        # Если это валидный JSON с правилами, не добавляем дефолтные
                        conn.close()
                        return
                except (json.JSONDecodeError, ValueError):
                    # Если не JSON, но есть содержимое - возможно старый HTML формат
                    # Не добавляем дефолтные, чтобы не перезаписать существующие
                    conn.close()
                    return
            # Если значение пустое, удаляем запись и создадим новую с дефолтными правилами
            conn.execute('DELETE FROM settings WHERE key = ?', ('rules_content',))
            conn.commit()
        
        # Дефолтные правила для тестирования
        default_rules = [
            {'point': '1', 'text': 'Все участники должны быть зарегистрированы через GWars и иметь активный аккаунт в игре.'},
            {'point': '1.1', 'text': 'При регистрации на мероприятие необходимо заполнить все обязательные поля профиля.'},
            {'point': '1.1.1', 'text': 'Обязательные поля включают: фамилию, имя, отчество, полный адрес и хотя бы один контакт для связи.'},
            {'point': '1.2', 'text': 'Администраторы могут участвовать в мероприятиях без заполнения обязательных полей профиля.'},
            {'point': '2', 'text': 'Регистрация на мероприятие возможна только в установленные администратором сроки.'},
            {'point': '2.1', 'text': 'Предварительная регистрация открывается для раннего участия.'},
            {'point': '2.2', 'text': 'Основная регистрация является основным периодом для записи на мероприятие.'},
            {'point': '2.3', 'text': 'После закрытия регистрации отмена участия невозможна.'},
            {'point': '3', 'text': 'Жеребьёвка проводится автоматически после закрытия регистрации.'},
            {'point': '3.1', 'text': 'Каждый участник получает случайного получателя подарка.'},
            {'point': '3.2', 'text': 'Информация о получателе становится доступна только после завершения жеребьёвки.'},
            {'point': '4', 'text': 'Подарки должны быть отправлены в установленные сроки.'},
            {'point': '4.1', 'text': 'Участник обязан отправить подарок своему получателю до даты праздника.'},
            {'point': '4.2', 'text': 'Администраторы могут отслеживать статус отправки подарков.'},
            {'point': '5', 'text': 'Конфиденциальность участников строго соблюдается.'},
            {'point': '5.1', 'text': 'Имя отправителя подарка остается неизвестным получателю до даты праздника.'},
            {'point': '5.2', 'text': 'Контактная информация участников видна только администраторам и самим участникам.'},
            {'point': '6', 'text': 'Нарушение правил может привести к исключению из мероприятия или системы.'},
            {'point': '6.1', 'text': 'Администраторы оставляют за собой право исключить участника за нарушение правил.'},
            {'point': '6.2', 'text': 'При исключении участника его получатель подарка будет переназначен другому участнику.'},
        ]
        
        # Сохраняем в JSON формате
        rules_json = json.dumps(default_rules, ensure_ascii=False, indent=2)
        
        # Получаем первого администратора для created_by
        admin_user = conn.execute('SELECT user_id FROM users WHERE user_id IN (SELECT user_id FROM user_roles WHERE role_id = (SELECT id FROM roles WHERE name = "admin")) LIMIT 1').fetchone()
        created_by = admin_user['user_id'] if admin_user else None
        
        # Сохраняем правила в настройках
        # Проверяем структуру таблицы settings
        table_info = conn.execute("PRAGMA table_info(settings)").fetchall()
        columns = [col[1] for col in table_info]
        
        # Формируем запрос в зависимости от наличия колонок
        if 'updated_at' in columns and 'updated_by' in columns:
            # Структура с updated_at и updated_by
            conn.execute('''
                INSERT INTO settings (key, value, category, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now(), created_by))
        elif 'updated_at' in columns:
            # Структура только с updated_at
            conn.execute('''
                INSERT INTO settings (key, value, category, updated_at)
                VALUES (?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now()))
        else:
            # Минимальная структура
            conn.execute('''
                INSERT INTO settings (key, value, category)
                VALUES (?, ?, ?)
            ''', ('rules_content', rules_json, 'general'))
        
        conn.commit()
        log_debug("Default rules initialized successfully")
        log_debug(f"Rules JSON length: {len(rules_json)}")
    except Exception as e:
        log_error(f"Error initializing default rules: {e}")
        log_error(traceback.format_exc())
    finally:
        conn.close()

def init_default_modal_texts():
    """Инициализирует дефолтные тексты модальных окон для регистрации на мероприятия"""
    conn = get_db_connection()
    try:
        # Дефолтные тексты модальных окон
        default_modal_texts = {
            'modal_title': ('Заполнение обязательных данных', 'Заголовок модального окна регистрации'),
            'modal_intro_title_new': ('Для регистрации на мероприятие необходимо заполнить обязательную информацию.', 'Заголовок вступительного сообщения (новый пользователь)'),
            'modal_intro_text_new': ('Это необходимо для организации мероприятия и связи с участниками.', 'Текст вступительного сообщения (новый пользователь)'),
            'modal_intro_description_new': ('Мы поможем вам заполнить все необходимые данные пошагово.', 'Описание вступительного сообщения (новый пользователь)'),
            'modal_intro_title_existing': ('Ваши данные уже заполнены в системе.', 'Заголовок вступительного сообщения (существующий пользователь)'),
            'modal_intro_text_existing': ('Мы просто хотим убедиться, что все данные актуальны.', 'Текст вступительного сообщения (существующий пользователь)'),
            'modal_intro_description_existing': ('Пожалуйста, проверьте и подтвердите ваши данные.', 'Описание вступительного сообщения (существующий пользователь)'),
            'modal_step_personal_title': ('Шаг 1: Личные данные', 'Заголовок шага личных данных'),
            'modal_step_address_title': ('Шаг 2: Адрес', 'Заголовок шага адреса'),
            'modal_step_contact_title_prefix': ('Шаг', 'Префикс заголовка шага контактов'),
            'modal_step_contact_description_required': ('Для связи с вами необходимо указать хотя бы один способ связи.', 'Описание шага контактов (обязательное поле)'),
            'modal_step_contact_description_optional': ('Вы можете добавить еще один способ связи или пропустить этот шаг.', 'Описание шага контактов (необязательное поле)'),
            'modal_step_contact_description_review': ('Проверьте ваш контакт. Вы можете изменить его или подтвердить.', 'Описание шага контактов (проверка)'),
            'modal_final_title': ('🎉 Все данные собраны, вы готовы стать Анонимным Дедом Морозом!', 'Заголовок финального шага'),
            'modal_final_text': ('Теперь вы можете принять участие в мероприятии и подарить радость другим участникам!', 'Текст финального шага'),
            'modal_btn_back': ('Назад', 'Кнопка "Назад"'),
            'modal_btn_next': ('Далее', 'Кнопка "Далее"'),
            'modal_btn_skip': ('Пропустить', 'Кнопка "Пропустить"'),
            'modal_btn_not_using': ('Не использую', 'Кнопка "Не использую"'),
            'modal_btn_confirm': ('Без сомнений, участвую!', 'Кнопка подтверждения участия'),
            'modal_btn_cancel': ('Я ещё подумаю...', 'Кнопка отмены'),
            'modal_btn_save_continue': ('Сохранить и продолжить', 'Кнопка "Сохранить и продолжить"'),
            'modal_btn_finish_register': ('Завершить и зарегистрироваться', 'Кнопка "Завершить и зарегистрироваться"'),
            'modal_error_email_invalid': ('Это не email', 'Сообщение об ошибке: неверный email'),
        }
        
        # Проверяем, есть ли уже настройки модальных окон
        existing = conn.execute('SELECT key FROM settings WHERE category = ?', ('modals',)).fetchone()
        if existing:
            # Если уже есть настройки, не добавляем дефолтные
            conn.close()
            return
        
        # Получаем первого администратора для created_by
        admin_user = conn.execute('SELECT user_id FROM users WHERE user_id IN (SELECT user_id FROM user_roles WHERE role_id = (SELECT id FROM roles WHERE name = "admin")) LIMIT 1').fetchone()
        created_by = admin_user['user_id'] if admin_user else None
        
        # Добавляем настройки
        for key, (value, description) in default_modal_texts.items():
            set_setting(key, value, description, 'modals')
        
        conn.commit()
        log_debug("Default modal texts initialized")
    except Exception as e:
        log_error(f"Error initializing default modal texts: {e}")
    finally:
        conn.close()


def init_default_faq_items():
    """Инициализирует дефолтные FAQ элементы из статического контента"""
    conn = get_db_connection()
    try:
        # Проверяем, есть ли уже FAQ элементы
        existing_count = conn.execute('SELECT COUNT(*) as count FROM faq_items').fetchone()['count']
        if existing_count > 0:
            # Если уже есть элементы, не добавляем дефолтные
            conn.close()
            return
        
        # Дефолтные FAQ элементы
        default_faq_items = [
            # Общие вопросы
            ('Что такое "Анонимные Деды Морозы"?', 
             '<p>Это платформа для анонимного обмена подарками между участниками. Вы можете зарегистрироваться на мероприятие, получить случайного получателя подарка и отправить ему подарок, оставаясь инкогнито до праздника.</p>', 
             'general', 10),
            ('Как зарегистрироваться?', 
             '<p>Для регистрации необходимо войти через GWars. Нажмите на иконку GWars в правом верхнем углу или используйте кнопку "Войти через GWars" на главной странице. После авторизации ваш аккаунт будет автоматически создан в системе.</p>', 
             'general', 20),
            ('Могу ли я участвовать без авторизации через GWars?', 
             '<p>Нет, для участия в мероприятиях необходимо авторизоваться через GWars. Это обеспечивает безопасность и проверку личности участников.</p>', 
             'general', 30),
            
            # Мероприятия
            ('Как принять участие в мероприятии?', 
             '<p>Перейдите в раздел "Мероприятия" в меню, выберите интересующее вас мероприятие и нажмите "Зарегистрироваться". Регистрация доступна только в определенные периоды времени, указанные администратором.</p>', 
             'events', 10),
            ('Что такое этапы мероприятия?', 
             '<p>Каждое мероприятие состоит из нескольких этапов:</p><ul><li><strong>Предварительная регистрация</strong> - ранний этап для желающих участвовать</li><li><strong>Основная регистрация</strong> - основной период регистрации</li><li><strong>Закрытие регистрации</strong> - регистрация завершена</li><li><strong>Жеребьёвка</strong> - случайное распределение получателей подарков</li><li><strong>Отправка подарков</strong> - период отправки подарков</li><li><strong>Обмен подарками</strong> - день, когда подарки вручаются</li><li><strong>Мероприятие завершено</strong> - период для обмена впечатлениями</li></ul>', 
             'events', 20),
            ('Как узнать, кому я должен отправить подарок?', 
             '<p>После завершения жеребьёвки вы получите информацию о получателе подарка в личном кабинете. Вы узнаете, кому отправлять подарок, но ваше имя останется неизвестным получателю до праздника.</p>', 
             'events', 30),
            ('Можно ли отменить регистрацию на мероприятие?', 
             '<p>Отмена регистрации возможна только до начала этапа "Закрытие регистрации". После этого этапа отмена регистрации невозможна, так как начинается процесс жеребьёвки.</p>', 
             'events', 40),
            
            # Профиль и настройки
            ('Как изменить свой аватар?', 
             '<p>Перейдите в свой профиль (иконка в правом верхнем углу) и нажмите "Редактировать профиль". В разделе "Аватар" вы сможете выбрать новый аватар из библиотеки DiceBear. Каждый аватар уникален и не может быть повторен.</p>', 
             'profile', 10),
            ('Что такое звания и как их получить?', 
             '<p>Звания - это особые метки, которые администраторы могут присвоить пользователям за вклад в проект. Существуют звания: Автор идеи, Разработчик, Амбассадор, Дизайнер. Звания отображаются в вашем профиле.</p>', 
             'profile', 20),
            ('Можно ли изменить информацию из GWars?', 
             '<p>Нет, информация из GWars (имя, уровень, синдикат и т.д.) синхронизируется автоматически и не может быть изменена вручную. Вы можете редактировать только дополнительную информацию: биографию, контакты и аватар.</p>', 
             'profile', 30),
            
            # Технические вопросы
            ('Поддерживается ли мобильная версия?', 
             '<p>Да, сайт полностью адаптирован для мобильных устройств. Вы можете использовать меню, свайпая его влево-вправо для открытия/закрытия и вверх-вниз для прокрутки. Также доступна темная и светлая темы оформления.</p>', 
             'technical', 10),
            ('Как переключить тему оформления?', 
             '<p>Используйте иконку переключения темы в правом верхнем углу сайта. Выбранная тема сохраняется и будет использоваться при следующих посещениях.</p>', 
             'technical', 20),
            ('Что делать, если возникла ошибка при входе?', 
             '<p>Если при входе через GWars возникает ошибка, убедитесь, что вы авторизованы на сайте GWars.io. Если проблема сохраняется, обратитесь к администратору через контакты в вашем профиле или напишите в поддержку GWars.</p>', 
             'technical', 30),
            
            # Безопасность и конфиденциальность
            ('Безопасны ли мои данные?', 
             '<p>Да, мы используем безопасную авторизацию через GWars и не храним пароли пользователей. Вся информация передается через защищенные соединения (HTTPS). Ваши контактные данные видны только вам и администраторам системы.</p>', 
             'security', 10),
            ('Кто может видеть мой профиль?', 
             '<p>Ваш профиль доступен для просмотра всем авторизованным пользователям. Однако контактная информация (email, телефон, мессенджеры) видна только вам и администраторам. Остальные пользователи видят только публичную информацию: имя, уровень, синдикат, роли, звания и биографию.</p>', 
             'security', 20),
        ]
        
        # Получаем первого администратора для created_by
        admin_user = conn.execute('SELECT user_id FROM users WHERE user_id IN (SELECT user_id FROM user_roles WHERE role_id = (SELECT id FROM roles WHERE name = "admin")) LIMIT 1').fetchone()
        created_by = admin_user['user_id'] if admin_user else None
        
        # Добавляем FAQ элементы
        for question, answer, category, sort_order in default_faq_items:
            conn.execute('''
                INSERT INTO faq_items (question, answer, category, sort_order, is_active, created_by, created_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
            ''', (question, answer, category, sort_order, created_by, datetime.now()))
        
        conn.commit()
        log_debug("Default FAQ items initialized")
    except Exception as e:
        log_error(f"Error initializing default FAQ items: {e}")
    finally:
        conn.close()
