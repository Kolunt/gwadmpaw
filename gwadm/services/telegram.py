"""Telegram bot and verification helpers."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
def handle_telegram_message(message):
    """Обрабатывает сообщения от пользователей в Telegram"""
    chat_id = message.get('chat', {}).get('id')
    text = message.get('text', '').strip()
    username = message.get('from', {}).get('username')
    
    if not chat_id:
        return jsonify({'ok': False, 'error': 'No chat_id'}), 400
    
    # Обработка команд
    if text.startswith('/'):
        command = text.split()[0].lower()
        
        if command == '/start':
            return handle_start_command(chat_id, username, text)
        elif command == '/menu':
            return handle_menu_command(chat_id)
        elif command == '/verify':
            return handle_verify_command(chat_id, text)
        elif command == '/events':
            return handle_events_command(chat_id)
        elif command == '/assignments':
            return handle_assignments_command(chat_id)
        else:
            send_telegram_message_with_keyboard(
                "Неизвестная команда. Используйте /menu для просмотра меню.",
                chat_id
            )
            return jsonify({'ok': True})
    
    # Обработка кода верификации (6 цифр)
    elif text.isdigit() and len(text) == 6:
        return handle_verification_code(chat_id, text, username)
    
    # Обработка обычных сообщений
    else:
        send_telegram_message_with_keyboard(
            "Используйте /menu для просмотра доступных команд.",
            chat_id
        )
        return jsonify({'ok': True})


def handle_telegram_callback(callback_query):
    """Обрабатывает нажатия на inline кнопки"""
    chat_id = callback_query.get('message', {}).get('chat', {}).get('id')
    data = callback_query.get('data', '')
    
    if not chat_id:
        return jsonify({'ok': False, 'error': 'No chat_id'}), 400
    
    # Обработка callback_data
    if data.startswith('cmd_'):
        command = data.replace('cmd_', '')
        if command == 'events':
            return handle_events_command(chat_id)
        elif command == 'assignments':
            return handle_assignments_command(chat_id)
        elif command == 'faq':
            return handle_faq_command(chat_id)
        elif command == 'rules':
            return handle_rules_command(chat_id)
    
    # Отправляем подтверждение нажатия кнопки
    token = get_setting('telegram_bot_token', '')
    if token:
        try:
            api_url = f'https://api.telegram.org/bot{token}/answerCallbackQuery'
            requests.post(api_url, json={'callback_query_id': callback_query.get('id')}, timeout=5)
        except:
            pass
    
    return jsonify({'ok': True})


def handle_start_command(chat_id, username, full_text):
    """Обрабатывает команду /start"""
    # Проверяем, есть ли код верификации в команде
    parts = full_text.split()
    if len(parts) > 1:
        verification_code = parts[1]
        # Это может быть код верификации или user_id
        return handle_start_with_code(chat_id, username, verification_code)
    
    # Обычное приветствие
    welcome_text = (
        "👋 Добро пожаловать в бота Анонимных Дедов Морозов!\n\n"
        "Для использования бота необходимо привязать ваш аккаунт.\n"
        "Перейдите в свой профиль на сайте и запросите код верификации."
    )
    
    # Проверяем, есть ли настроенное меню
    menu_items = get_telegram_bot_menu()
    keyboard = None
    if menu_items:
        # Формируем inline клавиатуру из меню
        keyboard = {'inline_keyboard': []}
        row = []
        
        for item in menu_items:
            button_text = item['button_text']
            button_type = item['button_type']
            action = item['action']
            
            if button_type == 'command':
                row.append({'text': button_text, 'callback_data': f'cmd_{action}'})
            elif button_type == 'url':
                # Получаем базовый URL сайта
                base_url = get_base_url()
                full_url = action if action.startswith('http') else f"{base_url}{action}"
                row.append({'text': button_text, 'url': full_url})
            
            # Добавляем кнопки по 2 в ряд
            if len(row) >= 2:
                keyboard['inline_keyboard'].append(row)
                row = []
        
        if row:
            keyboard['inline_keyboard'].append(row)
        
        welcome_text += "\n\n📋 Выберите раздел:"
    
    send_telegram_message_with_keyboard(welcome_text, chat_id, keyboard)
    return jsonify({'ok': True})


def handle_start_with_code(chat_id, username, code):
    """Обрабатывает /start с кодом верификации"""
    # Если код - это user_id (обычно 6+ цифр), генерируем код верификации
    if code.isdigit():
        try:
            user_id = int(code)
            # Проверяем, что это действительно user_id (обычно больше 100000)
            # Или это может быть код верификации (ровно 6 цифр)
            if len(code) == 6:
                # Это код верификации
                handle_verification_code(chat_id, code, username)
            elif user_id > 100000:
                # Это user_id, генерируем код верификации
                verification_code = generate_telegram_verification_code(user_id)
                if verification_code:
                    send_telegram_message_with_keyboard(
                        f"Ваш код верификации: {verification_code}\n\n"
                        "Введите этот код в бота для завершения привязки аккаунта.\n"
                        f"Или используйте команду: /verify {verification_code}",
                        chat_id
                    )
                else:
                    send_telegram_message_with_keyboard(
                        "Ошибка при генерации кода. Попробуйте позже.",
                        chat_id
                    )
            else:
                # Небольшое число, возможно код верификации
                handle_verification_code(chat_id, code, username)
        except ValueError:
            # Не число, игнорируем
            send_telegram_message_with_keyboard(
                "Используйте /menu для просмотра доступных команд.",
                chat_id
            )
    else:
        # Не число, игнорируем
        send_telegram_message_with_keyboard(
            "Используйте /menu для просмотра доступных команд.",
            chat_id
        )
    
    return jsonify({'ok': True})


def handle_menu_command(chat_id):
    """Показывает меню бота"""
    menu_items = get_telegram_bot_menu()
    if not menu_items:
        send_telegram_message_with_keyboard("Меню пока не настроено.", chat_id)
        return jsonify({'ok': True})
    
    # Формируем inline клавиатуру
    keyboard = {'inline_keyboard': []}
    row = []
    
    for item in menu_items:
        button_text = item['button_text']
        button_type = item['button_type']
        action = item['action']
        
        if button_type == 'command':
            row.append({'text': button_text, 'callback_data': f'cmd_{action}'})
        elif button_type == 'url':
            # Получаем базовый URL сайта
            base_url = get_base_url()
            full_url = action if action.startswith('http') else f"{base_url}{action}"
            row.append({'text': button_text, 'url': full_url})
        
        # Добавляем кнопки по 2 в ряд
        if len(row) >= 2:
            keyboard['inline_keyboard'].append(row)
            row = []
    
    if row:
        keyboard['inline_keyboard'].append(row)
    
    menu_text = "📋 Главное меню:\n\nВыберите раздел:"
    send_telegram_message_with_keyboard(menu_text, chat_id, keyboard)
    return jsonify({'ok': True})


def handle_verify_command(chat_id, full_text):
    """Обрабатывает команду /verify"""
    parts = full_text.split()
    if len(parts) > 1:
        code = parts[1]
        username = None
        return handle_verification_code(chat_id, code, username)
    else:
        send_telegram_message_with_keyboard(
            "Для верификации введите команду:\n/verify <код>\n\n"
            "Код можно получить в вашем профиле на сайте.",
            chat_id
        )
    return jsonify({'ok': True})


def handle_verification_code(chat_id, code, username):
    """Обрабатывает код верификации"""
    conn = get_db_connection()
    try:
        # Ищем пользователя с этим кодом
        telegram_user = conn.execute('''
            SELECT user_id, verification_code, verification_code_expires_at
            FROM telegram_users
            WHERE verification_code = ? AND verified = 0
        ''', (code,)).fetchone()
        
        if not telegram_user:
            send_telegram_message_with_keyboard(
                "Код верификации не найден или уже использован.\n"
                "Запросите новый код в вашем профиле на сайте.",
                chat_id
            )
            return jsonify({'ok': True})
        
        user_id = telegram_user['user_id']
        expires_at_str = telegram_user['verification_code_expires_at']
        
        # Проверяем срок действия
        if expires_at_str:
            expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
            if expires_at.tzinfo:
                expires_at = expires_at.replace(tzinfo=None)
            if datetime.utcnow() > expires_at:
                send_telegram_message_with_keyboard(
                    "Код верификации истёк. Запросите новый код в вашем профиле.",
                    chat_id
                )
                return jsonify({'ok': True})
        
        # Связываем пользователя с Telegram
        success, message = verify_telegram_code(user_id, code, str(chat_id), username)
        
        if success:
            send_telegram_message_with_keyboard(
                f"✅ {message}\n\nИспользуйте /menu для просмотра доступных команд.",
                chat_id
            )
        else:
            send_telegram_message_with_keyboard(f"❌ {message}", chat_id)
        
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error handling verification code: {e}")
        send_telegram_message_with_keyboard(
            "Произошла ошибка при верификации. Попробуйте позже.",
            chat_id
        )
        return jsonify({'ok': True})
    finally:
        conn.close()


def get_base_url():
    """Получает базовый URL сайта"""
    # Сначала проверяем настройку из БД
    site_url = get_setting('site_url', '')
    if site_url:
        return site_url.rstrip('/')
    
    # Затем пытаемся получить из request
    try:
        if has_request_context():
            return request.host_url.rstrip('/')
    except:
        pass
    
    # Fallback: дефолтное значение для разработки
    return 'http://localhost:5000'


def handle_events_command(chat_id):
    """Показывает список мероприятий"""
    conn = get_db_connection()
    try:
        events = conn.execute('''
            SELECT id, name, description
            FROM events
            WHERE deleted_at IS NULL
            ORDER BY created_at DESC
            LIMIT 10
        ''').fetchall()
        conn.close()
        
        if not events:
            send_telegram_message_with_keyboard("Мероприятия не найдены.", chat_id)
            return jsonify({'ok': True})
        
        base_url = get_base_url()
        text = "🎉 Мероприятия:\n\n"
        keyboard = {'inline_keyboard': []}
        
        for event in events:
            event_id = event['id']
            event_name = event['name']
            event_url = f"{base_url}/events#{event_id}"
            text += f"• {event_name}\n"
            keyboard['inline_keyboard'].append([{
                'text': f"📋 {event_name}",
                'url': event_url
            }])
        
        send_telegram_message_with_keyboard(text, chat_id, keyboard)
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error handling events command: {e}")
        send_telegram_message_with_keyboard("Ошибка при получении мероприятий.", chat_id)
        return jsonify({'ok': True})


def handle_assignments_command(chat_id):
    """Показывает задания пользователя"""
    conn = get_db_connection()
    try:
        # Находим user_id по chat_id
        telegram_user = conn.execute('''
            SELECT user_id FROM telegram_users
            WHERE telegram_chat_id = ? AND verified = 1
        ''', (str(chat_id),)).fetchone()
        
        if not telegram_user:
            send_telegram_message_with_keyboard(
                "Ваш аккаунт не привязан к Telegram. Используйте /verify для привязки.",
                chat_id
            )
            return jsonify({'ok': True})
        
        user_id = telegram_user['user_id']
        
        # Получаем задания пользователя
        assignments = conn.execute('''
            SELECT ea.id, ea.event_id, e.name as event_name,
                   ea.recipient_user_id, u.username as recipient_username,
                   ea.santa_sent_at, ea.recipient_received_at
            FROM event_assignments ea
            JOIN events e ON ea.event_id = e.id
            JOIN users u ON ea.recipient_user_id = u.user_id
            WHERE ea.santa_user_id = ? AND e.deleted_at IS NULL
            ORDER BY ea.assigned_at DESC
            LIMIT 10
        ''', (user_id,)).fetchall()
        conn.close()
        
        if not assignments:
            send_telegram_message_with_keyboard(
                "У вас пока нет заданий.",
                chat_id
            )
            return jsonify({'ok': True})
        
        base_url = get_base_url()
        text = "📋 Ваши задания:\n\n"
        keyboard = {'inline_keyboard': []}
        
        for assignment in assignments:
            event_name = assignment['event_name']
            recipient = assignment['recipient_username']
            sent = "✅" if assignment['santa_sent_at'] else "⏳"
            received = "✅" if assignment['recipient_received_at'] else "⏳"
            
            text += f"{sent} Отправить: {recipient}\n"
            text += f"{received} Получить от: {recipient}\n"
            text += f"Мероприятие: {event_name}\n\n"
            
            assignment_url = f"{base_url}/assignments"
            keyboard['inline_keyboard'].append([{
                'text': f"📋 {event_name}",
                'url': assignment_url
            }])
        
        send_telegram_message_with_keyboard(text, chat_id, keyboard)
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error handling assignments command: {e}")
        send_telegram_message_with_keyboard("Ошибка при получении заданий.", chat_id)
        return jsonify({'ok': True})


def handle_faq_command(chat_id):
    """Отправляет ссылку на FAQ"""
    base_url = get_base_url()
    faq_url = f"{base_url}/faq"
    keyboard = {'inline_keyboard': [[{'text': '📖 Открыть FAQ', 'url': faq_url}]]}
    send_telegram_message_with_keyboard(
        "❓ Часто задаваемые вопросы:\n\nНажмите кнопку ниже, чтобы открыть FAQ на сайте.",
        chat_id,
        keyboard
    )
    return jsonify({'ok': True})


def handle_rules_command(chat_id):
    """Отправляет ссылку на правила"""
    base_url = get_base_url()
    rules_url = f"{base_url}/rules"
    keyboard = {'inline_keyboard': [[{'text': '📜 Открыть правила', 'url': rules_url}]]}
    send_telegram_message_with_keyboard(
        "📜 Правила проекта:\n\nНажмите кнопку ниже, чтобы открыть правила на сайте.",
        chat_id,
        keyboard
    )
    return jsonify({'ok': True})


def verify_dadata_api(api_key, secret_key):
    """Проверяет валидность Dadata API ключей"""
    if not requests:
        return False, "Библиотека requests не установлена. Установите: pip install requests"
    
    if not api_key or not secret_key:
        return False, "API ключ и Secret ключ обязательны"
    
    try:
        # Используем простой endpoint для проверки (например, версия API)
        headers = {
            'Authorization': f'Token {api_key}',
            'X-Secret': secret_key,
            'Content-Type': 'application/json'
        }
        
        # Проверяем через endpoint /v1/version (более легкий запрос)
        response = requests.get('https://dadata.ru/api/v1/version', headers=headers, timeout=5)
        
        if response.status_code == 200:
            return True, "Ключи успешно проверены"
        elif response.status_code == 401:
            return False, "Неверный API ключ или Secret ключ"
        elif response.status_code == 403:
            return False, "Доступ запрещен. Проверьте права доступа для ключей"
        else:
            return False, f"Ошибка проверки: {response.status_code} - {response.text[:100]}"
    except requests.exceptions.Timeout:
        return False, "Таймаут при подключении к Dadata API"
    except requests.exceptions.ConnectionError:
        return False, "Ошибка подключения к Dadata API. Проверьте интернет-соединение"
    except Exception as e:
        return False, f"Ошибка при проверке: {str(e)}"



def verify_smtp_connection(host, port, username, password, use_tls=False, from_email=None):
    """Проверяет подключение к SMTP серверу"""
    import smtplib
    from email.mime.text import MIMEText
    
    if not host or not port or not username or not password:
        return False, "Все поля обязательны для заполнения"
    
    try:
        port_int = int(port)
        if port_int < 1 or port_int > 65535:
            return False, "Порт должен быть в диапазоне 1-65535"
    except ValueError:
        return False, "Порт должен быть числом"
    
    try:
        # Создаем подключение к SMTP серверу
        if use_tls:
            # Для TLS (порт 587)
            server = smtplib.SMTP(host, port_int, timeout=10)
            server.starttls()
        else:
            # Для SSL (порт 465) или без шифрования (порт 25)
            if port_int == 465:
                server = smtplib.SMTP_SSL(host, port_int, timeout=10)
            else:
                server = smtplib.SMTP(host, port_int, timeout=10)
        
        # Пытаемся авторизоваться
        server.login(username, password)
        
        # Если указан email отправителя, пытаемся отправить тестовое письмо
        if from_email:
            try:
                test_msg = MIMEText('Тестовое письмо для проверки SMTP подключения.')
                test_msg['Subject'] = 'Проверка SMTP - Анонимные Деды Морозы'
                test_msg['From'] = from_email
                test_msg['To'] = from_email  # Отправляем себе для проверки
                
                # Отправляем тестовое письмо
                server.sendmail(from_email, [from_email], test_msg.as_string())
                server.quit()
                return True, "Подключение успешно. Тестовое письмо отправлено на " + from_email
            except Exception as e:
                server.quit()
                return False, f"Подключение установлено, но не удалось отправить тестовое письмо: {str(e)}"
        else:
            server.quit()
            return True, "Подключение успешно установлено"
            
    except smtplib.SMTPAuthenticationError:
        return False, "Ошибка аутентификации. Проверьте логин и пароль"
    except smtplib.SMTPConnectError as e:
        return False, f"Ошибка подключения к серверу: {str(e)}"
    except smtplib.SMTPException as e:
        return False, f"Ошибка SMTP: {str(e)}"
    except Exception as e:
        return False, f"Ошибка при проверке: {str(e)}"


def verify_telegram_bot(token, chat_id=None):
    """Проверяет подключение к Telegram боту"""
    if not requests:
        return False, "Библиотека requests не установлена. Установите: pip install requests"
    
    if not token:
        return False, "Токен бота обязателен"
    
    try:
        # Проверяем токен через getMe
        api_url = f'https://api.telegram.org/bot{token}/getMe'
        response = requests.get(api_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('ok'):
                bot_info = data.get('result', {})
                bot_username = bot_info.get('username', 'неизвестен')
                bot_name = bot_info.get('first_name', 'Бот')
                
                # Пытаемся настроить вебхук автоматически
                try:
                    base_url = get_base_url()
                    webhook_url = f"{base_url}/telegram/webhook"
                    webhook_api_url = f'https://api.telegram.org/bot{token}/setWebhook'
                    webhook_response = requests.post(webhook_api_url, json={
                        'url': webhook_url
                    }, timeout=10)
                    if webhook_response.status_code == 200:
                        webhook_result = webhook_response.json()
                        if webhook_result.get('ok'):
                            log_debug(f"Webhook set successfully: {webhook_url}")
                        else:
                            log_error(f"Failed to set webhook: {webhook_result.get('description')}")
                except Exception as e:
                    log_error(f"Error setting webhook: {e}")
                    # Не критично, продолжаем
                
                # Устанавливаем команды меню бота
                try:
                    set_telegram_bot_commands(token)
                except Exception as e:
                    log_error(f"Error setting bot commands: {e}")
                    # Не критично, продолжаем
                
                # Если указан chat_id, пытаемся отправить тестовое сообщение
                if chat_id:
                    try:
                        send_url = f'https://api.telegram.org/bot{token}/sendMessage'
                        send_data = {
                            'chat_id': chat_id,
                            'text': '✅ Тестовое сообщение от бота "Анонимные Деды Морозы". Интеграция работает!'
                        }
                        send_response = requests.post(send_url, json=send_data, timeout=10)
                        
                        if send_response.status_code == 200 and send_response.json().get('ok'):
                            return True, f"Бот '{bot_name}' (@{bot_username}) подключен. Тестовое сообщение отправлено в чат {chat_id}"
                        else:
                            error_data = send_response.json() if send_response.status_code == 200 else {}
                            error_desc = error_data.get('description', 'Неизвестная ошибка')
                            return False, f"Бот подключен, но не удалось отправить сообщение в чат {chat_id}: {error_desc}"
                    except requests.exceptions.RequestException as e:
                        return False, f"Бот подключен, но ошибка при отправке тестового сообщения: {str(e)}"
                else:
                    return True, f"Бот '{bot_name}' (@{bot_username}) успешно подключен. Chat ID не указан - можно отправлять сообщения по username"
            else:
                return False, "Неверный ответ от Telegram API"
        elif response.status_code == 401:
            return False, "Неверный токен бота. Проверьте токен от @BotFather"
        else:
            error_text = response.text[:200] if response.text else 'Неизвестная ошибка'
            return False, f"Ошибка проверки: {response.status_code} - {error_text}"
            
    except requests.exceptions.Timeout:
        return False, "Таймаут при подключении к Telegram API"
    except requests.exceptions.ConnectionError:
        return False, "Ошибка подключения к Telegram API. Проверьте интернет-соединение"
    except Exception as e:
        return False, f"Ошибка при проверке: {str(e)}"


def send_telegram_message(message, chat_id=None, parse_mode=None):
    """Отправляет сообщение через Telegram бота
    
    Args:
        message: Текст сообщения
        chat_id: Chat ID или username (может начинаться с @) получателя. 
                 Если не указан, используется из настроек.
        parse_mode: Режим парсинга (HTML, Markdown и т.д.)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not requests:
        return False, "Библиотека requests не установлена"
    
    # Получаем настройки Telegram
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    if not telegram_enabled:
        return False, "Telegram бот не включен в настройках"
    
    telegram_verified = get_setting('telegram_verified', '0') == '1'
    if not telegram_verified:
        return False, "Telegram бот не проверен. Проверьте подключение в настройках"
    
    token = get_setting('telegram_bot_token', '')
    if not token:
        return False, "Токен бота не настроен"
    
    # Используем chat_id из параметра или из настроек
    target_chat_id = chat_id or get_setting('telegram_chat_id', '')
    if not target_chat_id:
        return False, "Chat ID не указан. Укажите chat_id в параметрах или настройках"
    
    try:
        api_url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = {
            'chat_id': target_chat_id,
            'text': message
        }
        if parse_mode:
            data['parse_mode'] = parse_mode
        
        response = requests.post(api_url, json=data, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                return True, "Сообщение успешно отправлено"
            else:
                error_desc = result.get('description', 'Неизвестная ошибка')
                return False, f"Ошибка отправки: {error_desc}"
        else:
            error_data = response.json() if response.status_code == 200 else {}
            error_desc = error_data.get('description', f'HTTP {response.status_code}')
            return False, f"Ошибка отправки: {error_desc}"
            
    except requests.exceptions.Timeout:
        return False, "Таймаут при отправке сообщения"
    except requests.exceptions.ConnectionError:
        return False, "Ошибка подключения к Telegram API"
    except Exception as e:
        log_error(f"Error sending Telegram message: {e}")
        return False, f"Ошибка при отправке: {str(e)}"


def send_telegram_message_with_keyboard(message, chat_id, keyboard=None, parse_mode=None):
    """Отправляет сообщение через Telegram бота с клавиатурой (меню)
    
    Args:
        message: Текст сообщения
        chat_id: Chat ID получателя
        keyboard: InlineKeyboardMarkup или ReplyKeyboardMarkup (dict)
        parse_mode: Режим парсинга (HTML, Markdown и т.д.)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not requests:
        return False, "Библиотека requests не установлена"
    
    token = get_setting('telegram_bot_token', '')
    if not token:
        return False, "Токен бота не настроен"
    
    try:
        api_url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = {
            'chat_id': chat_id,
            'text': message
        }
        if parse_mode:
            data['parse_mode'] = parse_mode
        if keyboard:
            data['reply_markup'] = keyboard
        
        response = requests.post(api_url, json=data, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                return True, "Сообщение успешно отправлено"
            else:
                error_desc = result.get('description', 'Неизвестная ошибка')
                return False, f"Ошибка отправки: {error_desc}"
        else:
            error_data = response.json() if response.status_code == 200 else {}
            error_desc = error_data.get('description', f'HTTP {response.status_code}')
            return False, f"Ошибка отправки: {error_desc}"
            
    except Exception as e:
        log_error(f"Error sending Telegram message with keyboard: {e}")
        return False, f"Ошибка при отправке: {str(e)}"


def generate_telegram_verification_code(user_id):
    """Генерирует код верификации для пользователя"""
    conn = get_db_connection()
    try:
        # Генерируем 6-значный код
        code = ''.join([str(random.randint(0, 9)) for _ in range(6)])
        expires_at = datetime.utcnow() + timedelta(minutes=10)  # Код действителен 10 минут
        
        # Сохраняем или обновляем код
        conn.execute('''
            INSERT INTO telegram_users (user_id, verification_code, verification_code_expires_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                verification_code = excluded.verification_code,
                verification_code_expires_at = excluded.verification_code_expires_at,
                verified = 0
        ''', (user_id, code, expires_at))
        conn.commit()
        return code
    except Exception as e:
        log_error(f"Error generating verification code: {e}")
        return None
    finally:
        conn.close()


def verify_telegram_code(user_id, code, telegram_chat_id, telegram_username=None):
    """Проверяет код верификации и связывает пользователя с Telegram"""
    conn = get_db_connection()
    try:
        telegram_user = conn.execute('''
            SELECT verification_code, verification_code_expires_at
            FROM telegram_users
            WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if not telegram_user:
            return False, "Код верификации не найден. Запросите новый код."
        
        stored_code = telegram_user['verification_code']
        expires_at_str = telegram_user['verification_code_expires_at']
        
        if not stored_code or stored_code != code:
            return False, "Неверный код верификации."
        
        # Проверяем срок действия
        if expires_at_str:
            expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
            if expires_at.tzinfo:
                expires_at = expires_at.replace(tzinfo=None)
            if datetime.utcnow() > expires_at:
                return False, "Код верификации истёк. Запросите новый код."
        
        # Связываем пользователя с Telegram
        conn.execute('''
            UPDATE telegram_users
            SET telegram_chat_id = ?,
                telegram_username = ?,
                verified = 1,
                verified_at = CURRENT_TIMESTAMP,
                verification_code = NULL,
                verification_code_expires_at = NULL
            WHERE user_id = ?
        ''', (telegram_chat_id, telegram_username, user_id))
        conn.commit()
        
        return True, "Telegram успешно привязан к вашему аккаунту!"
    except Exception as e:
        log_error(f"Error verifying Telegram code: {e}")
        return False, f"Ошибка при верификации: {str(e)}"
    finally:
        conn.close()


def get_telegram_bot_menu():
    """Получает активные пункты меню бота"""
    conn = get_db_connection()
    try:
        menu_items = conn.execute('''
            SELECT button_text, button_type, action
            FROM telegram_bot_menu
            WHERE is_active = 1
            ORDER BY sort_order ASC
        ''').fetchall()
        conn.close()
        return menu_items
    except Exception as e:
        log_error(f"Error getting bot menu: {e}")
        if conn:
            conn.close()
        return []


def set_telegram_bot_commands(token):
    """Устанавливает команды меню бота через setMyCommands API"""
    if not requests or not token:
        return False
    
    try:
        menu_items = get_telegram_bot_menu()
        if not menu_items:
            # Если меню не настроено, устанавливаем базовые команды
            commands = [
                {'command': 'start', 'description': 'Начать работу с ботом'},
                {'command': 'menu', 'description': 'Показать главное меню'},
                {'command': 'verify', 'description': 'Привязать аккаунт'},
            ]
        else:
            # Формируем команды из меню
            commands = []
            for item in menu_items:
                if item['button_type'] == 'command':
                    action = item['action']
                    button_text = item['button_text']
                    # Создаем команду только для основных действий
                    if action in ['events', 'assignments', 'faq', 'rules']:
                        commands.append({
                            'command': action,
                            'description': button_text[:32]  # Максимум 32 символа
                        })
            
            # Всегда добавляем базовые команды
            base_commands = [
                {'command': 'start', 'description': 'Начать работу с ботом'},
                {'command': 'menu', 'description': 'Показать главное меню'},
            ]
            # Объединяем, избегая дубликатов
            existing_commands = {cmd['command'] for cmd in commands}
            for base_cmd in base_commands:
                if base_cmd['command'] not in existing_commands:
                    commands.append(base_cmd)
        
        # Устанавливаем команды через API
        api_url = f'https://api.telegram.org/bot{token}/setMyCommands'
        response = requests.post(api_url, json={'commands': commands}, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                log_debug(f"Bot commands set successfully: {len(commands)} commands")
                return True
            else:
                log_error(f"Failed to set bot commands: {result.get('description')}")
                return False
        else:
            log_error(f"Error setting bot commands: HTTP {response.status_code}")
            return False
    except Exception as e:
        log_error(f"Error setting Telegram bot commands: {e}")
        return False


def send_email_via_smtp(to_email, subject, body, html_body=None):
    """Отправляет email через настроенный SMTP сервер"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    # Получаем настройки SMTP
    smtp_enabled = get_setting('smtp_enabled', '0') == '1'
    if not smtp_enabled:
        return False, "SMTP не включен в настройках"
    
    smtp_verified = get_setting('smtp_verified', '0') == '1'
    if not smtp_verified:
        return False, "SMTP не проверен. Проверьте подключение в настройках"
    
    smtp_host = get_setting('smtp_host', '')
    smtp_port = get_setting('smtp_port', '587')
    smtp_username = get_setting('smtp_username', '')
    smtp_password = get_setting('smtp_password', '')
    smtp_use_tls = get_setting('smtp_use_tls', '0') == '1'
    smtp_from_email = get_setting('smtp_from_email', '')
    smtp_from_name = get_setting('smtp_from_name', 'Анонимные Деды Морозы')
    
    if not smtp_host or not smtp_username or not smtp_password or not smtp_from_email:
        return False, "SMTP настройки неполные. Проверьте настройки в админ-панели"
    
    try:
        port_int = int(smtp_port)
        
        # Создаем сообщение
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{smtp_from_name} <{smtp_from_email}>"
        msg['To'] = to_email
        
        # Добавляем текстовую и HTML версию
        if html_body:
            part1 = MIMEText(body, 'plain', 'utf-8')
            part2 = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(part1)
            msg.attach(part2)
        else:
            part = MIMEText(body, 'plain', 'utf-8')
            msg.attach(part)
        
        # Подключаемся к SMTP серверу
        if smtp_use_tls:
            server = smtplib.SMTP(smtp_host, port_int, timeout=10)
            server.starttls()
        else:
            if port_int == 465:
                server = smtplib.SMTP_SSL(smtp_host, port_int, timeout=10)
            else:
                server = smtplib.SMTP(smtp_host, port_int, timeout=10)
        
        # Авторизуемся и отправляем
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_from_email, [to_email], msg.as_string())
        server.quit()
        
        return True, "Письмо успешно отправлено"
        
    except smtplib.SMTPAuthenticationError:
        return False, "Ошибка аутентификации SMTP. Проверьте логин и пароль"
    except smtplib.SMTPException as e:
        return False, f"Ошибка SMTP: {str(e)}"
    except Exception as e:
        log_error(f"Error sending email: {e}")
        return False, f"Ошибка при отправке письма: {str(e)}"
