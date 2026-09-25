"""External integrations."""

import secrets
from datetime import datetime

from flask import Blueprint, jsonify, request, session

from gwadm.config import CRON_SECRET_TOKEN
try:
    import requests
except ImportError:
    requests = None

from gwadm.services.telegram import (
    generate_telegram_verification_code,
    handle_telegram_callback,
    handle_telegram_message,
    verify_dadata_api,
    verify_smtp_connection,
    verify_telegram_bot,
)

from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role
from gwadm.logging_config import log_error
from gwadm.services.settings import get_setting

bp = Blueprint("integrations", __name__)

@bp.route('/telegram/verify/generate', methods=['POST'])
@require_login
def telegram_verify_generate():
    """Генерирует код верификации для пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'message': 'Необходима авторизация'}), 401
    
    code = generate_telegram_verification_code(user_id)
    if code:
        # Получаем имя бота для ссылки
        token = get_setting('telegram_bot_token', '')
        bot_username = None
        if token and requests:
            try:
                api_url = f'https://api.telegram.org/bot{token}/getMe'
                response = requests.get(api_url, timeout=5)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('ok'):
                        bot_username = result.get('result', {}).get('username')
            except:
                pass
        
        return jsonify({
            'success': True,
            'code': code,
            'user_id': user_id,
            'bot_username': bot_username,
            'message': f'Код верификации: {code}\n\nОткройте бота в Telegram и отправьте ему этот код.'
        })
    else:
        return jsonify({'success': False, 'message': 'Ошибка при генерации кода'}), 500


@bp.route('/telegram/verify/status', methods=['GET'])
@require_login
def telegram_verify_status():
    """Проверяет статус верификации Telegram"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'verified': False}), 401
    
    conn = get_db_connection()
    telegram_user = conn.execute('''
        SELECT verified, telegram_chat_id, telegram_username, verified_at
        FROM telegram_users
        WHERE user_id = ?
    ''', (user_id,)).fetchone()
    conn.close()
    
    if telegram_user:
        return jsonify({
            'success': True,
            'verified': bool(telegram_user['verified']),
            'telegram_chat_id': telegram_user['telegram_chat_id'],
            'telegram_username': telegram_user['telegram_username'],
            'verified_at': telegram_user['verified_at']
        })
    else:
        return jsonify({'success': True, 'verified': False})


@bp.route('/telegram/verify/unlink', methods=['POST'])
@require_login
def telegram_verify_unlink():
    """Отвязывает Telegram аккаунт от пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'message': 'Необходима авторизация'}), 401
    
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM telegram_users
            WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        return jsonify({'success': True, 'message': 'Telegram аккаунт успешно отвязан'})
    except Exception as e:
        log_error(f"Error unlinking Telegram: {e}")
        return jsonify({'success': False, 'message': f'Ошибка при отвязке: {str(e)}'}), 500
    finally:
        conn.close()


@bp.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """Вебхук для обработки сообщений от Telegram бота"""
    if not requests:
        return jsonify({'ok': False, 'error': 'requests library not available'}), 500
    
    # Проверяем, что бот включен
    telegram_enabled = get_setting('telegram_enabled', '0') == '1'
    telegram_verified = get_setting('telegram_verified', '0') == '1'
    if not telegram_enabled or not telegram_verified:
        return jsonify({'ok': False, 'error': 'Telegram bot not enabled or verified'}), 503
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({'ok': False}), 400
        
        message = data.get('message')
        callback_query = data.get('callback_query')
        
        if callback_query:
            # Обработка нажатий на кнопки
            try:
                return handle_telegram_callback(callback_query)
            except Exception as e:
                log_error(f"Error handling callback: {e}")
                return jsonify({'ok': True})  # Возвращаем ok, чтобы Telegram не повторял запрос
        elif message:
            # Обработка текстовых сообщений и команд
            try:
                return handle_telegram_message(message)
            except Exception as e:
                log_error(f"Error handling message: {e}")
                return jsonify({'ok': True})  # Возвращаем ok, чтобы Telegram не повторял запрос
        
        return jsonify({'ok': True})
    except Exception as e:
        log_error(f"Error processing Telegram webhook: {e}")
        import traceback
        log_error(traceback.format_exc())
        return jsonify({'ok': True})  # Возвращаем ok, чтобы Telegram не повторял запрос


@bp.route('/admin/settings/verify-dadata', methods=['POST'])
@require_role('admin')
def verify_dadata():
    """Проверка Dadata API ключей"""
    api_key = request.form.get('api_key', '').strip()
    secret_key = request.form.get('secret_key', '').strip()
    
    if not api_key or not secret_key:
        return jsonify({'success': False, 'message': 'API ключ и Secret ключ обязательны'}), 400
    
    success, message = verify_dadata_api(api_key, secret_key)
    
    if success:
        # Сохраняем ключи и помечаем как проверенные
        conn = get_db_connection()
        try:
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (api_key, 'dadata_api_key'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (secret_key, 'dadata_secret_key'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1', 'dadata_verified'))
            conn.commit()
        except Exception as e:
            log_error(f"Error saving Dadata keys: {e}")
            conn.close()
            return jsonify({'success': False, 'message': f'Ошибка сохранения ключей: {str(e)}'}), 500
        conn.close()
    
    return jsonify({'success': success, 'message': message})


@bp.route('/admin/settings/verify-smtp', methods=['POST'])
@require_role('admin')
def verify_smtp():
    """Проверка SMTP подключения"""
    host = request.form.get('host', '').strip()
    port = request.form.get('port', '').strip()
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()
    use_tls = request.form.get('use_tls', '0') == '1'
    from_email = request.form.get('from_email', '').strip()
    
    if not host or not port or not username or not password:
        return jsonify({'success': False, 'message': 'Все поля обязательны для заполнения'}), 400
    
    success, message = verify_smtp_connection(host, port, username, password, use_tls, from_email)
    
    if success:
        # Сохраняем настройки и помечаем как проверенные
        conn = get_db_connection()
        try:
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (host, 'smtp_host'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (port, 'smtp_port'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (username, 'smtp_username'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (password, 'smtp_password'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1' if use_tls else '0', 'smtp_use_tls'))
            if from_email:
                conn.execute('UPDATE settings SET value = ? WHERE key = ?', (from_email, 'smtp_from_email'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1', 'smtp_verified'))
            conn.commit()
        except Exception as e:
            log_error(f"Error saving SMTP settings: {e}")
            conn.close()
            return jsonify({'success': False, 'message': f'Ошибка сохранения настроек: {str(e)}'}), 500
        conn.close()
    
    return jsonify({'success': success, 'message': message})


@bp.route('/admin/settings/verify-telegram', methods=['POST'])
@require_role('admin')
def verify_telegram():
    """Проверка Telegram бота"""
    token = request.form.get('token', '').strip()
    chat_id = request.form.get('chat_id', '').strip() or None
    
    if not token:
        return jsonify({'success': False, 'message': 'Токен бота обязателен'}), 400
    
    success, message = verify_telegram_bot(token, chat_id)
    
    if success:
        # Сохраняем настройки и помечаем как проверенные
        conn = get_db_connection()
        try:
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', (token, 'telegram_bot_token'))
            if chat_id:
                conn.execute('UPDATE settings SET value = ? WHERE key = ?', (chat_id, 'telegram_chat_id'))
            conn.execute('UPDATE settings SET value = ? WHERE key = ?', ('1', 'telegram_verified'))
            conn.commit()
        except Exception as e:
            log_error(f"Error saving Telegram settings: {e}")
            conn.close()
            return jsonify({'success': False, 'message': f'Ошибка сохранения настроек: {str(e)}'}), 500
        conn.close()
    
    return jsonify({'success': success, 'message': message})


@bp.route('/cron/run', methods=['GET', 'POST'])
def cron_run():
    """HTTP endpoint для запуска cron задач из внешнего сервиса
    
    Защищен секретным токеном, который можно настроить через переменную окружения
    CRON_SECRET_TOKEN или через настройку в БД.
    
    Использование:
    https://gwadm.pythonanywhere.com/cron/run?token=YOUR_SECRET_TOKEN
    """
    # Получаем секретный токен из переменной окружения или настроек
    expected_token = CRON_SECRET_TOKEN or get_setting('cron_secret_token', '')
    
    # Если токен не настроен, генерируем случайный при первом запуске
    if not expected_token:
        # Генерируем случайный токен и сохраняем в настройках
        import secrets
        expected_token = secrets.token_urlsafe(32)
        conn = get_db_connection()
        try:
            # Проверяем, существует ли настройка
            existing = conn.execute('SELECT key FROM settings WHERE key = ?', ('cron_secret_token',)).fetchone()
            if not existing:
                conn.execute('''
                    INSERT INTO settings (key, value, description, category)
                    VALUES (?, ?, ?, ?)
                ''', ('cron_secret_token', expected_token, 'Секретный токен для запуска cron задач', 'system'))
                conn.commit()
            else:
                # Получаем существующий токен
                setting = conn.execute('SELECT value FROM settings WHERE key = ?', ('cron_secret_token',)).fetchone()
                if setting:
                    expected_token = setting['value']
        except Exception as e:
            log_error(f"Error getting/setting cron token: {e}")
        finally:
            conn.close()
    
    # Проверяем токен из запроса
    provided_token = request.args.get('token') or request.form.get('token')
    
    if not provided_token or provided_token != expected_token:
        return jsonify({
            'success': False,
            'error': 'Invalid or missing token'
        }), 401
    
    # Запускаем задачи
    try:
        from cron_tasks import cleanup_expired_verification_codes, cleanup_old_activity_logs, backup_database
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'tasks': {}
        }
        
        # Очистка истекших кодов верификации
        cleaned_codes = cleanup_expired_verification_codes()
        results['tasks']['cleanup_expired_verification_codes'] = {
            'success': True,
            'cleaned_count': cleaned_codes
        }
        
        # Опциональные задачи (можно включить через параметры)
        if request.args.get('cleanup_logs') == '1' or request.form.get('cleanup_logs') == '1':
            days = int(request.args.get('logs_days', request.form.get('logs_days', 90)))
            cleaned_logs = cleanup_old_activity_logs(days=days)
            results['tasks']['cleanup_old_activity_logs'] = {
                'success': True,
                'cleaned_count': cleaned_logs,
                'days': days
            }
        
        if request.args.get('backup') == '1' or request.form.get('backup') == '1':
            backup_success = backup_database()
            results['tasks']['backup_database'] = {
                'success': backup_success
            }
        
        results['success'] = True
        return jsonify(results), 200
        
    except Exception as e:
        log_error(f"Error running cron tasks: {e}")
        import traceback
        log_error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
