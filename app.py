from flask import Flask, render_template, request, redirect, url_for, session, flash
from urllib.parse import unquote
import hashlib
import sqlite3
from datetime import datetime
import os
import logging

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Константы для GWars авторизации
GWARS_PASSWORD = "deadmoroz"
GWARS_HOST = "gwadm.pythonanywhere.com"
GWARS_SITE_ID = 4

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            username TEXT NOT NULL,
            level INTEGER,
            synd INTEGER,
            has_passport INTEGER,
            has_mobile INTEGER,
            old_passport INTEGER,
            usersex TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn

# Проверка подписи sign
def verify_sign(username, user_id, sign, encoded_name=None):
    # Формируем подпись: md5(password + username + user_id)
    # В PHP: $sign=md5($pass.$user_name.$user_user_id);
    # Важно: для подписи используется оригинальное имя (до urlencode)
    
    # Пробуем оба варианта: с декодированным и закодированным именем
    # Обычно подпись делается с оригинальным значением (до urlencode)
    expected_sign_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id)).encode('utf-8')
    ).hexdigest()
    
    # Если передан закодированный вариант, пробуем и его
    expected_sign_encoded = None
    if encoded_name:
        expected_sign_encoded = hashlib.md5(
            (GWARS_PASSWORD + encoded_name + str(user_id)).encode('utf-8')
        ).hexdigest()
    
    # Логирование для отладки
    logger.debug(f"verify_sign: username={username}, user_id={user_id}")
    logger.debug(f"verify_sign: encoded_name={encoded_name}")
    logger.debug(f"verify_sign: expected (decoded)={expected_sign_decoded}")
    if expected_sign_encoded:
        logger.debug(f"verify_sign: expected (encoded)={expected_sign_encoded}")
    logger.debug(f"verify_sign: received={sign}")
    logger.debug(f"verify_sign: match (decoded)={expected_sign_decoded == sign}")
    if expected_sign_encoded:
        logger.debug(f"verify_sign: match (encoded)={expected_sign_encoded == sign}")
    
    # Проверяем оба варианта
    if expected_sign_decoded == sign:
        return True
    if expected_sign_encoded and expected_sign_encoded == sign:
        return True
    
    return False

# Проверка подписи sign2
def verify_sign2(level, synd, user_id, sign2):
    expected_sign2 = hashlib.md5(
        (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
    ).hexdigest()
    return expected_sign2 == sign2

# Проверка подписи sign3
def verify_sign3(username, user_id, has_passport, has_mobile, old_passport, sign3):
    expected_sign3 = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id) + str(has_passport) + str(has_mobile) + str(old_passport)).encode('utf-8')
    ).hexdigest()[:10]
    return expected_sign3 == sign3

# Проверка подписи sign4 (дата)
def verify_sign4(sign3, sign4):
    today = datetime.now().strftime("%Y-%m-%d")
    expected_sign4 = hashlib.md5(
        (today + sign3 + GWARS_PASSWORD).encode('utf-8')
    ).hexdigest()[:10]
    return expected_sign4 == sign4

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return render_template('index.html')

@app.route('/login')
def login():
    # Получаем параметры от GWars
    sign = request.args.get('sign', '')
    name_encoded = request.args.get('name', '')  # Получаем имя в закодированном виде
    name = unquote(name_encoded)  # Декодируем имя из URL
    user_id = request.args.get('user_id', '')
    level = request.args.get('level', '0')
    synd = request.args.get('synd', '0')
    sign2 = request.args.get('sign2', '')
    has_passport = request.args.get('has_passport', '0')
    has_mobile = request.args.get('has_mobile', '0')
    old_passport = request.args.get('old_passport', '0')
    sign3 = request.args.get('sign3', '')
    usersex = request.args.get('usersex', '')
    sign4 = request.args.get('sign4', '')
    
    # Если нет параметров, редиректим на GWars для авторизации
    if not sign or not user_id:
        # Формируем полный URL для редиректа обратно
        # На PythonAnywhere используем https://, локально - http://
        if 'pythonanywhere.com' in request.host:
            callback_url = f"https://{request.host}/login"
        else:
            callback_url = f"{request.scheme}://{request.host}/login"
        
        gwars_url = f"https://www.gwars.io/cross-server-login.php?site_id={GWARS_SITE_ID}&url={callback_url}"
        logger.debug(f"Redirecting to GWars: {gwars_url}")
        logger.debug(f"Callback URL: {callback_url}")
        return redirect(gwars_url)
    
    # Логируем все полученные параметры для отладки
    logger.error(f"=== LOGIN DEBUG ===")
    logger.error(f"Received parameters:")
    logger.error(f"  sign={sign}")
    logger.error(f"  name (encoded)={name_encoded}")
    logger.error(f"  name (decoded)={name}")
    logger.error(f"  user_id={user_id}")
    logger.error(f"  level={level}")
    logger.error(f"  synd={synd}")
    logger.error(f"  sign2={sign2}")
    logger.error(f"Full URL: {request.url}")
    
    # Проверяем подписи (пробуем оба варианта - с декодированным и закодированным именем)
    if not verify_sign(name, user_id, sign, name_encoded):
        logger.error(f"Sign verification failed!")
        logger.error(f"Trying to compute manually:")
        logger.error(f"  Password: {GWARS_PASSWORD}")
        logger.error(f"  Username (decoded): {name}")
        logger.error(f"  Username (encoded): {name_encoded}")
        logger.error(f"  User ID: {user_id}")
        
        # Вычисляем все возможные варианты
        variant1 = hashlib.md5((GWARS_PASSWORD + name + str(user_id)).encode('utf-8')).hexdigest()
        variant2 = hashlib.md5((GWARS_PASSWORD + name_encoded + str(user_id)).encode('utf-8')).hexdigest()
        variant3 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name).encode('utf-8')).hexdigest()
        
        logger.error(f"  Variant 1 (pass+name+id): {variant1}")
        logger.error(f"  Variant 2 (pass+encoded_name+id): {variant2}")
        logger.error(f"  Variant 3 (pass+id+name): {variant3}")
        logger.error(f"  Received sign: {sign}")
        
        flash('Ошибка проверки подписи sign. Проверьте логи.', 'error')
        return redirect(url_for('debug') + '?' + request.query_string.decode())
    
    if not verify_sign2(level, synd, user_id, sign2):
        flash('Ошибка проверки подписи sign2', 'error')
        return redirect(url_for('index'))
    
    if not verify_sign3(name, user_id, has_passport, has_mobile, old_passport, sign3):
        flash('Ошибка проверки подписи sign3', 'error')
        return redirect(url_for('index'))
    
    if not verify_sign4(sign3, sign4):
        flash('Ошибка проверки подписи sign4 (устаревшая подпись)', 'error')
        return redirect(url_for('index'))
    
    # Сохраняем пользователя в БД
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, last_login)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, name, level, synd, has_passport, has_mobile, old_passport, usersex, datetime.now()))
        conn.commit()
    except Exception as e:
        flash(f'Ошибка сохранения пользователя: {str(e)}', 'error')
        return redirect(url_for('index'))
    finally:
        conn.close()
    
    # Сохраняем в сессию
    session['user_id'] = user_id
    session['username'] = name
    session['level'] = level
    session['synd'] = synd
    
    return redirect(url_for('dashboard'))

@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('index'))
    
    # Получаем данные пользователя из БД
    conn = get_db_connection()
    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (session['user_id'],)
    ).fetchone()
    conn.close()
    
    return render_template('dashboard.html', user=user)

@app.route('/logout')
def logout():
    session.clear()
    flash('Вы успешно вышли из системы', 'success')
    return redirect(url_for('index'))

@app.route('/debug')
def debug():
    """Страница для отладки - показывает все параметры от GWars"""
    if request.args:
        # Вычисляем ожидаемые подписи
        sign = request.args.get('sign', '')
        name_encoded = request.args.get('name', '')
        name = unquote(name_encoded)
        user_id = request.args.get('user_id', '')
        level = request.args.get('level', '0')
        synd = request.args.get('synd', '0')
        sign2 = request.args.get('sign2', '')
        has_passport = request.args.get('has_passport', '0')
        has_mobile = request.args.get('has_mobile', '0')
        old_passport = request.args.get('old_passport', '0')
        sign3 = request.args.get('sign3', '')
        sign4 = request.args.get('sign4', '')
        
        # Вычисляем все возможные варианты подписи
        variant1 = hashlib.md5((GWARS_PASSWORD + name + str(user_id)).encode('utf-8')).hexdigest()
        variant2 = hashlib.md5((GWARS_PASSWORD + name_encoded + str(user_id)).encode('utf-8')).hexdigest()
        variant3 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name).encode('utf-8')).hexdigest()
        
        expected_sign2 = hashlib.md5(
            (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
        ).hexdigest()
        
        debug_info = {
            'received_params': dict(request.args),
            'password': GWARS_PASSWORD,
            'encoded_name': name_encoded,
            'decoded_name': name,
            'user_id': user_id,
            'variant1': variant1,
            'variant2': variant2,
            'variant3': variant3,
            'received_sign': sign,
            'sign_match_v1': variant1 == sign,
            'sign_match_v2': variant2 == sign,
            'sign_match_v3': variant3 == sign,
            'expected_sign2': expected_sign2,
            'received_sign2': sign2,
            'sign2_match': expected_sign2 == sign2,
        }
        
        return render_template('debug.html', debug_info=debug_info)
    return render_template('debug.html', debug_info=None)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)

