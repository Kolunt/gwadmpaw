from flask import Flask, render_template, request, redirect, url_for, session, flash
from urllib.parse import unquote, unquote_to_bytes
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

# Для PythonAnywhere также используем print (видно в error log)
def log_error(msg):
    """Логирует ошибку через logger и print для PythonAnywhere"""
    logger.error(msg)
    print(msg, flush=True)  # flush=True для немедленного вывода

def log_debug(msg):
    """Логирует отладочную информацию через logger и print"""
    logger.debug(msg)
    print(msg, flush=True)

# Константы для GWars авторизации
GWARS_PASSWORD = "deadmoroz"
GWARS_HOST = "gwadm.pythonanywhere.com"
GWARS_SITE_ID = 4

# Инициализация базы данных
_db_initialized = False
_db_path = None

def get_db_path():
    """Определяет путь к базе данных"""
    global _db_path
    if _db_path is None:
        # На PythonAnywhere используем абсолютный путь в домашней директории
        if os.path.exists('/home/gwadm'):
            # Мы на PythonAnywhere
            _db_path = '/home/gwadm/gwadm/database.db'
        else:
            # Локально используем относительный путь
            _db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database.db')
    return _db_path

def init_db():
    """Инициализирует базу данных, создавая таблицы если их нет"""
    global _db_initialized
    try:
        db_path = get_db_path()
        log_debug(f"Initializing database at: {db_path}")
        
        # Создаем директорию если её нет
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
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
        _db_initialized = True
        log_debug(f"Database initialized successfully at: {db_path}")
    except Exception as e:
        log_error(f"Error initializing database: {e}")
        raise

def ensure_db():
    """Убеждается, что база данных инициализирована"""
    if not _db_initialized:
        init_db()

def get_db_connection():
    """Получает соединение с базой данных"""
    ensure_db()  # Убеждаемся, что БД инициализирована
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Проверка подписи sign
def verify_sign(username, user_id, sign, encoded_name=None):
    # Формируем подпись: md5(password + username + user_id)
    # В PHP: $sign=md5($pass.$user_name.$user_user_id);
    # ВАЖНО: В PHP подпись вычисляется с оригинальными байтами ДО urlencode!
    # Поэтому нужно использовать unquote_to_bytes для получения оригинальных байтов
    
    variants = []
    
    # Вариант 1: ОРИГИНАЛЬНЫЕ БАЙТЫ из URL (правильный способ!)
    # В PHP подпись вычисляется с оригинальными байтами, а не с декодированной строкой
    if encoded_name:
        try:
            name_bytes = unquote_to_bytes(encoded_name)
            expected_sign_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8')
            ).hexdigest()
            variants.append(('bytes', expected_sign_bytes))
        except:
            pass
    
    # Вариант 2: декодированное имя через UTF-8
    expected_sign_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id)).encode('utf-8')
    ).hexdigest()
    variants.append(('decoded', expected_sign_decoded))
    
    # Вариант 3: закодированное имя (как пришло в URL)
    if encoded_name:
        expected_sign_encoded = hashlib.md5(
            (GWARS_PASSWORD + encoded_name + str(user_id)).encode('utf-8')
        ).hexdigest()
        variants.append(('encoded', expected_sign_encoded))
    
    # Вариант 4: декодированное через CP1251 (Windows-1251)
    if encoded_name:
        try:
            name_cp1251 = unquote(encoded_name, encoding='cp1251')
            expected_sign_cp1251 = hashlib.md5(
                (GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8')
            ).hexdigest()
            variants.append(('cp1251', expected_sign_cp1251))
        except:
            pass
        
        # Вариант 5: декодированное через latin1, затем байты
        try:
            name_latin1 = unquote(encoded_name, encoding='latin1')
            name_latin1_bytes = name_latin1.encode('latin1')
            expected_sign_latin1_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_latin1_bytes + str(user_id).encode('utf-8')
            ).hexdigest()
            variants.append(('latin1_bytes', expected_sign_latin1_bytes))
        except:
            pass
    
    # Логирование для отладки
    log_error(f"verify_sign: username={username}, user_id={user_id}")
    log_error(f"verify_sign: encoded_name={encoded_name}")
    for variant_name, variant_sign in variants:
        match_status = "MATCH" if variant_sign == sign else "NO MATCH"
        log_error(f"verify_sign: variant {variant_name}={variant_sign}, {match_status}")
    
    # Проверяем все варианты
    for variant_name, variant_sign in variants:
        if variant_sign == sign:
            log_error(f"verify_sign: SUCCESS with variant {variant_name}!")
            return True
    
    log_error(f"verify_sign: ALL VARIANTS FAILED! Received sign={sign}")
    return False

# Проверка подписи sign2
def verify_sign2(level, synd, user_id, sign2):
    expected_sign2 = hashlib.md5(
        (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
    ).hexdigest()
    return expected_sign2 == sign2

# Проверка подписи sign3
def verify_sign3(username, user_id, has_passport, has_mobile, old_passport, sign3, encoded_name=None):
    # В PHP: $sign3=substr(md5($pass.$user_name.$user_id.$has_passport.$has_mobile.$old_passport),0,10);
    # ВАЖНО: Используем оригинальные байты, как и для sign!
    variants = []
    
    # Вариант 1: ОРИГИНАЛЬНЫЕ БАЙТЫ из URL (правильный способ!)
    if encoded_name:
        try:
            name_bytes = unquote_to_bytes(encoded_name)
            expected_sign3_bytes = hashlib.md5(
                GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8') + 
                str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8')
            ).hexdigest()[:10]
            variants.append(('bytes', expected_sign3_bytes))
        except:
            pass
    
    # Вариант 2: декодированное имя
    expected_sign3_decoded = hashlib.md5(
        (GWARS_PASSWORD + username + str(user_id) + str(has_passport) + str(has_mobile) + str(old_passport)).encode('utf-8')
    ).hexdigest()[:10]
    variants.append(('decoded', expected_sign3_decoded))
    
    # Проверяем все варианты
    for variant_name, variant_sign in variants:
        if variant_sign == sign3:
            log_error(f"verify_sign3: SUCCESS with variant {variant_name}!")
            return True
    
    log_error(f"verify_sign3: ALL VARIANTS FAILED! Received sign3={sign3}")
    return False

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
    user_id = request.args.get('user_id', '')
    
    # ВАЖНО: Flask автоматически декодирует URL параметры, но нам нужен оригинальный закодированный вариант
    # Получаем оригинальное значение из query string напрямую
    try:
        query_string_raw = request.query_string
        query_string = query_string_raw.decode('utf-8', errors='replace')
    except:
        query_string = request.query_string.decode('utf-8')
    
    name_encoded = None
    # Пробуем извлечь name из query string
    for param in query_string.split('&'):
        if param.startswith('name='):
            name_encoded = param.split('=', 1)[1]  # Берем все после первого =
            break
    
    # Если не получилось получить из query_string, пробуем через request.args (но это уже декодированное)
    if not name_encoded or name_encoded == '':
        name_encoded = request.args.get('name', '')
        # Если получили через args, значит оно уже декодировано, нужно закодировать обратно для проверки
        if name_encoded:
            from urllib.parse import quote
            name_encoded_for_comparison = quote(name_encoded, safe='')
        else:
            name_encoded_for_comparison = ''
    else:
        name_encoded_for_comparison = name_encoded
    
    # Пробуем декодировать разными способами
    # ВАЖНО: GWars использует CP1251 (Windows-1251) для кодирования русских символов!
    name = name_encoded
    name_latin1 = None
    name_cp1251 = None
    try:
        # Сначала пробуем CP1251 (Windows-1251) - это основная кодировка для русских символов
        name_cp1251 = unquote(name_encoded, encoding='cp1251')
        name = name_cp1251  # Используем CP1251 как основной вариант
    except:
        try:
            name = unquote(name_encoded, encoding='utf-8')
        except:
            try:
                name = unquote(name_encoded, encoding='latin1')
                name_latin1 = name
            except:
                name = name_encoded
                name_latin1 = name_encoded
    
    # Если CP1251 декодирование не сработало, пробуем еще раз
    if not name_cp1251:
        try:
            name_cp1251 = unquote(name_encoded, encoding='cp1251')
        except:
            name_cp1251 = None
    
    level = request.args.get('level', '0')
    synd = request.args.get('synd', '0')
    sign2 = request.args.get('sign2', '')
    has_passport = request.args.get('has_passport', '0')
    has_mobile = request.args.get('has_mobile', '0')
    old_passport = request.args.get('old_passport', '0')
    sign3 = request.args.get('sign3', '')
    usersex = request.args.get('usersex', '')
    sign4 = request.args.get('sign4', '')
    
    # Если name пустое, пробуем получить из request.args напрямую
    if not name or name == '':
        name = request.args.get('name', '')
        if name:
            name_encoded = name  # Если получили через args, значит оно уже декодировано
    
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
    log_error("=== LOGIN DEBUG ===")
    log_error(f"Received parameters:")
    log_error(f"  sign={sign}")
    log_error(f"  name (from args)={request.args.get('name', '')}")
    log_error(f"  name (encoded/raw from query_string)={name_encoded}")
    log_error(f"  name (decoded)={name}")
    log_error(f"  name (repr)={repr(name)}")
    log_error(f"  name_encoded (repr)={repr(name_encoded)}")
    log_error(f"  user_id={user_id}")
    log_error(f"  level={level}")
    log_error(f"  synd={synd}")
    log_error(f"  sign2={sign2}")
    log_error(f"Full URL: {request.url}")
    log_error(f"Query string (raw bytes): {request.query_string}")
    log_error(f"Query string (decoded): {query_string}")
    log_error(f"All args: {dict(request.args)}")
    
    # Проверяем подписи (пробуем оба варианта - с декодированным и закодированным именем)
    if not verify_sign(name, user_id, sign, name_encoded):
        # Вместо редиректа, сразу показываем страницу отладки
        # Это позволит увидеть информацию даже если логи не работают
        flash('Ошибка проверки подписи sign. Смотрите информацию ниже.', 'error')
        
        # Вычисляем все варианты для отображения
        # ВАЖНО: Правильный способ - использовать оригинальные байты из URL!
        variant_bytes = None
        if name_encoded:
            try:
                name_bytes = unquote_to_bytes(name_encoded)
                variant_bytes = hashlib.md5(
                    GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8')
                ).hexdigest()
            except:
                pass
        
        variant1 = hashlib.md5((GWARS_PASSWORD + name + str(user_id)).encode('utf-8')).hexdigest()
        variant2 = hashlib.md5((GWARS_PASSWORD + name_encoded + str(user_id)).encode('utf-8')).hexdigest()
        variant3 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name).encode('utf-8')).hexdigest()
        variant4 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name_encoded).encode('utf-8')).hexdigest()
        
        # Пробуем CP1251
        try:
            if not name_cp1251:
                name_cp1251 = unquote(name_encoded, encoding='cp1251') if name_encoded else None
            if name_cp1251:
                variant5 = hashlib.md5((GWARS_PASSWORD + name_cp1251 + str(user_id)).encode('utf-8')).hexdigest()
            else:
                variant5 = None
        except:
            name_cp1251 = None
            variant5 = None
        
        # Пробуем latin1 с байтами (правильный способ!)
        variant_latin1_bytes = None
        try:
            if not name_latin1:
                name_latin1 = unquote(name_encoded, encoding='latin1') if name_encoded else None
            if name_latin1:
                name_latin1_bytes = name_latin1.encode('latin1')
                variant_latin1_bytes = hashlib.md5(
                    GWARS_PASSWORD.encode('utf-8') + name_latin1_bytes + str(user_id).encode('utf-8')
                ).hexdigest()
        except:
            name_latin1 = None
            variant_latin1_bytes = None
        
        # Пробуем с именем как оно пришло через request.args (уже декодированное)
        name_from_args = request.args.get('name', '')
        variant7 = None
        if name_from_args and name_from_args != name:
            variant7 = hashlib.md5((GWARS_PASSWORD + name_from_args + str(user_id)).encode('utf-8')).hexdigest()
        
        # Пробуем с пустым именем (если имя пустое)
        variant8 = None
        variant9 = None
        if not name or name == '':
            variant8 = hashlib.md5((GWARS_PASSWORD + '' + str(user_id)).encode('utf-8')).hexdigest()
            variant9 = hashlib.md5((GWARS_PASSWORD + str(user_id) + '').encode('utf-8')).hexdigest()
        
        expected_sign2 = hashlib.md5(
            (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
        ).hexdigest()
        
        debug_info = {
            'received_params': dict(request.args),
            'password': GWARS_PASSWORD,
            'encoded_name': name_encoded if name_encoded else 'EMPTY',
            'decoded_name': name if name else 'EMPTY',
            'decoded_name_cp1251': name_cp1251 if name_cp1251 else 'N/A',
            'decoded_name_latin1': name_latin1 if name_latin1 else 'N/A',
            'name_from_args': name_from_args if name_from_args else 'EMPTY',
            'user_id': user_id,
            'query_string': query_string,
            'full_url': request.url,
            'variant_bytes': variant_bytes if variant_bytes else 'N/A',
            'variant1': variant1,
            'variant2': variant2,
            'variant3': variant3,
            'variant4': variant4,
            'variant5': variant5 if variant5 else 'N/A',
            'variant_latin1_bytes': variant_latin1_bytes if variant_latin1_bytes else 'N/A',
            'received_sign': sign,
            'sign_match_bytes': variant_bytes == sign if variant_bytes else False,
            'sign_match_v1': variant1 == sign,
            'sign_match_v2': variant2 == sign,
            'sign_match_v3': variant3 == sign,
            'sign_match_v4': variant4 == sign,
            'sign_match_v5': variant5 == sign if variant5 else False,
            'sign_match_latin1_bytes': variant_latin1_bytes == sign if variant_latin1_bytes else False,
            'expected_sign2': expected_sign2,
            'received_sign2': sign2,
            'sign2_match': expected_sign2 == sign2,
        }
        
        return render_template('debug.html', debug_info=debug_info)
    
    if not verify_sign2(level, synd, user_id, sign2):
        flash('Ошибка проверки подписи sign2', 'error')
        return redirect(url_for('index'))
    
    if not verify_sign3(name, user_id, has_passport, has_mobile, old_passport, sign3, name_encoded):
        # Показываем страницу отладки для sign3
        flash('Ошибка проверки подписи sign3. Смотрите информацию ниже.', 'error')
        
        # Вычисляем варианты sign3 для отладки
        sign3_variant_bytes = None
        if name_encoded:
            try:
                name_bytes = unquote_to_bytes(name_encoded)
                sign3_variant_bytes = hashlib.md5(
                    GWARS_PASSWORD.encode('utf-8') + name_bytes + str(user_id).encode('utf-8') + 
                    str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8')
                ).hexdigest()[:10]
            except:
                pass
        
        sign3_variant_decoded = hashlib.md5(
            (GWARS_PASSWORD + name + str(user_id) + str(has_passport) + str(has_mobile) + str(old_passport)).encode('utf-8')
        ).hexdigest()[:10]
        
        # Вычисляем sign4 варианты
        today = datetime.now().strftime("%Y-%m-%d")
        sign4_variant1 = hashlib.md5((today + sign3 + GWARS_PASSWORD).encode('utf-8')).hexdigest()[:10]
        
        debug_info = {
            'received_params': dict(request.args),
            'password': GWARS_PASSWORD,
            'encoded_name': name_encoded if name_encoded else 'EMPTY',
            'decoded_name': name if name else 'EMPTY',
            'user_id': user_id,
            'has_passport': has_passport,
            'has_mobile': has_mobile,
            'old_passport': old_passport,
            'sign3_received': sign3,
            'sign3_variant_bytes': sign3_variant_bytes if sign3_variant_bytes else 'N/A',
            'sign3_variant_decoded': sign3_variant_decoded,
            'sign3_match_bytes': sign3_variant_bytes == sign3 if sign3_variant_bytes else False,
            'sign3_match_decoded': sign3_variant_decoded == sign3,
            'sign4_received': sign4,
            'sign4_variant1': sign4_variant1,
            'sign4_match': sign4_variant1 == sign4,
        }
        
        return render_template('debug_sign3.html', debug_info=debug_info)
    
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
        log_debug(f"User saved successfully: user_id={user_id}, username={name}")
    except Exception as e:
        log_error(f"Error saving user: {e}")
        # Если ошибка из-за отсутствия таблицы, пробуем инициализировать БД заново
        if "no such table" in str(e).lower():
            log_error("Table not found, reinitializing database...")
            init_db()
            # Пробуем еще раз
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO users 
                    (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, last_login)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, name, level, synd, has_passport, has_mobile, old_passport, usersex, datetime.now()))
                conn.commit()
                log_debug(f"User saved successfully after reinitialization: user_id={user_id}")
            except Exception as e2:
                log_error(f"Error saving user after reinitialization: {e2}")
                flash(f'Ошибка сохранения пользователя: {str(e2)}', 'error')
                return redirect(url_for('index'))
        else:
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
        # Получаем параметры
        sign = request.args.get('sign', '')
        user_id = request.args.get('user_id', '')
        
        # Получаем оригинальное закодированное имя из query_string
        query_string = request.query_string.decode('utf-8')
        name_encoded = None
        for param in query_string.split('&'):
            if param.startswith('name='):
                name_encoded = param.split('=', 1)[1]
                break
        
        if not name_encoded:
            name_encoded = request.args.get('name', '')
        
        # Пробуем декодировать разными способами
        try:
            name = unquote(name_encoded, encoding='utf-8')
        except:
            try:
                name = unquote(name_encoded, encoding='cp1251')
            except:
                try:
                    name = unquote(name_encoded, encoding='latin1')
                except:
                    name = name_encoded
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
        variant4 = hashlib.md5((GWARS_PASSWORD + str(user_id) + name_encoded).encode('utf-8')).hexdigest()
        
        # Пробуем latin1 декодирование
        try:
            name_latin1 = unquote(name_encoded, encoding='latin1')
            variant5 = hashlib.md5((GWARS_PASSWORD + name_latin1 + str(user_id)).encode('utf-8')).hexdigest()
        except:
            name_latin1 = None
            variant5 = None
        
        expected_sign2 = hashlib.md5(
            (GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')
        ).hexdigest()
        
        debug_info = {
            'received_params': dict(request.args),
            'password': GWARS_PASSWORD,
            'encoded_name': name_encoded,
            'decoded_name': name,
            'decoded_name_latin1': name_latin1 if name_latin1 else 'N/A',
            'user_id': user_id,
            'variant1': variant1,
            'variant2': variant2,
            'variant3': variant3,
            'variant4': variant4,
            'variant5': variant5 if variant5 else 'N/A',
            'received_sign': sign,
            'sign_match_v1': variant1 == sign,
            'sign_match_v2': variant2 == sign,
            'sign_match_v3': variant3 == sign,
            'sign_match_v4': variant4 == sign,
            'sign_match_v5': variant5 == sign if variant5 else False,
            'expected_sign2': expected_sign2,
            'received_sign2': sign2,
            'sign2_match': expected_sign2 == sign2,
        }
        
        return render_template('debug.html', debug_info=debug_info)
    return render_template('debug.html', debug_info=None)

# Инициализируем БД при импорте модуля (для WSGI)
try:
    init_db()
except Exception as e:
    log_error(f"Failed to initialize database on startup: {e}")

if __name__ == '__main__':
    app.run(debug=True)

