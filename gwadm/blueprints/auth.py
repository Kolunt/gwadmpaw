"""GWars authentication routes."""

import hashlib
import traceback
from datetime import datetime
from urllib.parse import quote, unquote, unquote_plus, unquote_to_bytes

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from gwadm.config import ADMIN_USER_IDS, GWARS_PASSWORD, is_debug
from gwadm.db import ensure_db, get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.activity import log_activity
from gwadm.services.avatars import generate_unique_avatar_seed
from gwadm.services.gwars_auth import (
    finalize_user_login,
    verify_sign,
    verify_sign2,
    verify_sign3,
    verify_sign4,
)
from gwadm.services.gwars_domains import (
    build_gwars_login_url,
    is_local_dev_host,
    load_gwars_domain_map,
)
from gwadm.services.roles import assign_role, get_user_role_names, get_user_roles, has_role

bp = Blueprint('auth', __name__)

@bp.route('/login/dev')
def login_dev():
    """Тестовый режим авторизации для локальной разработки"""
    # Проверяем, что мы на localhost
    is_local = request.host in ['127.0.0.1:5000', 'localhost:5000', '127.0.0.1', 'localhost']
    
    if not is_local:
        flash('Тестовый режим доступен только на localhost', 'error')
        return redirect(url_for('public.index'))
    
    # Используем тестовые данные для первого администратора (user_id 283494)
    user_id = ADMIN_USER_IDS[0]
    name = "_Колунт_"
    level = 50
    synd = 5594
    has_passport = 1
    has_mobile = 1
    old_passport = 0
    usersex = "0"
    
    # Генерируем правильные подписи для тестовых данных
    from urllib.parse import quote
    name_encoded = quote(name.encode('cp1251'), safe='')
    
    # Вычисляем подписи
    sign = hashlib.md5((GWARS_PASSWORD.encode('utf-8') + name.encode('cp1251') + str(user_id).encode('utf-8'))).hexdigest()
    sign2 = hashlib.md5((GWARS_PASSWORD + str(level) + str(round(float(synd))) + str(user_id)).encode('utf-8')).hexdigest()
    sign3 = hashlib.md5((GWARS_PASSWORD.encode('utf-8') + name.encode('cp1251') + str(user_id).encode('utf-8') + str(has_passport).encode('utf-8') + str(has_mobile).encode('utf-8') + str(old_passport).encode('utf-8'))).hexdigest()[:10]
    
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    sign4 = hashlib.md5((today + sign3 + GWARS_PASSWORD).encode('utf-8')).hexdigest()[:10]
    
    # Сохраняем пользователя в БД
    conn = get_db_connection()
    try:
        # Проверяем, существует ли пользователь и получаем все его данные
        existing_user = conn.execute('''
            SELECT username, level, synd, has_passport, has_mobile, old_passport, usersex,
                   avatar_seed, avatar_style, bio, contact_info, email, phone, telegram, whatsapp, viber 
            FROM users WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        # Преобразуем данные в правильные типы
        level_int = int(level) if level else 0
        synd_int = int(synd) if synd else 0
        has_passport_int = 1 if has_passport == 1 else 0
        has_mobile_int = 1 if has_mobile == 1 else 0
        old_passport_int = 1 if old_passport == 1 else 0
        
        if not existing_user:
            # Новый пользователь - создаем запись
            avatar_seed = generate_unique_avatar_seed(user_id)
            avatar_style = 'avataaars'  # Стиль по умолчанию
            conn.execute('''
                INSERT INTO users 
                (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, 
                 avatar_seed, avatar_style, last_login)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, name, level_int, synd_int, has_passport_int, has_mobile_int, 
                  old_passport_int, usersex, avatar_seed, avatar_style, datetime.now()))
            log_debug(f"New dev user created: user_id={user_id}, username={name}")
        else:
            # Существующий пользователь - проверяем, изменились ли данные
            needs_update = False
            update_fields = []
            update_values = []
            
            # Проверяем каждое поле
            if existing_user['username'] != name:
                needs_update = True
                update_fields.append('username = ?')
                update_values.append(name)
                log_debug(f"Dev username changed for user {user_id}: '{existing_user['username']}' -> '{name}'")
            
            if existing_user['level'] != level_int:
                needs_update = True
                update_fields.append('level = ?')
                update_values.append(level_int)
                log_debug(f"Dev level changed for user {user_id}: {existing_user['level']} -> {level_int}")
            
            if existing_user['synd'] != synd_int:
                needs_update = True
                update_fields.append('synd = ?')
                update_values.append(synd_int)
                log_debug(f"Dev synd changed for user {user_id}: {existing_user['synd']} -> {synd_int}")
            
            if existing_user['has_passport'] != has_passport_int:
                needs_update = True
                update_fields.append('has_passport = ?')
                update_values.append(has_passport_int)
            
            if existing_user['has_mobile'] != has_mobile_int:
                needs_update = True
                update_fields.append('has_mobile = ?')
                update_values.append(has_mobile_int)
            
            if existing_user['old_passport'] != old_passport_int:
                needs_update = True
                update_fields.append('old_passport = ?')
                update_values.append(old_passport_int)
            
            if existing_user['usersex'] != usersex:
                needs_update = True
                update_fields.append('usersex = ?')
                update_values.append(usersex)
            
            # Всегда обновляем last_login
            update_fields.append('last_login = ?')
            update_values.append(datetime.now())
            
            # Если есть изменения, обновляем только измененные поля
            if needs_update:
                update_values.append(user_id)
                update_query = f'''
                    UPDATE users 
                    SET {', '.join(update_fields)}
                    WHERE user_id = ?
                '''
                conn.execute(update_query, update_values)
                log_debug(f"Dev user data updated: user_id={user_id}, fields: {', '.join([f.split('=')[0].strip() for f in update_fields])}")
            else:
                # Если данных не изменилось, обновляем только last_login
                conn.execute('''
                    UPDATE users 
                    SET last_login = ?
                    WHERE user_id = ?
                ''', (datetime.now(), user_id))
                log_debug(f"Dev user data unchanged, only last_login updated: user_id={user_id}")
            
            # Если у пользователя нет avatar_seed, генерируем его
            if not existing_user['avatar_seed']:
                avatar_seed = generate_unique_avatar_seed(user_id)
                avatar_style = existing_user['avatar_style'] or 'avataaars'
                conn.execute('''
                    UPDATE users 
                    SET avatar_seed = ?, avatar_style = ?
                    WHERE user_id = ?
                ''', (avatar_seed, avatar_style, user_id))
                log_debug(f"Generated avatar_seed for dev user {user_id}")
        
        conn.commit()
        log_debug(f"Dev user saved successfully: user_id={user_id}, username={name}")
    except Exception as e:
        log_error(f"Error saving dev user: {e}")
        flash(f'Ошибка сохранения пользователя: {str(e)}', 'error')
        return redirect(url_for('public.index'))
    finally:
        conn.close()
    
    # Автоматически назначаем роль админа для администраторов по умолчанию
    if user_id in ADMIN_USER_IDS:
        if not has_role(user_id, 'admin'):
            assign_role(user_id, 'admin', assigned_by=user_id)
            log_debug(f"Admin role automatically assigned to user_id {user_id}")
    
    # Если у пользователя нет ролей, назначаем роль 'user' по умолчанию
    if not get_user_roles(user_id):
        assign_role(user_id, 'user', assigned_by=user_id)
        log_debug(f"Default 'user' role assigned to user_id {user_id}")
    
    # Сохраняем в сессию
    session['user_id'] = user_id
    session['username'] = name
    session['level'] = level
    session['synd'] = synd
    session['roles'] = get_user_role_names(user_id)
    # Очищаем флаг попытки авторизации через GWars (если был установлен)
    session.pop('gwars_auth_attempt', None)
    
    log_activity(
        'login',
        details='Тестовый вход через login_dev',
        metadata={'source': 'dev', 'user_id': user_id, 'username': name}
    )
    
    flash('Тестовая авторизация выполнена успешно!', 'success')
    return redirect(url_for('dashboard'))

@bp.route('/login')
def login():
    try:
        # Получаем параметры от GWars
        sign = request.args.get('sign', '')
        user_id = request.args.get('user_id', '')
        
        # ВАЖНО: Flask автоматически декодирует URL параметры, но нам нужен оригинальный закодированный вариант
        # Получаем оригинальное значение из query string напрямую
        try:
            query_string_raw = request.query_string
            query_string = query_string_raw.decode('utf-8', errors='replace')
        except:
            query_string = request.query_string.decode('utf-8') if request.query_string else ''
        
        name_encoded = None
        # Пробуем извлечь name из query string
        if query_string:
            for param in query_string.split('&'):
                if param.startswith('name='):
                    name_encoded = param.split('=', 1)[1]  # Берем все после первого =
                    break
        
        # Если не получилось получить из query_string, пробуем через request.args (но это уже декодированное)
        if not name_encoded or name_encoded == '':
            name_encoded = request.args.get('name', '')
            # Если получили через args, значит оно уже декодировано, нужно закодировать обратно для проверки
            if name_encoded:
                name_encoded_for_comparison = quote(name_encoded, safe='')
            else:
                name_encoded_for_comparison = ''
        else:
            name_encoded_for_comparison = name_encoded
        
        # Пробуем декодировать разными способами
        # ВАЖНО: GWars использует CP1251 (Windows-1251) для кодирования русских символов!
        name = name_encoded if name_encoded else ''
        name_latin1 = None
        name_cp1251 = None
        if name_encoded:
            try:
                # Сначала пробуем CP1251 (Windows-1251) - это основная кодировка для русских символов
                name_cp1251 = unquote_plus(name_encoded, encoding='cp1251')
                name = name_cp1251  # Используем CP1251 как основной вариант
            except:
                try:
                    name = unquote_plus(name_encoded, encoding='utf-8')
                except:
                    try:
                        name = unquote_plus(name_encoded, encoding='latin1')
                        name_latin1 = name
                    except:
                        name = name_encoded
                        name_latin1 = name_encoded
            
            # Если CP1251 декодирование не сработало, пробуем еще раз
            if not name_cp1251:
                try:
                    name_cp1251 = unquote_plus(name_encoded, encoding='cp1251')
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
        
        # Если нет параметров, проверяем, вернулся ли пользователь с GWars без авторизации
        if not sign or not user_id:
            # Проверяем, есть ли в сессии флаг о попытке авторизации через GWars
            gwars_auth_attempt = session.get('gwars_auth_attempt', False)
            
            # Если пользователь уже пытался авторизоваться через GWars (флаг в сессии),
            # но параметров авторизации нет, значит он не авторизован в GWars
            if gwars_auth_attempt:
                # Очищаем флаг
                session.pop('gwars_auth_attempt', None)
                # Показываем страницу с сообщением о необходимости авторизации
                return redirect(url_for('auth.gwars_required'))
            
            # Если параметров нет и пользователь еще не пытался авторизоваться,
            # устанавливаем флаг и редиректим на GWars для авторизации
            session['gwars_auth_attempt'] = True
            
            # ВАЖНО: GWars проверяет домен callback URL
            # Для локальной разработки используем production URL, чтобы GWars принял запрос
            # После авторизации пользователь будет редиректиться на production, 
            # где можно будет протестировать функционал
            
            is_local = is_local_dev_host(request.host)
            domain_map = load_gwars_domain_map()
            gwars_login_url = build_gwars_login_url(
                request.host,
                is_local=is_local,
                domain_map=domain_map,
            )
            if is_local:
                log_debug(f"Local development detected. GWars login URL: {gwars_login_url}")
            return redirect(gwars_login_url)
        
        if is_debug():
            log_debug("=== LOGIN DEBUG ===")
            log_debug("Received parameters:")
            log_debug(f"  sign={sign}")
            log_debug(f"  name (from args)={request.args.get('name', '')}")
            log_debug(f"  name (encoded/raw from query_string)={name_encoded}")
            log_debug(f"  name (decoded)={name}")
            log_debug(f"  name (repr)={repr(name)}")
            log_debug(f"  name_encoded (repr)={repr(name_encoded)}")
            log_debug(f"  user_id={user_id}")
            log_debug(f"  level={level}")
            log_debug(f"  synd={synd}")
            log_debug(f"  sign2={sign2}")
            log_debug(f"Full URL: {request.url}")
            log_debug(f"Query string (raw bytes): {request.query_string}")
            log_debug(f"Query string (decoded): {query_string}")
            log_debug(f"All args: {dict(request.args)}")
        
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
            return redirect(url_for('public.index'))
        
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
            # Логируем детали для отладки
            today = datetime.now().strftime("%Y-%m-%d")
            log_error(f"sign4 verification failed: sign3={sign3}, sign4={sign4}, today={today}")
            log_error(f"sign4 verification failed: user_id={user_id}, name={name}")
            
            # Показываем более информативное сообщение
            flash('Ошибка проверки подписи sign4. Возможно, разница в часовых поясах. Попробуйте войти еще раз.', 'error')
            return redirect(url_for('public.index'))
        
        # Сохраняем пользователя в БД
        conn = get_db_connection()
        try:
            # Проверяем, существует ли пользователь и получаем все его данные
            existing_user = conn.execute('''
                SELECT username, level, synd, has_passport, has_mobile, old_passport, usersex,
                       avatar_seed, avatar_style, bio, contact_info, email, phone, telegram, whatsapp, viber 
                FROM users WHERE user_id = ?
            ''', (user_id,)).fetchone()
            
            # Преобразуем данные из GWars в правильные типы
            level_int = int(level) if level else 0
            synd_int = int(synd) if synd else 0
            has_passport_int = 1 if has_passport == '1' else 0
            has_mobile_int = 1 if has_mobile == '1' else 0
            old_passport_int = 1 if old_passport == '1' else 0
            
            if not existing_user:
                # Новый пользователь - создаем запись
                avatar_seed = generate_unique_avatar_seed(user_id)
                avatar_style = 'avataaars'  # Стиль по умолчанию
                # Явно устанавливаем все поля контактов в NULL для нового пользователя
                conn.execute('''
                    INSERT INTO users 
                    (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, 
                     avatar_seed, avatar_style, last_login,
                     email, phone, telegram, whatsapp, viber,
                     last_name, first_name, middle_name,
                     postal_code, country, city, street, house, building, apartment,
                     bio, contact_info)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL)
                ''', (user_id, name, level_int, synd_int, has_passport_int, has_mobile_int, 
                      old_passport_int, usersex, avatar_seed, avatar_style, datetime.now()))
                log_debug(f"New user created: user_id={user_id}, username={name}")
            else:
                # Существующий пользователь - проверяем, изменились ли данные из GWars
                needs_update = False
                update_fields = []
                update_values = []
                
                # Проверяем каждое поле из GWars
                if existing_user['username'] != name:
                    needs_update = True
                    update_fields.append('username = ?')
                    update_values.append(name)
                    log_debug(f"Username changed for user {user_id}: '{existing_user['username']}' -> '{name}'")
                
                if existing_user['level'] != level_int:
                    needs_update = True
                    update_fields.append('level = ?')
                    update_values.append(level_int)
                    log_debug(f"Level changed for user {user_id}: {existing_user['level']} -> {level_int}")
                
                if existing_user['synd'] != synd_int:
                    needs_update = True
                    update_fields.append('synd = ?')
                    update_values.append(synd_int)
                    log_debug(f"Synd changed for user {user_id}: {existing_user['synd']} -> {synd_int}")
                
                if existing_user['has_passport'] != has_passport_int:
                    needs_update = True
                    update_fields.append('has_passport = ?')
                    update_values.append(has_passport_int)
                
                if existing_user['has_mobile'] != has_mobile_int:
                    needs_update = True
                    update_fields.append('has_mobile = ?')
                    update_values.append(has_mobile_int)
                
                if existing_user['old_passport'] != old_passport_int:
                    needs_update = True
                    update_fields.append('old_passport = ?')
                    update_values.append(old_passport_int)
                
                if existing_user['usersex'] != usersex:
                    needs_update = True
                    update_fields.append('usersex = ?')
                    update_values.append(usersex)
                
                # Всегда обновляем last_login
                update_fields.append('last_login = ?')
                update_values.append(datetime.now())
                
                # Если есть изменения, обновляем только измененные поля
                if needs_update:
                    update_values.append(user_id)
                    update_query = f'''
                        UPDATE users 
                        SET {', '.join(update_fields)}
                        WHERE user_id = ?
                    '''
                    conn.execute(update_query, update_values)
                    log_debug(f"User data updated: user_id={user_id}, fields: {', '.join([f.split('=')[0].strip() for f in update_fields])}")
                else:
                    # Если данных не изменилось, обновляем только last_login
                    conn.execute('''
                        UPDATE users 
                        SET last_login = ?
                        WHERE user_id = ?
                    ''', (datetime.now(), user_id))
                    log_debug(f"User data unchanged, only last_login updated: user_id={user_id}")
                
                # Если у пользователя нет avatar_seed, генерируем его
                if not existing_user['avatar_seed']:
                    avatar_seed = generate_unique_avatar_seed(user_id)
                    avatar_style = existing_user['avatar_style'] or 'avataaars'
                    conn.execute('''
                        UPDATE users 
                        SET avatar_seed = ?, avatar_style = ?
                        WHERE user_id = ?
                    ''', (avatar_seed, avatar_style, user_id))
                    log_debug(f"Generated avatar_seed for user {user_id}")
            
            conn.commit()
            log_debug(f"User saved successfully: user_id={user_id}, username={name}")
        except Exception as e:
            log_error(f"Error saving user: {e}")
            # Если ошибка из-за отсутствия таблицы, пробуем инициализировать БД заново
            if "no such table" in str(e).lower():
                log_error("Table not found, reinitializing database...")
                ensure_db()
                # Пробуем еще раз
                try:
                    # Проверяем существующего пользователя еще раз
                    existing_user = conn.execute('SELECT avatar_seed, avatar_style, bio, contact_info FROM users WHERE user_id = ?', (user_id,)).fetchone()
                    
                    # Генерируем seed для нового пользователя или используем существующий
                    if not existing_user or not existing_user['avatar_seed']:
                        avatar_seed = generate_unique_avatar_seed(user_id)
                        avatar_style = 'avataaars'
                        bio = None
                        contact_info = None
                    else:
                        avatar_seed = existing_user['avatar_seed']
                        avatar_style = existing_user['avatar_style']
                        bio = existing_user['bio']
                        contact_info = existing_user['contact_info']
                    conn.execute('''
                        INSERT OR REPLACE INTO users 
                        (user_id, username, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, last_login)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (user_id, name, level, synd, has_passport, has_mobile, old_passport, usersex, avatar_seed, avatar_style, bio, contact_info, datetime.now()))
                    conn.commit()
                    log_debug(f"User saved successfully after reinitialization: user_id={user_id}")
                except Exception as e2:
                    log_error(f"Error saving user after reinitialization: {e2}")
                    flash(f'Ошибка сохранения пользователя: {str(e2)}', 'error')
                    conn.close()
                    return redirect(url_for('public.index'))
            else:
                flash(f'Ошибка сохранения пользователя: {str(e)}', 'error')
                conn.close()
                return redirect(url_for('public.index'))
        finally:
            conn.close()
        
        # Автоматически назначаем роль админа для администраторов по умолчанию
        if int(user_id) in ADMIN_USER_IDS:
            if not has_role(user_id, 'admin'):
                assign_role(user_id, 'admin', assigned_by=user_id)
                log_debug(f"Admin role automatically assigned to user_id {user_id}")
        
        # Если у пользователя нет ролей, назначаем роль 'user' по умолчанию
        if not get_user_roles(user_id):
            assign_role(user_id, 'user', assigned_by=user_id)
            log_debug(f"Default 'user' role assigned to user_id {user_id}")
        
        # Сохраняем в сессию
        session['user_id'] = user_id
        session['username'] = name
        session['level'] = level
        session['synd'] = synd
        session['roles'] = get_user_role_names(user_id)  # Сохраняем роли в сессию
        # Очищаем флаг попытки авторизации через GWars (если был установлен)
        session.pop('gwars_auth_attempt', None)
        
        log_activity(
            'login',
            details='Вход через GWars',
            metadata={'source': 'gwars', 'user_id': user_id, 'username': name}
        )
        
        return redirect(url_for('dashboard'))
    except Exception as e:
        log_error(f"Error in login route: {e}")
        log_error(f"Traceback: {traceback.format_exc()}")
        flash(f'Ошибка при входе: {str(e)}', 'error')
        return redirect(url_for('public.index'))

@bp.route('/logout')
def logout():
    if session.get('user_id'):
        log_activity(
            'logout',
            details='Пользователь вышел из системы',
            metadata={'username': session.get('username')}
        )
    session.clear()
    flash('Вы успешно вышли из системы', 'success')
    return redirect(url_for('public.index'))

@bp.route('/gwars-required')
def gwars_required():
    """Страница с сообщением о необходимости авторизации в GWars"""
    domain_map = load_gwars_domain_map()
    gwars_login_url = build_gwars_login_url(
        request.host,
        is_local=is_local_dev_host(request.host),
        domain_map=domain_map,
    )
    return render_template('gwars_required.html', gwars_login_url=gwars_login_url)

