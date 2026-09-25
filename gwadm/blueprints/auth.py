"""GWars authentication routes."""

import traceback
from datetime import datetime

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from gwadm.config import ADMIN_USER_IDS, is_debug
from gwadm.db import ensure_db, get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.activity import log_activity
from gwadm.services.avatars import generate_unique_avatar_seed
from gwadm.services.gwars_auth import finalize_user_login
from gwadm.services.gwars_signatures import (
    build_sign3_debug_info,
    build_sign_debug_info,
    decode_gwars_name,
    extract_name_encoded_from_request,
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
    return redirect(url_for('profile.dashboard'))

@bp.route('/login')
def login():
    try:
        # Получаем параметры от GWars
        sign = request.args.get('sign', '')
        user_id = request.args.get('user_id', '')
        
        name_encoded = extract_name_encoded_from_request(request)
        name_info = decode_gwars_name(name_encoded, request.args.get('name', ''))
        name = name_info['name']
        name_cp1251 = name_info['name_cp1251']
        name_latin1 = name_info['name_latin1']
        if not name_encoded and name_info['name_encoded']:
            name_encoded = name_info['name_encoded']

        level = request.args.get('level', '0')
        synd = request.args.get('synd', '0')
        sign2 = request.args.get('sign2', '')
        has_passport = request.args.get('has_passport', '0')
        has_mobile = request.args.get('has_mobile', '0')
        old_passport = request.args.get('old_passport', '0')
        sign3 = request.args.get('sign3', '')
        usersex = request.args.get('usersex', '')
        sign4 = request.args.get('sign4', '')

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
            log_debug(f"All args: {dict(request.args)}")
        
        # Проверяем подписи (пробуем оба варианта - с декодированным и закодированным именем)
        if not verify_sign(name, user_id, sign, name_encoded):
            flash('Ошибка проверки подписи sign. Смотрите информацию ниже.', 'error')
            debug_info = build_sign_debug_info(
                request, name, name_encoded, name_cp1251, name_latin1,
                user_id, sign, sign2, level, synd,
            )
            return render_template('debug.html', debug_info=debug_info)
        
        if not verify_sign2(level, synd, user_id, sign2):
            flash('Ошибка проверки подписи sign2', 'error')
            return redirect(url_for('public.index'))
        
        if not verify_sign3(name, user_id, has_passport, has_mobile, old_passport, sign3, name_encoded):
            flash('Ошибка проверки подписи sign3. Смотрите информацию ниже.', 'error')
            debug_info = build_sign3_debug_info(
                request, name, name_encoded, user_id,
                has_passport, has_mobile, old_passport, sign3, sign4,
            )
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
        
        return redirect(url_for('profile.dashboard'))
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

