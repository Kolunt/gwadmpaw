"""Admin: rules."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/rules/init-defaults', methods=['POST'])
@require_role('admin')
def admin_rules_init_defaults():
    """Принудительная инициализация дефолтных правил"""
    try:
        import json
        conn = get_db_connection()
        user_id = session.get('user_id')
        
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
        
        # Удаляем существующую запись, если есть
        conn.execute('DELETE FROM settings WHERE key = ?', ('rules_content',))
        
        # Проверяем структуру таблицы settings
        table_info = conn.execute("PRAGMA table_info(settings)").fetchall()
        columns = [col[1] for col in table_info]
        
        # Формируем запрос в зависимости от наличия колонок
        if 'created_at' in columns and 'created_by' in columns and 'updated_at' in columns and 'updated_by' in columns:
            # Полная структура с датами и пользователями
            conn.execute('''
                INSERT INTO settings (key, value, category, created_at, created_by, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', ('rules_content', rules_json, 'general', datetime.now(), user_id, datetime.now(), user_id))
        elif 'updated_at' in columns:
            # Структура с updated_at
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
        conn.close()
        
        flash('Дефолтные правила успешно инициализированы', 'success')
        log_debug("Default rules initialized via admin panel")
    except Exception as e:
        log_error(f"Error initializing default rules: {e}")
        log_error(traceback.format_exc())
        flash(f'Ошибка инициализации правил: {str(e)}', 'error')
    
    return redirect(url_for('admin.admin_rules'))


@bp.route('/rules')
@require_role('admin')
def admin_rules():
    """Управление правилами"""
    try:
        import json
        rules_content = get_setting('rules_content', '')
        rules_items = []
        has_rules = False
        
        if rules_content:
            try:
                # Пытаемся распарсить как JSON
                rules_items = json.loads(rules_content)
                if isinstance(rules_items, list) and len(rules_items) > 0:
                    has_rules = True
                else:
                    rules_items = []
            except (json.JSONDecodeError, ValueError):
                # Старый формат HTML - оставляем как есть для обратной совместимости
                if rules_content.strip():
                    has_rules = True
        
        return render_template('admin/rules.html', rules_content=rules_content, rules_items=rules_items, has_rules=has_rules)
    except Exception as e:
        log_error(f"Error in admin_rules route: {e}")
        flash(f'Ошибка загрузки правил: {str(e)}', 'error')
        return render_template('admin/rules.html', rules_content='', rules_items=[], has_rules=False)


@bp.route('/rules/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_rules_edit():
    """Редактирование правил"""
    try:
        if request.method == 'POST':
            import json
            
            # Получаем данные из формы
            rule_points = request.form.getlist('rule_point[]')
            rule_texts = request.form.getlist('rule_text[]')
            
            # Формируем список правил
            rules_items = []
            for point, text in zip(rule_points, rule_texts):
                point = point.strip()
                text = text.strip()
                if point and text:  # Сохраняем только заполненные строки
                    rules_items.append({
                        'point': point,
                        'text': text
                    })
            
            # Сортируем по пунктам (1, 1.1, 1.1.1 и т.д.)
            def sort_key(item):
                parts = item['point'].split('.')
                return tuple(int(p) for p in parts)
            
            rules_items.sort(key=sort_key)
            
            # Сохраняем в JSON формате
            rules_json = json.dumps(rules_items, ensure_ascii=False, indent=2)
            
            # Сохраняем правила в настройках
            conn = get_db_connection()
            user_id = session.get('user_id')
            
            try:
                # Проверяем, существует ли настройка
                existing = conn.execute('SELECT * FROM settings WHERE key = ?', ('rules_content',)).fetchone()
                
                if existing:
                    # Обновляем существующую настройку
                    conn.execute('''
                        UPDATE settings 
                        SET value = ?, updated_at = ?, updated_by = ?
                        WHERE key = ?
                    ''', (rules_json, datetime.now(), user_id, 'rules_content'))
                else:
                    # Создаем новую настройку
                    conn.execute('''
                        INSERT INTO settings (key, value, category, created_at, created_by, updated_at, updated_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', ('rules_content', rules_json, 'general', datetime.now(), user_id, datetime.now(), user_id))
                
                conn.commit()
                flash('Правила успешно сохранены', 'success')
            except Exception as e:
                log_error(f"Error saving rules: {e}")
                flash(f'Ошибка сохранения правил: {str(e)}', 'error')
            finally:
                conn.close()
            
            return redirect(url_for('admin.admin_rules'))
        
        # GET запрос - получаем существующие правила
        rules_content = get_setting('rules_content', '')
        rules_items = []
        
        if rules_content:
            try:
                import json
                # Пытаемся распарсить как JSON
                rules_items = json.loads(rules_content)
                if not isinstance(rules_items, list):
                    rules_items = []
            except (json.JSONDecodeError, ValueError):
                # Если не JSON, значит старый формат - конвертируем в новый
                # Для старых данных можно оставить пустым или попытаться распарсить HTML
                rules_items = []
        
        return render_template('admin/rules_edit.html', rules_items=rules_items)
    except Exception as e:
        log_error(f"Error in admin_rules_edit route: {e}")
        flash(f'Ошибка: {str(e)}', 'error')
        return redirect(url_for('admin.admin_rules'))
