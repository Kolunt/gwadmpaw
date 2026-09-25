"""Admin: telegram_menu."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp

@bp.route('/telegram/menu', methods=['GET', 'POST'])
@require_role('admin')
def admin_telegram_menu():
    """Управление меню Telegram бота"""
    conn = get_db_connection()
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create':
            button_text = request.form.get('button_text', '').strip()
            button_type = request.form.get('button_type', 'command')
            action_value = request.form.get('action_value', '').strip()
            sort_order = int(request.form.get('sort_order', 100))
            
            if not button_text or not action_value:
                conn.close()
                flash('Название кнопки и действие обязательны', 'error')
                return redirect(url_for('admin.admin_settings') + '#integrations')
            
            try:
                conn.execute('''
                    INSERT INTO telegram_bot_menu (button_text, button_type, action, sort_order, is_active)
                    VALUES (?, ?, ?, ?, 1)
                ''', (button_text, button_type, action_value, sort_order))
                conn.commit()
                # Обновляем команды меню в Telegram
                token = get_setting('telegram_bot_token', '')
                if token:
                    set_telegram_bot_commands(token)
                flash('Пункт меню успешно добавлен', 'success')
            except Exception as e:
                log_error(f"Error creating menu item: {e}")
                flash('Ошибка при добавлении пункта меню', 'error')
        
        elif action == 'update':
            menu_id = request.form.get('menu_id')
            button_text = request.form.get('button_text', '').strip()
            button_type = request.form.get('button_type', 'command')
            action_value = request.form.get('action_value', '').strip()
            sort_order = int(request.form.get('sort_order', 100))
            is_active = 1 if request.form.get('is_active') == '1' else 0
            
            if not menu_id or not button_text or not action_value:
                conn.close()
                flash('Все поля обязательны', 'error')
                return redirect(url_for('admin.admin_settings') + '#integrations')
            
            try:
                conn.execute('''
                    UPDATE telegram_bot_menu
                    SET button_text = ?, button_type = ?, action = ?, sort_order = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (button_text, button_type, action_value, sort_order, is_active, menu_id))
                conn.commit()
                # Обновляем команды меню в Telegram
                token = get_setting('telegram_bot_token', '')
                if token:
                    set_telegram_bot_commands(token)
                flash('Пункт меню успешно обновлен', 'success')
            except Exception as e:
                log_error(f"Error updating menu item: {e}")
                flash('Ошибка при обновлении пункта меню', 'error')
        
        elif action == 'delete':
            menu_id = request.form.get('menu_id')
            if menu_id:
                try:
                    conn.execute('DELETE FROM telegram_bot_menu WHERE id = ?', (menu_id,))
                    conn.commit()
                    # Обновляем команды меню в Telegram
                    token = get_setting('telegram_bot_token', '')
                    if token:
                        set_telegram_bot_commands(token)
                    flash('Пункт меню успешно удален', 'success')
                except Exception as e:
                    log_error(f"Error deleting menu item: {e}")
                    flash('Ошибка при удалении пункта меню', 'error')
        
        conn.close()
        return redirect(url_for('admin.admin_settings') + '#integrations')
    
    # GET - возвращаем список меню
    menu_items = conn.execute('''
        SELECT id, button_text, button_type, action, sort_order, is_active, created_at, updated_at
        FROM telegram_bot_menu
        ORDER BY sort_order ASC
    ''').fetchall()
    conn.close()
    
    return jsonify([dict(item) for item in menu_items])


@bp.route('/telegram/menu/<int:menu_id>', methods=['GET'])
@require_role('admin')
def admin_telegram_menu_get(menu_id):
    """Получение пункта меню по ID"""
    conn = get_db_connection()
    menu_item = conn.execute('''
        SELECT id, button_text, button_type, action, sort_order, is_active
        FROM telegram_bot_menu
        WHERE id = ?
    ''', (menu_id,)).fetchone()
    conn.close()
    
    if menu_item:
        return jsonify(dict(menu_item))
    return jsonify({'error': 'Menu item not found'}), 404
