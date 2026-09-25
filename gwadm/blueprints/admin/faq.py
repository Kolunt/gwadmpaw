"""Admin: faq."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp
from gwadm.services.content_init import init_default_faq_items

@bp.route('/faq')
@require_role('admin')
def admin_faq():
    """Управление FAQ"""
    # Инициализируем дефолтные FAQ элементы, если их еще нет
    init_default_faq_items()
    
    conn = get_db_connection()
    faq_items = conn.execute('''
        SELECT f.*, 
               u1.username as creator_name,
               u2.username as updater_name
        FROM faq_items f
        LEFT JOIN users u1 ON f.created_by = u1.user_id
        LEFT JOIN users u2 ON f.updated_by = u2.user_id
        ORDER BY f.category, f.sort_order, f.id
    ''').fetchall()
    
    faq_categories = conn.execute('''
        SELECT c.*, 
               COUNT(f.id) as items_count,
               u1.username as creator_name,
               u2.username as updater_name
        FROM faq_categories c
        LEFT JOIN faq_items f ON c.name = f.category
        LEFT JOIN users u1 ON c.created_by = u1.user_id
        LEFT JOIN users u2 ON c.updated_by = u2.user_id
        GROUP BY c.id
        ORDER BY c.sort_order, c.display_name
    ''').fetchall()
    
    conn.close()
    
    return render_template('admin/faq.html', 
                         faq_items=faq_items, 
                         faq_categories=faq_categories)


@bp.route('/faq/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_create():
    """Создание нового FAQ вопроса"""
    categories = get_faq_categories()
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        answer = request.form.get('answer', '').strip()
        category = request.form.get('category', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not question or not answer:
            flash('Вопрос и ответ обязательны для заполнения', 'error')
            return render_template('admin/faq_form.html', categories=categories)
        
        if not category and categories:
            category = categories[0]['name']
        elif not category:
            category = 'general'
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = 100
            is_active = 1
        
        conn = get_db_connection()
        try:
            conn.execute('''
                INSERT INTO faq_items (question, answer, category, sort_order, is_active, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (question, answer, category, sort_order, is_active, session['user_id']))
            conn.commit()
            flash('FAQ вопрос успешно создан', 'success')
            conn.close()
            return redirect(url_for('admin.admin_faq'))
        except Exception as e:
            log_error(f"Error creating FAQ: {e}")
            flash(f'Ошибка создания FAQ: {str(e)}', 'error')
            conn.close()
            return render_template('admin/faq_form.html', categories=categories)
    
    return render_template('admin/faq_form.html', categories=categories)

@bp.route('/faq/<int:faq_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_edit(faq_id):
    """Редактирование FAQ вопроса"""
    conn = get_db_connection()
    faq_item = conn.execute('SELECT * FROM faq_items WHERE id = ?', (faq_id,)).fetchone()
    
    if not faq_item:
        flash('FAQ вопрос не найден', 'error')
        conn.close()
        return redirect(url_for('admin.admin_faq'))
    
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        answer = request.form.get('answer', '').strip()
        category = request.form.get('category', 'general').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not question or not answer:
            flash('Вопрос и ответ обязательны для заполнения', 'error')
            conn.close()
            return render_template('admin/faq_form.html', faq_item=faq_item)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = faq_item['sort_order'] if faq_item['sort_order'] is not None else 100
            is_active = faq_item['is_active']
        
        try:
            conn.execute('''
                UPDATE faq_items 
                SET question = ?, answer = ?, category = ?, sort_order = ?, is_active = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (question, answer, category, sort_order, is_active, session['user_id'], faq_id))
            conn.commit()
            flash('FAQ вопрос успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('admin.admin_faq'))
        except Exception as e:
            log_error(f"Error updating FAQ: {e}")
            flash(f'Ошибка обновления FAQ: {str(e)}', 'error')
            conn.close()
    
    categories = get_faq_categories()
    conn.close()
    return render_template('admin/faq_form.html', faq_item=faq_item, categories=categories)


@bp.route('/faq/<int:faq_id>/delete', methods=['POST'])
@require_role('admin')
def admin_faq_delete(faq_id):
    """Удаление FAQ вопроса"""
    conn = get_db_connection()
    faq_item = conn.execute('SELECT * FROM faq_items WHERE id = ?', (faq_id,)).fetchone()
    
    if not faq_item:
        flash('FAQ вопрос не найден', 'error')
        conn.close()
        return redirect(url_for('admin.admin_faq'))
    
    try:
        conn.execute('DELETE FROM faq_items WHERE id = ?', (faq_id,))
        conn.commit()
        flash('FAQ вопрос успешно удален', 'success')
    except Exception as e:
        log_error(f"Error deleting FAQ: {e}")
        flash(f'Ошибка удаления FAQ: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin.admin_faq'))

@bp.route('/faq/categories/create', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_category_create():
    """Создание новой категории FAQ"""
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not name or not display_name:
            flash('Имя категории и отображаемое имя обязательны для заполнения', 'error')
            return render_template('admin/faq_category_form.html')
        
        # Проверяем уникальность имени
        conn = get_db_connection()
        existing = conn.execute('SELECT id FROM faq_categories WHERE name = ?', (name,)).fetchone()
        if existing:
            flash('Категория с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/faq_category_form.html')
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = 100
            is_active = 1
        
        try:
            conn.execute('''
                INSERT INTO faq_categories (name, display_name, description, sort_order, is_active, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (name, display_name, description, sort_order, is_active, session['user_id']))
            conn.commit()
            flash('Категория FAQ успешно создана', 'success')
            conn.close()
            return redirect(url_for('admin.admin_faq') + '#categories')
        except Exception as e:
            log_error(f"Error creating FAQ category: {e}")
            flash(f'Ошибка создания категории: {str(e)}', 'error')
            conn.close()
    
    return render_template('admin/faq_category_form.html')

@bp.route('/faq/categories/<int:category_id>/edit', methods=['GET', 'POST'])
@require_role('admin')
def admin_faq_category_edit(category_id):
    """Редактирование категории FAQ"""
    conn = get_db_connection()
    category = conn.execute('SELECT * FROM faq_categories WHERE id = ?', (category_id,)).fetchone()
    
    if not category:
        flash('Категория не найдена', 'error')
        conn.close()
        return redirect(url_for('admin.admin_faq') + '#categories')
    
    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        display_name = request.form.get('display_name', '').strip()
        description = request.form.get('description', '').strip()
        sort_order = request.form.get('sort_order', '100').strip()
        is_active = request.form.get('is_active', '0')
        
        if not name or not display_name:
            flash('Имя категории и отображаемое имя обязательны для заполнения', 'error')
            conn.close()
            return render_template('admin/faq_category_form.html', category=category)
        
        # Проверяем уникальность имени (исключая текущую категорию)
        existing = conn.execute('SELECT id FROM faq_categories WHERE name = ? AND id != ?', (name, category_id)).fetchone()
        if existing:
            flash('Категория с таким именем уже существует', 'error')
            conn.close()
            return render_template('admin/faq_category_form.html', category=category)
        
        try:
            sort_order = int(sort_order) if sort_order else 100
            is_active = 1 if is_active == '1' else 0
        except ValueError:
            sort_order = category['sort_order'] if category['sort_order'] is not None else 100
            is_active = category['is_active']
        
        try:
            # Если имя категории изменилось, обновляем все FAQ элементы с этой категорией
            if name != category['name']:
                conn.execute('''
                    UPDATE faq_items 
                    SET category = ? 
                    WHERE category = ?
                ''', (name, category['name']))
            
            conn.execute('''
                UPDATE faq_categories 
                SET name = ?, display_name = ?, description = ?, sort_order = ?, is_active = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (name, display_name, description, sort_order, is_active, session['user_id'], category_id))
            conn.commit()
            flash('Категория FAQ успешно обновлена', 'success')
            conn.close()
            return redirect(url_for('admin.admin_faq') + '#categories')
        except Exception as e:
            log_error(f"Error updating FAQ category: {e}")
            flash(f'Ошибка обновления категории: {str(e)}', 'error')
            conn.close()
    
    conn.close()
    return render_template('admin/faq_category_form.html', category=category)

@bp.route('/faq/categories/<int:category_id>/delete', methods=['POST'])
@require_role('admin')
def admin_faq_category_delete(category_id):
    """Удаление категории FAQ"""
    conn = get_db_connection()
    category = conn.execute('SELECT * FROM faq_categories WHERE id = ?', (category_id,)).fetchone()
    
    if not category:
        flash('Категория не найдена', 'error')
        conn.close()
        return redirect(url_for('admin.admin_faq') + '#categories')
    
    # Проверяем, есть ли FAQ элементы с этой категорией
    items_count = conn.execute('SELECT COUNT(*) as count FROM faq_items WHERE category = ?', (category['name'],)).fetchone()
    
    if items_count['count'] > 0:
        flash(f'Нельзя удалить категорию, в которой есть вопросы ({items_count["count"]} шт.). Сначала переместите или удалите вопросы.', 'error')
        conn.close()
        return redirect(url_for('admin.admin_faq') + '#categories')
    
    try:
        conn.execute('DELETE FROM faq_categories WHERE id = ?', (category_id,))
        conn.commit()
        flash('Категория FAQ успешно удалена', 'success')
    except Exception as e:
        log_error(f"Error deleting FAQ category: {e}")
        flash(f'Ошибка удаления категории: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('admin.admin_faq') + '#categories')
