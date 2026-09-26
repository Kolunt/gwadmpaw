"""Admin: hero whispers on homepage."""

from flask import flash, redirect, render_template, request, url_for

from gwadm.blueprints.admin import bp
from gwadm.db import get_db_connection
from gwadm.decorators import require_role
from gwadm.logging_config import log_error
from gwadm.services.hero_whispers import (
    create_whisper,
    delete_whisper,
    is_hero_whispers_enabled,
    list_all_whispers,
    set_hero_whispers_enabled,
    toggle_whisper_active,
    update_whisper,
)


@bp.route('/hero-whispers', methods=['GET', 'POST'])
@require_role('admin')
def admin_hero_whispers():
    """Управление шёпотами на главной странице."""
    conn = get_db_connection()

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'toggle_enabled':
            enabled = request.form.get('hero_whispers_enabled') == '1'
            set_hero_whispers_enabled(enabled)
            flash(
                'Шёпоты на главной включены' if enabled else 'Шёпоты на главной отключены',
                'success',
            )

        elif action == 'create':
            text = request.form.get('text', '').strip()
            sort_order_raw = request.form.get('sort_order', '100').strip()
            try:
                sort_order = int(sort_order_raw) if sort_order_raw else 100
            except ValueError:
                sort_order = 100

            if not text:
                flash('Текст фразы обязателен', 'error')
            elif len(text) > 300:
                flash('Текст фразы не должен превышать 300 символов', 'error')
            else:
                try:
                    create_whisper(conn, text, sort_order)
                    flash('Фраза добавлена', 'success')
                except Exception as e:
                    log_error(f"Error creating hero whisper: {e}")
                    flash('Ошибка при добавлении фразы', 'error')

        elif action == 'update':
            whisper_id = request.form.get('whisper_id')
            text = request.form.get('text', '').strip()
            sort_order_raw = request.form.get('sort_order', '100').strip()
            is_active = 1 if request.form.get('is_active') == '1' else 0

            try:
                sort_order = int(sort_order_raw) if sort_order_raw else 100
            except ValueError:
                sort_order = 100

            if not whisper_id or not text:
                flash('ID и текст обязательны', 'error')
            elif len(text) > 300:
                flash('Текст фразы не должен превышать 300 символов', 'error')
            else:
                if update_whisper(conn, int(whisper_id), text, sort_order, is_active):
                    flash('Фраза обновлена', 'success')
                else:
                    flash('Ошибка при обновлении фразы', 'error')

        elif action == 'delete':
            whisper_id = request.form.get('whisper_id')
            if whisper_id:
                if delete_whisper(conn, int(whisper_id)):
                    flash('Фраза удалена', 'success')
                else:
                    flash('Ошибка при удалении фразы', 'error')

        elif action == 'toggle_active':
            whisper_id = request.form.get('whisper_id')
            if whisper_id:
                if toggle_whisper_active(conn, int(whisper_id)):
                    flash('Статус фразы изменён', 'success')
                else:
                    flash('Ошибка при изменении статуса', 'error')

        conn.close()
        return redirect(url_for('admin.admin_hero_whispers'))

    whispers = list_all_whispers(conn)
    enabled = is_hero_whispers_enabled()
    conn.close()

    return render_template(
        'admin/hero_whispers.html',
        whispers=whispers,
        hero_whispers_enabled=enabled,
    )
