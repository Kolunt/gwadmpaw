"""Admin: UI strings (localization) editor."""

from flask import flash, redirect, render_template, request, session, url_for

from gwadm.blueprints.admin import bp
from gwadm.csrf import validate_csrf_token
from gwadm.db import get_db_connection
from gwadm.decorators import require_role
from gwadm.logging_config import log_error
from gwadm.services.ui_strings import (
    apply_bulk_updates,
    count_strings,
    invalidate_ui_strings_cache,
    list_categories,
    list_strings_for_admin,
)


def _parse_updates_from_form() -> list[tuple[str, str, str]]:
    """Collect (msg_key, locale, value) from indexed form fields."""
    out: list[tuple[str, str, str]] = []
    for i in range(100):
        mk = request.form.get(f'key_{i}')
        if not mk:
            break
        ru = request.form.get(f'ru_{i}', '')
        en = request.form.get(f'en_{i}', '')
        out.append((mk, 'ru', ru or ''))
        out.append((mk, 'en', en or ''))
    return out


@bp.route('/localization', methods=['GET', 'POST'])
@require_role('admin')
def admin_localization():
    """Table of UI strings (RU/EN) with filters."""
    conn = get_db_connection()
    try:
        if request.method == 'POST':
            token = request.form.get('_csrf_token')
            if not validate_csrf_token(token):
                flash('Ошибка CSRF', 'error')
                return redirect(url_for('admin.admin_localization'))
            updates = _parse_updates_from_form()
            uid = session.get('user_id')
            try:
                editor_id = int(uid) if uid is not None else None
            except (TypeError, ValueError):
                editor_id = None
            try:
                n = apply_bulk_updates(conn, updates, editor_id=editor_id)
                conn.commit()
                invalidate_ui_strings_cache()
                flash(f'Сохранено строк: {n}', 'success')
            except Exception as e:
                log_error(f'admin_localization save: {e}')
                flash('Не удалось сохранить', 'error')
            cat = (request.form.get('filter_category') or '').strip() or None
            qry = (request.form.get('filter_q') or '').strip() or None
            page_s = (request.form.get('filter_page') or '1').strip()
            kw: dict = {'page': page_s}
            if cat:
                kw['category'] = cat
            if qry:
                kw['q'] = qry
            return redirect(url_for('admin.admin_localization', **kw))

        category = (request.args.get('category') or '').strip() or None
        q = (request.args.get('q') or '').strip() or None
        try:
            page = max(1, int(request.args.get('page', 1)))
        except ValueError:
            page = 1
        per_page = 50
        total = count_strings(conn, category, q)
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page

        rows_raw = list_strings_for_admin(
            conn, category=category, q=q, limit=per_page, offset=offset
        )
        rows = [
            {
                'msg_key': r['msg_key'],
                'category': r['category'],
                'default_ru': r['default_ru'],
                'ru_value': r['ru_value'],
                'en_value': r['en_value'],
            }
            for r in rows_raw
        ]
        categories = list_categories(conn)
        default_language = None
        try:
            from gwadm.services.settings import get_setting

            default_language = get_setting('default_language', 'ru')
        except Exception:
            default_language = 'ru'

        return render_template(
            'admin/localization.html',
            rows=rows,
            categories=categories,
            current_category=category or '',
            search_query=q or '',
            page=page,
            total=total,
            pages=pages,
            per_page=per_page,
            default_language=default_language or 'ru',
        )
    finally:
        conn.close()
