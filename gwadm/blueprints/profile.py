"""Profile dashboard, edit, view, admin comments, and avatar API."""

import secrets
import sqlite3

from flask import Blueprint, flash, jsonify, redirect, render_template, request, session, url_for

from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role
from gwadm.logging_config import log_error
from gwadm.services.avatars import (
    generate_unique_avatar_candidates,
    get_avatar_url,
    get_used_avatar_seeds,
)
from gwadm.services.awards import get_user_awards
from gwadm.services.profile_comments import (
    add_user_admin_comment,
    delete_user_admin_comment,
    get_user_admin_comments,
    update_user_admin_comment,
)
from gwadm.services.roles import get_user_roles
from gwadm.services.titles import get_user_titles

bp = Blueprint('profile', __name__)


@bp.route('/dashboard')
@require_login
def dashboard():
    conn = get_db_connection()
    try:
        user = conn.execute(
            'SELECT * FROM users WHERE user_id = ?', (session['user_id'],)
        ).fetchone()

        user_roles = get_user_roles(session['user_id'])

        telegram_verified = False
        telegram_info = None
        try:
            telegram_user = conn.execute('''
                SELECT verified, telegram_chat_id, telegram_username, verified_at
                FROM telegram_users
                WHERE user_id = ?
            ''', (session['user_id'],)).fetchone()

            if telegram_user:
                telegram_verified = bool(telegram_user['verified'])
                telegram_info = dict(telegram_user)
        except sqlite3.OperationalError as e:
            log_error(f"Error fetching telegram user: {e}")
    finally:
        conn.close()

    return render_template(
        'dashboard.html',
        user=user,
        user_roles=user_roles,
        telegram_verified=telegram_verified,
        telegram_info=telegram_info,
    )


@bp.route('/api/avatar/generate-options', methods=['POST'])
@require_login
def api_generate_avatar_options():
    """API endpoint для генерации вариантов аватаров по стилю."""
    data = request.get_json()
    style = data.get('style', 'avataaars')
    count = data.get('count', 20)

    if not style:
        return jsonify({'error': 'Style is required'}), 400

    conn = get_db_connection()
    try:
        used_seeds = set(row[0] for row in conn.execute(
            'SELECT avatar_seed FROM users WHERE avatar_seed IS NOT NULL'
        ).fetchall())
        conn.close()
    except Exception as e:
        log_error(f"Error fetching used seeds: {e}")
        conn.close()
        used_seeds = set()

    options = []
    attempts = 0
    max_attempts = count * 10

    while len(options) < count and attempts < max_attempts:
        random_part = secrets.token_hex(8)
        seed = f"option_{random_part}"

        if seed not in used_seeds:
            options.append({
                'seed': seed,
                'url': get_avatar_url(seed, style, 128),
                'unique': True
            })
            used_seeds.add(seed)

        attempts += 1

    return jsonify({
        'style': style,
        'options': options,
        'count': len(options)
    })


@bp.route('/profile/edit', methods=['GET', 'POST'])
@require_login
def edit_profile():
    """Редактирование профиля пользователя."""
    conn = get_db_connection()
    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (session['user_id'],)
    ).fetchone()

    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('profile.dashboard'))

    if request.method == 'POST':
        bio = request.form.get('bio', '').strip()
        contact_info = request.form.get('contact_info', '').strip()
        avatar_seed = request.form.get('avatar_seed', '').strip()
        avatar_style = request.form.get('avatar_style', 'avataaars').strip()
        email = request.form.get('email', '').strip()
        phone = request.form.get('phone', '').strip()
        telegram = request.form.get('telegram', '').strip()
        whatsapp = request.form.get('whatsapp', '').strip()
        viber = request.form.get('viber', '').strip()
        last_name = request.form.get('last_name', '').strip()
        first_name = request.form.get('first_name', '').strip()
        middle_name = request.form.get('middle_name', '').strip()
        postal_code = request.form.get('postal_code', '').strip()
        country = request.form.get('country', '').strip()
        city = request.form.get('city', '').strip()
        street = request.form.get('street', '').strip()
        house = request.form.get('house', '').strip()
        building = request.form.get('building', '').strip()
        apartment = request.form.get('apartment', '').strip()

        try:
            if avatar_seed and avatar_style:
                used_seeds = get_used_avatar_seeds(exclude_user_id=session['user_id'])
                if avatar_seed in used_seeds:
                    flash('Выбранный аватар уже используется другим пользователем. Пожалуйста, выберите другой.', 'error')
                    conn.close()
                    return render_template('edit_profile.html', user=user)

            if avatar_seed and avatar_style:
                conn.execute('''
                    UPDATE users
                    SET bio = ?, contact_info = ?, avatar_style = ?, avatar_seed = ?,
                        email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?,
                        last_name = ?, first_name = ?, middle_name = ?,
                        postal_code = ?, country = ?, city = ?, street = ?, house = ?, building = ?, apartment = ?
                    WHERE user_id = ?
                ''', (bio, contact_info, avatar_style, avatar_seed, email, phone, telegram, whatsapp, viber,
                      last_name, first_name, middle_name,
                      postal_code, country, city, street, house, building, apartment, session['user_id']))
            else:
                conn.execute('''
                    UPDATE users
                    SET bio = ?, contact_info = ?,
                        email = ?, phone = ?, telegram = ?, whatsapp = ?, viber = ?,
                        last_name = ?, first_name = ?, middle_name = ?,
                        postal_code = ?, country = ?, city = ?, street = ?, house = ?, building = ?, apartment = ?
                    WHERE user_id = ?
                ''', (bio, contact_info, email, phone, telegram, whatsapp, viber,
                      last_name, first_name, middle_name,
                      postal_code, country, city, street, house, building, apartment, session['user_id']))

            conn.commit()
            flash('Профиль успешно обновлен', 'success')
            conn.close()
            return redirect(url_for('profile.dashboard'))
        except Exception as e:
            log_error(f"Error updating profile: {e}")
            flash(f'Ошибка обновления профиля: {str(e)}', 'error')
            conn.close()

    conn.close()
    return render_template('edit_profile.html', user=user)


@bp.route('/profile/clear', methods=['POST'])
@require_login
@require_role('admin')
def clear_profile():
    """Очистка всех редактируемых полей профиля (только для администратора)."""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'success': False, 'error': 'Необходимо авторизоваться'}), 401

    conn = get_db_connection()
    try:
        conn.execute('''
            UPDATE users
            SET bio = NULL, contact_info = NULL,
                email = NULL, phone = NULL, telegram = NULL, whatsapp = NULL, viber = NULL,
                last_name = NULL, first_name = NULL, middle_name = NULL,
                postal_code = NULL, country = NULL, city = NULL, street = NULL,
                house = NULL, building = NULL, apartment = NULL
            WHERE user_id = ?
        ''', (user_id,))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'Все редактируемые поля профиля очищены'})
    except Exception as e:
        log_error(f"Error clearing profile: {e}")
        conn.close()
        return jsonify({'success': False, 'error': f'Ошибка очистки профиля: {str(e)}'}), 500


@bp.route('/api/avatar/candidates', methods=['GET'])
@require_login
def get_avatar_candidates():
    """API endpoint для получения уникальных кандидатов аватаров выбранного стиля."""
    style = request.args.get('style', 'avataaars')
    count = int(request.args.get('count', 20))

    valid_styles = [
        'adventurer', 'adventurer-neutral', 'avataaars', 'avataaars-neutral',
        'big-ears', 'big-ears-neutral', 'big-smile', 'bottts', 'bottts-neutral',
        'croodles', 'croodles-neutral', 'fun-emoji', 'icons', 'identicon', 'initials',
        'lorelei', 'lorelei-neutral', 'micah', 'miniavs', 'open-peeps', 'personas',
        'pixel-art', 'pixel-art-neutral', 'rings', 'shapes', 'thumbs'
    ]

    if style not in valid_styles:
        return jsonify({'error': 'Invalid style'}), 400

    candidates = generate_unique_avatar_candidates(style, count, exclude_user_id=session['user_id'])

    return jsonify({
        'candidates': [
            {
                'seed': seed,
                'url': get_avatar_url(seed, style, size=128)
            }
            for seed in candidates
        ]
    })


@bp.route('/profile/<int:user_id>')
def view_profile(user_id):
    """Просмотр профиля другого пользователя (доступно всем)."""
    conn = get_db_connection()

    user = conn.execute(
        'SELECT * FROM users WHERE user_id = ?', (user_id,)
    ).fetchone()

    if not user:
        flash('Пользователь не найден', 'error')
        conn.close()
        return redirect(url_for('public.participants'))

    user_roles = get_user_roles(user_id)
    user_titles = get_user_titles(user_id)
    user_awards = get_user_awards(user_id)

    blocker_info = None
    user_keys = user.keys()
    if 'is_blocked' in user_keys and user['is_blocked'] and 'blocked_by' in user_keys and user['blocked_by']:
        blocker = conn.execute('SELECT user_id, username FROM users WHERE user_id = ?', (user['blocked_by'],)).fetchone()
        if blocker:
            blocker_info = dict(blocker)

    conn.close()

    session_user_id = session.get('user_id')
    try:
        session_user_id_int = int(session_user_id) if session_user_id is not None else None
    except (TypeError, ValueError):
        session_user_id_int = None
    is_own_profile = session_user_id_int == user_id
    is_admin = 'admin' in session.get('roles', []) if 'roles' in session else False
    impersonation_active = bool(session.get('impersonation_original'))
    can_impersonate = is_admin and not is_own_profile and not impersonation_active

    user_keys = user.keys()
    user_bio = user['bio'] if 'bio' in user_keys else None
    user_contact_info = user['contact_info'] if 'contact_info' in user_keys else None

    show_about = bool(user_bio or user_contact_info) and (is_admin or is_own_profile)
    bio_to_display = user_bio if show_about else None
    contact_info_to_display = user_contact_info if show_about and is_admin else None

    admin_comments = get_user_admin_comments(user_id, viewer_is_admin=is_admin)

    if is_admin:
        has_comments = True
    else:
        conn = get_db_connection()
        public_comments_count = conn.execute('''
            SELECT COUNT(*) as count
            FROM user_admin_comments
            WHERE user_id = ? AND (is_admin_only = 0 OR is_thanks_from_recipient = 1)
        ''', (user_id,)).fetchone()
        conn.close()
        has_comments = public_comments_count['count'] > 0 if public_comments_count else False

    return render_template(
        'view_profile.html',
        user=dict(user),
        user_roles=user_roles,
        user_titles=user_titles,
        user_awards=user_awards,
        is_own_profile=is_own_profile,
        is_admin=is_admin,
        can_impersonate=can_impersonate,
        impersonation_active=impersonation_active,
        show_about=show_about,
        bio_to_display=bio_to_display,
        contact_info_to_display=contact_info_to_display,
        blocker_info=blocker_info,
        admin_comments=admin_comments,
        has_comments=has_comments,
    )


@bp.route('/profile/<int:user_id>/admin-comment', methods=['POST'])
@require_role('admin')
def add_admin_comment(user_id):
    """Добавляет комментарий администратора к профилю пользователя."""
    if not session.get('user_id'):
        flash('Необходима авторизация', 'error')
        return redirect(url_for('auth.login'))

    admin_user_id = session.get('user_id')
    comment = request.form.get('comment', '').strip()
    is_admin_only = request.form.get('is_admin_only') == 'on'

    if not comment:
        flash('Комментарий не может быть пустым', 'error')
        return redirect(url_for('profile.view_profile', user_id=user_id))

    if add_user_admin_comment(user_id, admin_user_id, comment, is_admin_only=is_admin_only):
        flash('Комментарий добавлен', 'success')
    else:
        flash('Ошибка при добавлении комментария', 'error')

    return redirect(url_for('profile.view_profile', user_id=user_id) + '#comments')


@bp.route('/profile/<int:user_id>/admin-comment/<int:comment_id>/update', methods=['POST'])
@require_role('admin')
def update_admin_comment(user_id, comment_id):
    """Обновляет комментарий администратора."""
    if not session.get('user_id'):
        flash('Необходима авторизация', 'error')
        return redirect(url_for('auth.login'))

    admin_user_id = session.get('user_id')
    comment = request.form.get('comment', '').strip()

    if not comment:
        flash('Комментарий не может быть пустым', 'error')
        return redirect(url_for('profile.view_profile', user_id=user_id) + '#comments')

    if update_user_admin_comment(comment_id, admin_user_id, comment):
        flash('Комментарий обновлен', 'success')
    else:
        flash('Ошибка при обновлении комментария', 'error')

    return redirect(url_for('profile.view_profile', user_id=user_id) + '#comments')


@bp.route('/profile/<int:user_id>/admin-comment/<int:comment_id>/delete', methods=['POST'])
@require_role('admin')
def delete_admin_comment(user_id, comment_id):
    """Удаляет комментарий администратора."""
    if not session.get('user_id'):
        flash('Необходима авторизация', 'error')
        return redirect(url_for('auth.login'))

    admin_user_id = session.get('user_id')

    if delete_user_admin_comment(comment_id, admin_user_id):
        flash('Комментарий удален', 'success')
    else:
        flash('Ошибка при удалении комментария', 'error')

    return redirect(url_for('profile.view_profile', user_id=user_id) + '#comments')
