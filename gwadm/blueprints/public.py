"""Public pages: landing, participants, FAQ, rules, contacts, rating."""

import json
import traceback
from collections import OrderedDict
from datetime import datetime

from flask import Blueprint, make_response, render_template, request, session

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
from gwadm.services.avatars import get_avatar_url
from gwadm.services.events_stages import (
    EVENT_STAGES,
    get_current_event_stage,
    get_event_now,
    get_event_registrations_count,
    get_event_stages,
    parse_event_datetime,
)
from gwadm.services.roles import get_user_roles
from gwadm.services.settings import get_setting
from gwadm.services.titles import get_user_titles

bp = Blueprint('public', __name__)


@bp.route('/')
def index():
    conn = get_db_connection()

    total_users = conn.execute('SELECT COUNT(*) as count FROM users').fetchone()['count']
    online_users = conn.execute('''
        SELECT COUNT(*) as count FROM users
        WHERE datetime(last_login) > datetime('now', '-1 hour')
    ''').fetchone()['count']

    events_list = conn.execute('''
        SELECT e.*, u.username as creator_name
        FROM events e
        LEFT JOIN users u ON e.created_by = u.user_id
        WHERE e.deleted_at IS NULL
        ORDER BY e.created_at DESC
    ''').fetchall()

    events_with_stages_raw = []
    now = get_event_now()
    stage_info_map = {stage['type']: stage for stage in EVENT_STAGES}

    def parse_dt(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    for event in events_list:
        current_stage = get_current_event_stage(event['id'])
        display_stage_name = None
        next_stage = None
        if current_stage:
            display_stage_name = current_stage['info']['name']
            if current_stage['info']['type'] == 'registration_closed':
                lottery_stage = next((stage for stage in EVENT_STAGES if stage['type'] == 'lottery'), None)
                display_stage_name = lottery_stage['name'] if lottery_stage else 'Жеребьёвка'

        stages = get_event_stages(event['id'])
        stages_dict = {stage['stage_type']: dict(stage) for stage in stages}
        for stage in stages:
            start_dt = parse_dt(stage['start_datetime'])
            if not start_dt or start_dt <= now:
                continue

            stage_info = stage_info_map.get(stage['stage_type'])
            stage_name = stage_info['name'] if stage_info else stage['stage_type']

            if (not next_stage) or start_dt < next_stage['start_dt']:
                next_stage = {
                    'name': stage_name,
                    'start_dt': start_dt,
                    'start_iso': start_dt.isoformat()
                }

        if current_stage and not next_stage:
            current_type = current_stage['info']['type']
            try:
                current_index = next(i for i, s in enumerate(EVENT_STAGES) if s['type'] == current_type)
            except StopIteration:
                current_index = None

            if current_index is not None:
                for idx in range(current_index + 1, len(EVENT_STAGES)):
                    next_info = EVENT_STAGES[idx]
                    next_data = stages_dict.get(next_info['type'])
                    candidate_raw = None
                    candidate_dt = None

                    if next_data and next_data.get('start_datetime'):
                        candidate_raw = next_data['start_datetime']
                    elif next_data and next_data.get('end_datetime'):
                        candidate_raw = next_data['end_datetime']
                    elif next_info['type'] == 'after_party' and current_stage['data'] and current_stage['data'].get('end_datetime'):
                        candidate_raw = current_stage['data']['end_datetime']

                    if candidate_raw:
                        candidate_dt = parse_event_datetime(str(candidate_raw))

                    if candidate_dt and candidate_dt > now:
                        next_stage = {
                            'name': next_info['name'],
                            'start_dt': candidate_dt,
                            'start_iso': candidate_dt.isoformat()
                        }
                        break

        events_with_stages_raw.append({
            'event': event,
            'current_stage': current_stage,
            'display_stage_name': display_stage_name,
            'next_stage': next_stage
        })

    events_with_stages = events_with_stages_raw

    for item in events_with_stages:
        event = item['event']
        item['registrations_count'] = get_event_registrations_count(event['id'])

    project_name = get_setting('project_name', 'Анонимные Деды Морозы')

    conn.close()

    return render_template(
        'index.html',
        total_users=total_users,
        online_users=online_users,
        events_with_stages=events_with_stages,
        project_name=project_name,
    )


@bp.route('/participants')
def participants():
    """Страница со списком участников."""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        search_query = request.args.get('search', '').strip()

        if search_query:
            log_debug(f"Participants search: query='{search_query}', encoded={search_query.encode('utf-8')}")

        per_page = min(max(per_page, 10), 100)

        conn = get_db_connection()

        if search_query:
            all_users = conn.execute('''
                SELECT
                    u.user_id,
                    u.username,
                    u.avatar_seed,
                    u.avatar_style,
                    u.created_at,
                    u.last_login,
                    GROUP_CONCAT(r.display_name, ', ') as roles
                FROM users u
                LEFT JOIN user_roles ur ON u.user_id = ur.user_id
                LEFT JOIN roles r ON ur.role_id = r.id
                GROUP BY u.user_id
                ORDER BY u.created_at ASC
            ''').fetchall()

            filtered_users = []
            for user in all_users:
                user_keys = user.keys()
                username = user['username'] if 'username' in user_keys else ''
                user_id_str = str(user['user_id']) if 'user_id' in user_keys else ''
                roles_str = user['roles'] if ('roles' in user_keys and user['roles']) else ''

                if (search_query.lower() in username.lower() or
                    search_query.lower() in user_id_str.lower() or
                    search_query.lower() in roles_str.lower()):
                    filtered_users.append(user)

            total_count = len(filtered_users)
            offset = (page - 1) * per_page
            users = filtered_users[offset:offset + per_page]
        else:
            total_count = conn.execute('''
                SELECT COUNT(DISTINCT u.user_id)
                FROM users u
            ''').fetchone()[0]

            offset = (page - 1) * per_page
            users_query = '''
                SELECT
                    u.user_id,
                    u.username,
                    u.avatar_seed,
                    u.avatar_style,
                    u.created_at,
                    u.last_login,
                    GROUP_CONCAT(r.display_name, ', ') as roles
                FROM users u
                LEFT JOIN user_roles ur ON u.user_id = ur.user_id
                LEFT JOIN roles r ON ur.role_id = r.id
                GROUP BY u.user_id
                ORDER BY u.created_at ASC
                LIMIT ? OFFSET ?
            '''
            users = conn.execute(users_query, [per_page, offset]).fetchall()

        participants_data = []
        for user in users:
            user_keys = user.keys()
            last_login = user['last_login'] if 'last_login' in user_keys else None

            status = 'Оффлайн'
            if last_login:
                try:
                    last_login_str = str(last_login).split('.')[0] if '.' in str(last_login) else str(last_login)
                    last_login_date = datetime.strptime(last_login_str, '%Y-%m-%d %H:%M:%S')
                    now = datetime.now()
                    if (now - last_login_date).total_seconds() < 3600:
                        status = 'Онлайн'
                    elif (now - last_login_date).days == 0:
                        status = 'Был сегодня'
                except Exception as e:
                    user_id = user['user_id'] if 'user_id' in user_keys else 'unknown'
                    log_debug(f"Error parsing last_login for user {user_id}: {e}")

            roles_str = user['roles'] if ('roles' in user_keys and user['roles']) else 'Пользователь'
            user_id = user['user_id'] if 'user_id' in user_keys else None
            username = user['username'] if ('username' in user_keys and user['username']) else 'Неизвестно'
            avatar_seed = user['avatar_seed'] if 'avatar_seed' in user_keys else None
            avatar_style = user['avatar_style'] if 'avatar_style' in user_keys else None
            created_at = user['created_at'] if ('created_at' in user_keys and user['created_at']) else 'N/A'

            participants_data.append({
                'user_id': user_id,
                'username': username,
                'avatar_seed': avatar_seed,
                'avatar_style': avatar_style,
                'status': status,
                'roles': roles_str,
                'created_at': created_at
            })

        conn.close()

        total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
        has_prev = page > 1
        has_next = page < total_pages

        log_debug(f"Participants pagination: page={page}, per_page={per_page}, total_count={total_count}, total_pages={total_pages}, participants_count={len(participants_data)}")

        return render_template(
            'participants.html',
            participants=participants_data,
            get_avatar_url=get_avatar_url,
            page=page,
            per_page=per_page,
            total_count=total_count,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            search_query=search_query,
        )
    except Exception as e:
        log_error(f"Error in participants route: {e}")
        log_error(traceback.format_exc())
        try:
            conn.close()
        except Exception:
            pass
        return f"Ошибка при загрузке участников: {str(e)}", 500


@bp.route('/faq')
def faq():
    """Страница с часто задаваемыми вопросами."""
    conn = get_db_connection()
    categories_rows = conn.execute('''
        SELECT name, display_name
        FROM faq_categories
        WHERE is_active = 1
        ORDER BY sort_order, display_name
    ''').fetchall()
    items_rows = conn.execute('''
        SELECT question, answer, category, sort_order, id
        FROM faq_items
        WHERE is_active = 1
        ORDER BY sort_order, id
    ''').fetchall()
    conn.close()

    def _format_category_label(key: str, display: str | None) -> str:
        if display:
            return display
        mapping = {
            'general': 'Общие вопросы',
            'events': 'Мероприятия',
            'profile': 'Профиль и настройки',
            'technical': 'Технические вопросы',
            'security': 'Безопасность и конфиденциальность',
        }
        return mapping.get(key, key.replace('_', ' ').title())

    sections = OrderedDict()
    for row in categories_rows:
        key = row['name']
        sections[key] = {
            'key': key,
            'display_name': _format_category_label(key, row['display_name']),
            'entries': []
        }

    for item in items_rows:
        key = (item['category'] or '').strip() or 'general'
        if key not in sections:
            sections[key] = {
                'key': key,
                'display_name': _format_category_label(key, None),
                'entries': []
            }
        sections[key]['entries'].append({
            'id': item['id'],
            'question': item['question'],
            'answer': item['answer']
        })

    faq_sections = [section for section in sections.values() if section['entries']]

    return render_template('faq.html', faq_sections=faq_sections)


@bp.route('/rules')
def rules():
    """Страница с правилами."""
    try:
        rules_content = get_setting('rules_content', '')
        rules_items = []

        if rules_content:
            try:
                rules_items = json.loads(rules_content)
                if not isinstance(rules_items, list):
                    rules_items = []
            except (json.JSONDecodeError, ValueError):
                pass

        return render_template('rules.html', rules_content=rules_content, rules_items=rules_items)
    except Exception as e:
        log_error(f"Error in rules route: {e}")
        return render_template('rules.html', rules_content='', rules_items=[])


@bp.route('/contacts')
def contacts():
    """Страница контактов - показывает администраторов/модераторов и пользователей со званиями."""
    conn = get_db_connection()

    admins_moderators = conn.execute('''
        SELECT DISTINCT u.*,
               GROUP_CONCAT(DISTINCT r.name) as roles_list
        FROM users u
        INNER JOIN user_roles ur ON u.user_id = ur.user_id
        INNER JOIN roles r ON ur.role_id = r.id
        WHERE r.name IN ('admin', 'moderator')
        GROUP BY u.user_id
        ORDER BY
            CASE WHEN r.name = 'admin' THEN 1 ELSE 2 END,
            u.username
    ''').fetchall()

    users_with_titles = conn.execute('''
        SELECT DISTINCT u.*
        FROM users u
        INNER JOIN user_titles ut ON u.user_id = ut.user_id
        WHERE u.user_id NOT IN (
            SELECT DISTINCT u2.user_id
            FROM users u2
            INNER JOIN user_roles ur2 ON u2.user_id = ur2.user_id
            INNER JOIN roles r2 ON ur2.role_id = r2.id
            WHERE r2.name IN ('admin', 'moderator')
        )
        GROUP BY u.user_id
        ORDER BY u.username
    ''').fetchall()

    users_with_titles_data = []
    for user in users_with_titles:
        user_dict = dict(user)
        user_titles = get_user_titles(user['user_id'])
        user_dict['titles'] = user_titles
        users_with_titles_data.append(user_dict)

    admins_moderators_data = []
    for user in admins_moderators:
        user_dict = dict(user)
        user_roles = get_user_roles(user['user_id'])
        user_dict['roles'] = user_roles
        admins_moderators_data.append(user_dict)

    conn.close()

    return render_template(
        'contacts.html',
        admins_moderators=admins_moderators_data,
        users_with_titles=users_with_titles_data,
    )


@bp.route('/rating')
def user_rating():
    """Простая система рейтинга участников (прямая ссылка)."""
    roles = session.get('roles')
    if isinstance(roles, (list, tuple, set)):
        is_admin = 'admin' in roles
    elif isinstance(roles, str):
        is_admin = roles == 'admin'
    else:
        is_admin = False

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)

    per_page = min(max(per_page, 10), 200)
    page = max(1, page)

    from gwadm.services.rating import (
        get_live_rating_page,
        get_rating_page,
        rating_cache_is_populated,
    )

    conn = get_db_connection()
    try:
        if rating_cache_is_populated(conn):
            rating_rows_raw, total_count = get_rating_page(conn, page, per_page)
        else:
            rating_rows_raw, total_count = get_live_rating_page(conn, page, per_page)
    finally:
        conn.close()

    rating_rows = []
    for row in rating_rows_raw:
        try:
            rating_rows.append({
                'user_id': row['user_id'],
                'username': row['username'],
                'rating': float(row['total_points']) if row['total_points'] is not None else 0.0,
            })
        except (ValueError, TypeError):
            continue

    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    has_prev = page > 1
    has_next = page < total_pages

    resp = make_response(render_template(
        'rating.html',
        rating_rows=rating_rows,
        is_admin=is_admin,
        page=page,
        per_page=per_page,
        total_count=total_count,
        total_pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
    ))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp
