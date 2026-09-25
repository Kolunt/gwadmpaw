"""Admin: assignments_admin."""

from flask import (
    Blueprint, flash, jsonify, redirect, render_template, request, session, url_for,
)
from gwadm.db import get_db_connection
from gwadm.decorators import require_login, require_role, require_any_role
from gwadm.logging_config import log_error, log_debug

from gwadm.blueprints.admin import bp
from gwadm.services.assignments import get_admin_letter_assignments

@bp.route('/letters/archived')
@require_role('admin')
def admin_letters_archived():
    """Просмотр архивных чатов (расформированных пар)"""
    conn = get_db_connection()
    try:
        archived_chats = conn.execute('''
            SELECT
                ach.*,
                e.name AS event_name,
                e.id AS event_id,
                santa.username AS santa_username,
                santa.first_name AS santa_first_name,
                santa.last_name AS santa_last_name,
                santa.middle_name AS santa_middle_name,
                recipient.username AS recipient_username,
                COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
                COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
                COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
                archiver.username AS archiver_username,
                (SELECT COUNT(*) FROM letter_messages WHERE assignment_id = ach.original_assignment_id) AS message_count,
                (SELECT MAX(created_at) FROM letter_messages WHERE assignment_id = ach.original_assignment_id) AS last_message_at
            FROM assignment_chat_history ach
            JOIN events e ON ach.event_id = e.id
            JOIN users santa ON ach.santa_user_id = santa.user_id
            JOIN users recipient ON ach.recipient_user_id = recipient.user_id
            LEFT JOIN event_registration_details rd
                ON rd.event_id = ach.event_id AND rd.user_id = ach.recipient_user_id
            LEFT JOIN users archiver ON ach.archived_by = archiver.user_id
            ORDER BY ach.archived_at DESC
        ''').fetchall()
        conn.close()
        
        archived_data = []
        for row in archived_chats:
            archived_data.append({
                'id': row['id'],
                'original_assignment_id': row['original_assignment_id'],
                'event_id': row['event_id'],
                'event_name': row['event_name'],
                'santa_user_id': row['santa_user_id'],
                'santa_username': row['santa_username'],
                'santa_name': f"{row['santa_last_name'] or ''} {row['santa_first_name'] or ''} {row['santa_middle_name'] or ''}".strip() or row['santa_username'],
                'recipient_user_id': row['recipient_user_id'],
                'recipient_username': row['recipient_username'],
                'recipient_name': f"{row['recipient_last_name'] or ''} {row['recipient_first_name'] or ''} {row['recipient_middle_name'] or ''}".strip() or row['recipient_username'],
                'archived_at': row['archived_at'],
                'archived_by': row['archived_by'],
                'archiver_username': row['archiver_username'],
                'notes': row['notes'],
                'message_count': row['message_count'] or 0,
                'last_message_at': row['last_message_at']
            })
        
        return render_template('admin/letters_archived.html', archived_chats=archived_data)
    except Exception as e:
        log_error(f"Error loading archived chats: {e}")
        flash('Ошибка при загрузке архивных чатов', 'error')
        return redirect(url_for('admin.admin_letters'))


@bp.route('/letters')
@require_role('admin')
def admin_letters():
    """Список всех переписок для администраторов"""
    assignments = get_admin_letter_assignments()
    return render_template('admin/letters.html', assignments=assignments)
