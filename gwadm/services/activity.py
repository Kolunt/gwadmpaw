"""User activity logging."""

import json

from flask import has_request_context, request, session

from gwadm.db import get_db_connection
from gwadm.logging_config import log_error


def log_activity(action, details=None, metadata=None, user_id=None, username=None):
    """Сохраняет информацию о действии пользователя в таблицу activity_logs."""
    if not action:
        return

    conn = None
    try:
        meta_dict = {}
        if metadata:
            if isinstance(metadata, dict):
                meta_dict.update(metadata)
            else:
                meta_dict['data'] = metadata

        ip_address = None
        if has_request_context():
            if user_id is None:
                user_id = session.get('user_id')
            if username is None:
                username = session.get('username')
            ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
            if ip_address and ',' in str(ip_address):
                ip_address = ip_address.split(',')[0].strip()
            meta_dict.setdefault('endpoint', request.endpoint)
            meta_dict.setdefault('path', request.path)
            meta_dict.setdefault('method', request.method)
            impersonation_original = session.get('impersonation_original')
            if impersonation_original:
                meta_dict.setdefault('impersonator_id', impersonation_original.get('user_id'))
                meta_dict.setdefault('impersonator_username', impersonation_original.get('username'))

        metadata_json = json.dumps(meta_dict, ensure_ascii=False) if meta_dict else None

        conn = get_db_connection()
        conn.execute('''
            INSERT INTO activity_logs (user_id, username, action, details, metadata, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, username, action, details, metadata_json, ip_address))
        conn.commit()
    except Exception as e:
        log_error(f"Error logging activity '{action}': {e}")
    finally:
        if conn:
            conn.close()
