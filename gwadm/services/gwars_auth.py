"""GWars session finalization after successful login."""

from flask import session

from gwadm.config import ADMIN_USER_IDS
from gwadm.logging_config import log_debug
from gwadm.services.activity import log_activity
from gwadm.services.gwars_signatures import verify_sign, verify_sign2, verify_sign3, verify_sign4
from gwadm.services.roles import assign_role, get_user_role_names, get_user_roles, has_role

__all__ = [
    'finalize_user_login',
    'verify_sign',
    'verify_sign2',
    'verify_sign3',
    'verify_sign4',
]


def finalize_user_login(user_id, name, level, synd, source='gwars', details=None):
    """Назначает роли, заполняет session и пишет activity log после успешного входа."""
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        user_id_int = user_id

    if user_id_int in ADMIN_USER_IDS:
        if not has_role(user_id, 'admin'):
            assign_role(user_id, 'admin', assigned_by=user_id)
            log_debug(f"Admin role automatically assigned to user_id {user_id}")

    if not get_user_roles(user_id):
        assign_role(user_id, 'user', assigned_by=user_id)
        log_debug(f"Default 'user' role assigned to user_id {user_id}")

    session['user_id'] = user_id
    session['username'] = name
    session['level'] = level
    session['synd'] = synd
    session['roles'] = get_user_role_names(user_id)
    session.pop('gwars_auth_attempt', None)

    log_activity(
        'login',
        details=details or ('Вход через GWars' if source == 'gwars' else 'Тестовый вход через login_dev'),
        metadata={'source': source, 'user_id': user_id, 'username': name},
    )
