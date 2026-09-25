"""User roles assignment and lookup."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_error
from gwadm.services.activity import log_activity


def get_user_roles(user_id):
    """Получает список ролей пользователя."""
    if not user_id:
        return []
    conn = get_db_connection()
    roles = conn.execute('''
        SELECT r.id, r.name, r.display_name, r.description
        FROM roles r
        INNER JOIN user_roles ur ON r.id = ur.role_id
        WHERE ur.user_id = ?
    ''', (user_id,)).fetchall()
    conn.close()
    return [dict(role) for role in roles]


def get_user_role_names(user_id):
    """Получает список имен ролей пользователя."""
    if not user_id:
        return ['guest']
    roles = get_user_roles(user_id)
    return [role['name'] for role in roles] if roles else ['user']


def has_role(user_id, role_name):
    """Проверяет, есть ли у пользователя указанная роль."""
    if not user_id:
        return role_name == 'guest'
    role_names = get_user_role_names(user_id)
    return role_name in role_names


def has_any_role(user_id, role_names):
    """Проверяет, есть ли у пользователя хотя бы одна из указанных ролей."""
    if not user_id:
        return 'guest' in role_names
    user_roles = get_user_role_names(user_id)
    return any(role in user_roles for role in role_names)


def assign_role(user_id, role_name, assigned_by=None):
    """Назначает роль пользователю."""
    conn = get_db_connection()
    role = conn.execute('SELECT id FROM roles WHERE name = ?', (role_name,)).fetchone()
    if not role:
        conn.close()
        return False

    try:
        conn.execute('''
            INSERT OR REPLACE INTO user_roles (user_id, role_id, assigned_by)
            VALUES (?, ?, ?)
        ''', (user_id, role['id'], assigned_by))
        conn.commit()
        log_activity(
            'role_assign',
            details=f'Назначена роль {role_name} пользователю {user_id}',
            metadata={'target_user_id': user_id, 'role': role_name, 'assigned_by': assigned_by},
            user_id=assigned_by,
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning role: {e}")
        conn.close()
        return False


def remove_role(user_id, role_name):
    """Удаляет роль у пользователя."""
    conn = get_db_connection()
    role = conn.execute('SELECT id FROM roles WHERE name = ?', (role_name,)).fetchone()
    if not role:
        conn.close()
        return False

    try:
        conn.execute('''
            DELETE FROM user_roles
            WHERE user_id = ? AND role_id = ?
        ''', (user_id, role['id']))
        conn.commit()
        log_activity(
            'role_remove',
            details=f'Удалена роль {role_name} у пользователя {user_id}',
            metadata={'target_user_id': user_id, 'role': role_name},
        )
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing role: {e}")
        conn.close()
        return False
