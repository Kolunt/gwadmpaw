"""Role permissions."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
def get_all_permissions():
    """Получает список всех прав"""
    conn = get_db_connection()
    permissions = conn.execute('''
        SELECT * FROM permissions ORDER BY category, display_name
    ''').fetchall()
    conn.close()
    
    # Возвращаем список словарей
    return [dict(perm) for perm in permissions]


def get_role_permissions(role_id):
    """Получает список прав роли"""
    conn = get_db_connection()
    permissions = conn.execute('''
        SELECT p.* FROM permissions p
        INNER JOIN role_permissions rp ON p.id = rp.permission_id
        WHERE rp.role_id = ?
    ''', (role_id,)).fetchall()
    conn.close()
    return [dict(p) for p in permissions]


def assign_permission_to_role(role_id, permission_id):
    """Назначает право роли"""
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT OR IGNORE INTO role_permissions (role_id, permission_id)
            VALUES (?, ?)
        ''', (role_id, permission_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error assigning permission: {e}")
        conn.close()
        return False


def remove_permission_from_role(role_id, permission_id):
    """Удаляет право у роли"""
    conn = get_db_connection()
    try:
        conn.execute('''
            DELETE FROM role_permissions 
            WHERE role_id = ? AND permission_id = ?
        ''', (role_id, permission_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log_error(f"Error removing permission: {e}")
        conn.close()
        return False


def has_permission(user_id, permission_name):
    """Проверяет, есть ли у пользователя указанное право"""
    if not user_id:
        return False
    
    conn = get_db_connection()
    # Получаем роли пользователя
    roles = conn.execute('''
        SELECT r.id FROM roles r
        INNER JOIN user_roles ur ON r.id = ur.role_id
        WHERE ur.user_id = ?
    ''', (user_id,)).fetchall()
    
    if not roles:
        conn.close()
        return False
    
    # Проверяем, есть ли у любой роли пользователя это право
    role_ids = [r['id'] for r in roles]
    placeholders = ','.join(['?'] * len(role_ids))
    
    permission = conn.execute(f'''
        SELECT p.id FROM permissions p
        INNER JOIN role_permissions rp ON p.id = rp.permission_id
        WHERE rp.role_id IN ({placeholders}) AND p.name = ?
    ''', role_ids + [permission_name]).fetchone()
    
    conn.close()
    return permission is not None
