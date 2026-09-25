"""Auth decorators for routes and blueprints."""

from functools import wraps

from flask import flash, redirect, request, session, url_for

from gwadm.services.roles import has_any_role, has_role


def require_role(role_name):
    """Декоратор для проверки наличия роли у пользователя."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = session.get('user_id')
            if not has_role(user_id, role_name):
                if not user_id:
                    flash('Для доступа к этой странице необходимо авторизоваться', 'error')
                    return redirect(url_for('auth.login', next=request.path))
                flash('У вас нет прав для доступа к этой странице', 'error')
                return redirect(url_for('profile.dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_any_role(*role_names):
    """Декоратор для проверки наличия хотя бы одной из ролей."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = session.get('user_id')
            if not has_any_role(user_id, role_names):
                if not user_id:
                    flash('Для доступа к этой странице необходимо авторизоваться', 'error')
                    return redirect(url_for('auth.login', next=request.path))
                flash('У вас нет прав для доступа к этой странице', 'error')
                return redirect(url_for('profile.dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_login(f):
    """Декоратор для проверки авторизации."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Для доступа к этой странице необходимо авторизоваться', 'error')
            return redirect(url_for('auth.login', next=request.path))
        return f(*args, **kwargs)
    return decorated_function
