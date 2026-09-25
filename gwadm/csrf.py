"""Lightweight CSRF protection for form posts and fetch/XHR."""

import secrets
from functools import wraps

from flask import flash, jsonify, redirect, request, session, url_for

SESSION_CSRF_KEY = '_csrf_token'
CSRF_FORM_FIELD = '_csrf_token'
CSRF_HEADER = 'X-CSRF-Token'

_EXEMPT_ENDPOINTS: set[str] = set()


def csrf_exempt(view):
    """Mark a view as exempt from CSRF validation."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        return view(*args, **kwargs)

    wrapped._csrf_exempt = True
    return wrapped


def generate_csrf_token() -> str:
    token = session.get(SESSION_CSRF_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[SESSION_CSRF_KEY] = token
    return token


def validate_csrf_token(token: str | None) -> bool:
    expected = session.get(SESSION_CSRF_KEY)
    if not expected or not token:
        return False
    return secrets.compare_digest(expected, token)


def _get_submitted_csrf_token() -> str | None:
    token = request.headers.get(CSRF_HEADER)
    if token:
        return token.strip()
    if request.form:
        form_token = request.form.get(CSRF_FORM_FIELD)
        if form_token:
            return form_token.strip()
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        if isinstance(payload, dict):
            json_token = payload.get(CSRF_FORM_FIELD)
            if json_token:
                return str(json_token).strip()
    return None


def _wants_json_response() -> bool:
    if request.path.startswith('/api/'):
        return True
    accept = request.headers.get('Accept', '')
    if 'application/json' in accept:
        return True
    return request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest'


def init_csrf(app) -> None:
    app.jinja_env.globals['csrf_token'] = generate_csrf_token

    @app.context_processor
    def inject_csrf():
        return {'csrf_token': generate_csrf_token}

    @app.before_request
    def csrf_protect():
        if request.method not in ('POST', 'PUT', 'PATCH', 'DELETE'):
            return None

        endpoint = request.endpoint or ''
        view_func = app.view_functions.get(endpoint)
        if view_func is not None and getattr(view_func, '_csrf_exempt', False):
            return None
        if endpoint in _EXEMPT_ENDPOINTS:
            return None

        if not validate_csrf_token(_get_submitted_csrf_token()):
            if _wants_json_response():
                return jsonify({'success': False, 'error': 'CSRF validation failed'}), 400
            flash('Ошибка безопасности (CSRF). Обновите страницу и попробуйте снова.', 'error')
            return redirect(request.referrer or url_for('public.index'))

        return None


def register_csrf_exempt(endpoint: str) -> None:
    _EXEMPT_ENDPOINTS.add(endpoint)
