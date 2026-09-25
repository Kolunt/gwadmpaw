"""GWars login session state and client detection."""

import re

from flask import session

LOGIN_STATE_PENDING = 'pending'
SESSION_KEY_LOGIN_STATE = 'login_state'
SESSION_KEY_RETURN_URL = 'return_url'
LEGACY_SESSION_KEY_AUTH_ATTEMPT = 'gwars_auth_attempt'

_MOBILE_UA_RE = re.compile(r'Mobile|Android|iPhone|iPad|iPod', re.IGNORECASE)


def get_safe_return_url(next_param: str | None, default: str | None = None) -> str | None:
    """Validate return URL: same-origin relative path only."""
    if not next_param:
        return default
    candidate = next_param.strip()
    if not candidate.startswith('/') or candidate.startswith('//'):
        return default
    if '://' in candidate:
        return default
    return candidate


def start_login_flow(return_url: str | None = None) -> None:
    session[SESSION_KEY_LOGIN_STATE] = LOGIN_STATE_PENDING
    if return_url:
        session[SESSION_KEY_RETURN_URL] = return_url
    session.pop(LEGACY_SESSION_KEY_AUTH_ATTEMPT, None)


def clear_login_flow() -> None:
    """Clear pending login state; keeps return_url until successful login."""
    session.pop(SESSION_KEY_LOGIN_STATE, None)
    session.pop(LEGACY_SESSION_KEY_AUTH_ATTEMPT, None)


def clear_login_flow_full() -> None:
    """Clear all login flow session keys."""
    session.pop(SESSION_KEY_LOGIN_STATE, None)
    session.pop(SESSION_KEY_RETURN_URL, None)
    session.pop(LEGACY_SESSION_KEY_AUTH_ATTEMPT, None)


def is_login_pending() -> bool:
    return session.get(SESSION_KEY_LOGIN_STATE) == LOGIN_STATE_PENDING


def pop_return_url_after_login() -> str | None:
    return_url = session.pop(SESSION_KEY_RETURN_URL, None)
    clear_login_flow()
    return return_url


def is_mobile_client(user_agent: str | None) -> bool:
    if not user_agent:
        return False
    return bool(_MOBILE_UA_RE.search(user_agent))


def should_show_mobile_interstitial(user_agent: str | None) -> bool:
    return is_mobile_client(user_agent)


def can_view_auth_debug(session_data, debug_enabled: bool) -> bool:
    if debug_enabled:
        return True
    roles = session_data.get('roles') or []
    return 'admin' in roles
