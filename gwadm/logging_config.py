"""Centralized logging setup and helpers."""

import logging
import sys

from gwadm.config import is_debug, is_production

_configured = False
logger = logging.getLogger('gwadm')


def setup_logging() -> None:
    """Configure root logging once based on environment."""
    global _configured
    if _configured:
        return

    level = logging.DEBUG if is_debug() else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        stream=sys.stderr,
        force=True,
    )

    if is_production():
        logging.getLogger('werkzeug').setLevel(logging.WARNING)

    from gwadm.config import warn_insecure_defaults
    warn_insecure_defaults()

    _configured = True


def log_error(msg: str) -> None:
    """Log an error via logger and stderr (visible in journald / PythonAnywhere)."""
    logger.error(msg)
    print(msg, flush=True)


def log_debug(msg: str) -> None:
    """Log debug info; suppressed on production unless debug mode is enabled."""
    if not is_debug():
        return
    logger.debug(msg)
    print(msg, flush=True)
