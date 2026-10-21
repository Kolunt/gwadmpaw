"""Route smoke via scripts/verify_web_routes.py."""

from scripts.verify_web_routes import run_checks


def test_verify_web_routes_passes():
    errors, ok = run_checks()
    assert not errors, '\n'.join(errors)
    assert ok >= 50
