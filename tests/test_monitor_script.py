"""Tests for scripts/monitor_prod.sh contract."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / 'scripts' / 'monitor_prod.sh'


def _bash_available() -> str | None:
    bash = shutil.which('bash')
    if not bash:
        return None
    try:
        probe = subprocess.run(
            [bash, '--version'],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if probe.returncode != 0:
        return None
    return bash


BASH = _bash_available()

pytestmark = pytest.mark.skipif(not BASH, reason='working bash required for monitor_prod.sh tests')


def _run_monitor(env: dict | None = None) -> subprocess.CompletedProcess:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(
        [BASH, str(SCRIPT)],
        cwd=ROOT,
        env=merged,
        capture_output=True,
        text=True,
        check=False,
    )


def test_monitor_script_syntax():
    result = subprocess.run(
        [BASH, '-n', str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_monitor_fails_when_local_health_unreachable():
    result = _run_monitor({
        'MONITOR_BASE_URL': 'http://127.0.0.1:9',
        'MONITOR_SKIP_PUBLIC': '1',
    })
    assert result.returncode == 1
    assert 'CRITICAL' in result.stdout
