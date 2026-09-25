#!/usr/bin/env bash
# Post-deploy checklist for production (run on server after git pull + restart).
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
NGINX_URL="${NGINX_URL:-http://127.0.0.1}"
APP_DIR="${APP_DIR:-$HOME/gwadm}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"
PYTHON="${PYTHON:-$APP_DIR/venv/bin/python}"
MAX_RETRIES="${MAX_RETRIES:-15}"
RETRY_DELAY="${RETRY_DELAY:-2}"

if [[ ! -x "${PYTHON}" ]]; then
  PYTHON=python3
fi

expected_version="$("${PYTHON}" -c "import sys; sys.path.insert(0, '${APP_DIR}'); from version import __version__; print(__version__)")"

echo "Expected version: ${expected_version}"

if [[ -f "${ENV_FILE}" ]]; then
  echo "Checking GWARS_PASSWORD in ${ENV_FILE}"
  grep -q '^GWARS_PASSWORD=.' "${ENV_FILE}"
else
  echo "WARN: ${ENV_FILE} not found, skipping GWARS_PASSWORD check"
fi

echo "Checking gwadm service is active"
if ! systemctl --user is-active --quiet gwadm; then
  echo "FAIL: gwadm service is not active"
  systemctl --user status gwadm --no-pager || true
  exit 1
fi

linger="$(loginctl show-user "$(whoami)" -p Linger --value 2>/dev/null || echo unknown)"
echo "User linger: ${linger}"
if [[ "${linger}" != "yes" ]]; then
  echo "WARN: linger is not enabled — gwadm may stop when SSH session ends (sudo loginctl enable-linger $(whoami))"
fi

health_ok=0
for attempt in $(seq 1 "${MAX_RETRIES}"); do
  if curl -sf "${BASE_URL}/health" | grep -q '"status":"ok"'; then
    health_ok=1
    break
  fi
  echo "Waiting for ${BASE_URL}/health (attempt ${attempt}/${MAX_RETRIES})..."
  sleep "${RETRY_DELAY}"
done

if [[ "${health_ok}" -ne 1 ]]; then
  echo "FAIL: ${BASE_URL}/health not ready"
  journalctl --user -u gwadm -n 30 --no-pager || true
  exit 1
fi

health_json="$(curl -sf "${BASE_URL}/health")"
echo "Health: ${health_json}"

if ! echo "${health_json}" | grep -q "\"version\":\"${expected_version}\""; then
  echo "FAIL: /health version does not match version.py (${expected_version})"
  exit 1
fi

echo "Checking nginx proxy ${NGINX_URL}/health"
nginx_body="$(curl -sf "${NGINX_URL}/health")"
if ! echo "${nginx_body}" | grep -q '"status":"ok"'; then
  echo "FAIL: nginx proxy health check failed"
  exit 1
fi
if ! echo "${nginx_body}" | grep -q "\"version\":\"${expected_version}\""; then
  echo "FAIL: nginx /health version mismatch"
  exit 1
fi

echo "Checking ${BASE_URL}/login"
curl -sfI "${BASE_URL}/login" | head -n1 | grep -q '200'

echo "Checking ${BASE_URL}/login/go redirect"
curl -sfI "${BASE_URL}/login/go" | grep -qi 'location:.*gwars.io'

echo "Checking mobile interstitial"
curl -sfI "${BASE_URL}/login/go" -H "User-Agent: iPhone" | head -n1 | grep -q '200'

echo "Checking cron without token"
curl -sI "${BASE_URL}/cron/run" | head -n1 | grep -q '401'

echo "OK: post_deploy_smoke passed (version ${expected_version})"
