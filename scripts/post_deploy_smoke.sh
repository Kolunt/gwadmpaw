#!/usr/bin/env bash
# Post-deploy curl checklist for production (run on server).
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

echo "Checking ${BASE_URL}/health"
curl -sf "${BASE_URL}/health" | grep -q '"status":"ok"'

echo "Checking ${BASE_URL}/login"
curl -sfI "${BASE_URL}/login" | head -n1 | grep -q '200'

echo "Checking ${BASE_URL}/login/go redirect"
curl -sfI "${BASE_URL}/login/go" | grep -qi 'location:.*gwars.io'

echo "Checking mobile interstitial"
curl -sfI "${BASE_URL}/login/go" -H "User-Agent: iPhone" | head -n1 | grep -q '200'

echo "Checking cron without token"
curl -sfI "${BASE_URL}/cron/run" | head -n1 | grep -q '401'

echo "OK: post_deploy_smoke passed"
