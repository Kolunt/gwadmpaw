#!/usr/bin/env bash
# Summarize gwadm journal errors for post-refactor triage (default: last 120 hours).
# Run on production: bash scripts/audit_prod_errors.sh
set -uo pipefail

AUDIT_HOURS="${AUDIT_HOURS:-120}"
AUDIT_UNIT="${AUDIT_UNIT:-gwadm}"
TOP_N="${TOP_N:-25}"
RAW_TAIL="${RAW_TAIL:-20}"

if ! command -v journalctl >/dev/null 2>&1; then
  echo "journalctl not found; run this script on the server with systemd user journal."
  exit 0
fi

SINCE="${AUDIT_HOURS} hours ago"
echo "=== gwadm error audit ==="
echo "Unit: ${AUDIT_UNIT}"
echo "Since: ${SINCE}"
echo ""

mapfile -t lines < <(journalctl --user -u "${AUDIT_UNIT}" --since "${SINCE}" --no-pager 2>/dev/null || true)
total="${#lines[@]}"
echo "Journal lines (all priorities): ${total}"
echo ""

if [[ "$total" -eq 0 ]]; then
  echo "No journal entries in this window."
  exit 0
fi

journal_text="$(printf '%s\n' "${lines[@]}")"

count_pattern() {
  local label="$1"
  local pattern="$2"
  local n
  n="$(printf '%s\n' "${lines[@]}" | grep -Eci "${pattern}" || true)"
  echo "${label}: ${n}"
  if [[ "${n}" -gt 0 ]]; then
    printf '%s\n' "${lines[@]}" | grep -Ei "${pattern}" | head -n 1 | sed "s/^/  example: /"
  fi
}

echo "--- Pattern summary ---"
count_pattern "Error creating/updating/deleting" 'Error (creating|updating|deleting)'
count_pattern "Traceback" 'Traceback'
count_pattern "NameError" 'NameError'
count_pattern "ImportError" 'ImportError'
count_pattern "AttributeError" 'AttributeError'
count_pattern "Unhandled exception" 'Unhandled exception'
count_pattern "HTTP 500 / internal" '( 500 |Internal Server Error|internal error)'
echo ""

echo "--- Top ${TOP_N} normalized signatures (ERROR / Unhandled / Traceback tail) ---"
printf '%s\n' "${lines[@]}" \
  | grep -E 'ERROR \[gwadm\]|Unhandled exception|NameError|ImportError|AttributeError' \
  | sed -E 's/[0-9]{4,}/<id>/g; s/user_id[=: ][0-9]+/user_id=<id>/gi; s/event_id[=: ][0-9]+/event_id=<id>/gi' \
  | sort \
  | uniq -c \
  | sort -rn \
  | head -n "${TOP_N}"
echo ""

echo "--- Last ${RAW_TAIL} lines matching Error (raw) ---"
printf '%s\n' "${lines[@]}" | grep -E 'Error |ERROR \[gwadm\]' | tail -n "${RAW_TAIL}" || echo "(none)"
echo ""
echo "=== end audit (informational, exit 0) ==="
exit 0
