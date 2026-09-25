#!/usr/bin/env bash
# Production monitoring: health, disk, avatar cache, gunicorn errors.
# Run via systemd timer (gwadm-monitor) or manually: bash scripts/monitor_prod.sh
set -uo pipefail

MONITOR_BASE_URL="${MONITOR_BASE_URL:-http://127.0.0.1:8000}"
MONITOR_PUBLIC_URL="${MONITOR_PUBLIC_URL:-https://gwadm.ru}"
MONITOR_DISK_PATH="${MONITOR_DISK_PATH:-/}"
MONITOR_DISK_WARN_PERCENT="${MONITOR_DISK_WARN_PERCENT:-85}"
MONITOR_DISK_CRIT_PERCENT="${MONITOR_DISK_CRIT_PERCENT:-95}"
MONITOR_AVATAR_WARN_MB="${MONITOR_AVATAR_WARN_MB:-500}"
MONITOR_AVATAR_CRIT_MB="${MONITOR_AVATAR_CRIT_MB:-1000}"
MONITOR_GWADM_DIR="${MONITOR_GWADM_DIR:-${HOME}/gwadm}"
MONITOR_JOURNAL_ERR_WARN="${MONITOR_JOURNAL_ERR_WARN:-5}"
MONITOR_SKIP_PUBLIC="${MONITOR_SKIP_PUBLIC:-0}"

has_critical=0
has_warn=0

log_info() {
    echo "INFO: $*"
}

log_warn() {
    echo "WARN: $*"
    has_warn=1
}

log_critical() {
    echo "CRITICAL: $*"
    has_critical=1
}

check_health() {
    local base_url="$1"
    local label="$2"
    local url="${base_url%/}/health"

    local response
    if ! response="$(curl -sf --max-time 15 "$url" 2>/dev/null)"; then
        log_critical "${label} health check failed: curl error for ${url}"
        return
    fi
    if ! echo "$response" | grep -q '"status":"ok"'; then
        log_critical "${label} health check failed: unexpected response from ${url}"
        return
    fi
    log_info "${label} health OK (${url})"
}

check_disk() {
    if ! command -v df >/dev/null 2>&1; then
        log_info "df not available, skipping disk check"
        return
    fi

    local usage
    usage="$(df -P "$MONITOR_DISK_PATH" 2>/dev/null | awk 'NR==2 {print $5}' | tr -d '%')"
    if [[ -z "$usage" ]]; then
        log_warn "disk check failed: cannot read usage for ${MONITOR_DISK_PATH}"
        return
    fi

    log_info "disk usage on ${MONITOR_DISK_PATH}: ${usage}%"
    if [[ "$usage" -ge "$MONITOR_DISK_CRIT_PERCENT" ]]; then
        log_critical "disk usage ${usage}% >= ${MONITOR_DISK_CRIT_PERCENT}% on ${MONITOR_DISK_PATH}"
    elif [[ "$usage" -ge "$MONITOR_DISK_WARN_PERCENT" ]]; then
        log_warn "disk usage ${usage}% >= ${MONITOR_DISK_WARN_PERCENT}% on ${MONITOR_DISK_PATH}"
    fi
}

check_dir_info_mb() {
    local dir="$1"
    local label="$2"

    if [[ ! -d "$dir" ]]; then
        log_info "${label}: directory not found (${dir}), skipping"
        return
    fi
    if ! command -v du >/dev/null 2>&1; then
        log_info "du not available, skipping ${label} size check"
        return
    fi

    local size_mb
    size_mb="$(du -sm "$dir" 2>/dev/null | awk '{print $1}')"
    if [[ -z "$size_mb" ]]; then
        log_warn "${label}: cannot measure size for ${dir}"
        return
    fi
    log_info "${label}: ${size_mb} MB (${dir})"
}

check_dir_size_mb() {
    local dir="$1"
    local label="$2"
    local warn_mb="$3"
    local crit_mb="$4"

    if [[ ! -d "$dir" ]]; then
        log_info "${label}: directory not found (${dir}), skipping"
        return
    fi

    if ! command -v du >/dev/null 2>&1; then
        log_info "du not available, skipping ${label} size check"
        return
    fi

    local size_mb
    size_mb="$(du -sm "$dir" 2>/dev/null | awk '{print $1}')"
    if [[ -z "$size_mb" ]]; then
        log_warn "${label}: cannot measure size for ${dir}"
        return
    fi

    log_info "${label}: ${size_mb} MB (${dir})"
    if [[ "$size_mb" -ge "$crit_mb" ]]; then
        log_critical "${label} size ${size_mb} MB >= ${crit_mb} MB"
    elif [[ "$size_mb" -ge "$warn_mb" ]]; then
        log_warn "${label} size ${size_mb} MB >= ${warn_mb} MB"
    fi
}

check_gunicorn_errors() {
    if ! command -v journalctl >/dev/null 2>&1; then
        log_info "journalctl not available, skipping gunicorn error check"
        return
    fi

    local err_count
    err_count="$(journalctl --user -u gwadm -p err --since "1 hour ago" --no-pager 2>/dev/null | wc -l | tr -d ' ')"
    log_info "gunicorn errors in last hour: ${err_count}"
    if [[ "$err_count" -ge "$MONITOR_JOURNAL_ERR_WARN" ]]; then
        log_warn "gunicorn error count ${err_count} >= ${MONITOR_JOURNAL_ERR_WARN} in the last hour"
    fi
}

check_health "$MONITOR_BASE_URL" "local"
if [[ "$MONITOR_SKIP_PUBLIC" != "1" ]]; then
    check_health "$MONITOR_PUBLIC_URL" "public"
fi
check_disk
check_dir_size_mb \
    "${MONITOR_GWADM_DIR}/static/uploads/avatars/cache" \
    "avatar cache" \
    "$MONITOR_AVATAR_WARN_MB" \
    "$MONITOR_AVATAR_CRIT_MB"
check_dir_info_mb "${MONITOR_GWADM_DIR}/backups" "backups"
check_gunicorn_errors

if [[ "$has_critical" -eq 1 ]]; then
    exit 1
fi
if [[ "$has_warn" -eq 1 ]]; then
    exit 2
fi
exit 0
