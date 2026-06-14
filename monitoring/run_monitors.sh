#!/bin/bash
# run_monitors.sh — independent monitoring wrapper (additive, 2026-06-14)
#
# Runs alongside (NOT inside) the daily-pipeline. Invoked by launchd
# com.miratuku.content-freshness-monitor at 08:30 JST — right after the daily-pipeline
# healthcheck (H5, 08:00) so the content-date SLI and the created_at-based H5 are visible
# together each morning.
#
# Two responsibilities (both additive, both read-only w.r.t. pipeline DBs):
#   1. venv-loss watchdog (C): verify the app's dedicated .venv can import collect_news deps.
#      This is what was silently missing since 2026-04-24 (~/.venv vanished). If broken,
#      alert #事務 once. We do NOT auto-rebuild here (daily_update.sh preflight owns repair);
#      this is the early-warning sensor so loss is caught before the next 04:00 run.
#   2. content freshness SLI (A): published_date-based two-track freshness monitor.
#
# Kill switch: CONTENT_FRESHNESS_ALERTS=0 disables Slack POSTs (still logged locally).

set -u

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MON_DIR="$PROJECT_DIR/monitoring"
VENV_DIR="$PROJECT_DIR/.venv"
VENV_PY="$VENV_DIR/bin/python3"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/monitor_$(date +%Y-%m-%d).log"
mkdir -p "$LOG_DIR" "$MON_DIR/state"

# Load .env (for CONTENT_FRESHNESS_ALERTS kill switch, etc.)
if [ -f "$PROJECT_DIR/.env" ]; then set -a; source "$PROJECT_DIR/.env"; set +a; fi

# Prefer the dedicated venv, fall back to Homebrew python.
export PATH="$VENV_DIR/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

slack_alert() {
  local msg="$1"
  if [ "${CONTENT_FRESHNESS_ALERTS:-1}" = "0" ]; then
    log "  (Slack alert suppressed by CONTENT_FRESHNESS_ALERTS=0): $msg"; return 0
  fi
  local webhook
  webhook=$(security find-generic-password -s SLACK_WEBHOOK_PIPELINE -w 2>/dev/null || true)
  if [ -z "$webhook" ]; then
    log "  (no SLACK_WEBHOOK_PIPELINE in keychain): $msg"; return 0
  fi
  local body
  body=$(MSG="$msg" python3 - <<'PYEOF' 2>/dev/null
import json, os
print(json.dumps({"text": os.environ.get("MSG", "")}))
PYEOF
)
  [ -z "$body" ] && body="{\"text\":\"future-insight monitor alert\"}"
  curl -s -X POST -H 'Content-type: application/json' --data "$body" "$webhook" >/dev/null 2>&1 \
    && log "  Slack notified (#事務)." || log "  WARNING: Slack POST failed."
}

log "=== future-insight monitors ($(date '+%Y-%m-%d %H:%M')) ==="

# --- (C) venv-loss watchdog -------------------------------------------------
log "venv watchdog: checking $VENV_PY can import feedparser+requests..."
if [ -x "$VENV_PY" ] && "$VENV_PY" -c "import feedparser, requests" >/dev/null 2>&1; then
  log "  venv OK (collect_news deps importable)."
else
  log "  [WARN] dedicated venv missing or broken (feedparser/requests not importable)."
  slack_alert "[future-insight] venv watchdog: dedicated .venv is missing/broken — collect_news will fail at next 04:00 run unless rebuilt. The 04:00 daily_update.sh preflight will attempt one auto-rebuild; if it still fails the morning digest will report it. Fix: cd $PROJECT_DIR && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
fi

# --- (A) content freshness SLI ----------------------------------------------
log "content freshness SLI: running 09_content_freshness_check.py..."
python3 "$MON_DIR/09_content_freshness_check.py" >> "$LOG_FILE" 2>&1
log "  freshness check done (rc=$?)."

# Cleanup old monitor logs (keep 30 days)
find "$LOG_DIR" -name "monitor_*.log" -mtime +30 -delete 2>/dev/null || true

log "=== monitors complete ==="
