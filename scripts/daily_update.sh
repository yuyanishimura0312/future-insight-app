#!/bin/bash
# daily_update.sh — Collect news, run AI analysis, and deploy to GitHub Pages
# Runs as a daily cron/launchd job
# TODO: Change launchd schedule to 4:00 AM JST for miratuku-news pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/daily_$(date +%Y-%m-%d).log"

mkdir -p "$LOG_DIR"

# Load environment variables
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  source "$PROJECT_DIR/.env"
  set +a
fi

# --- Dedicated virtualenv (additive 2026-06-14) ---
# Previously this pointed at the shared ~/.venv, which vanished on 2026-04-24 and silently
# broke collect_news.py (feedparser dependency) every morning. We now use this app's OWN
# .venv so the daily pipeline is isolated from shared-venv loss. requirements.txt is the
# single source of truth; the preflight below rebuilds it if it is missing/broken.
VENV_DIR="$PROJECT_DIR/.venv"
VENV_PY="$VENV_DIR/bin/python3"

# Ensure PATH prefers the app venv, then Homebrew python (fallback if venv is unavailable).
export PATH="$VENV_DIR/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

# Get API key from macOS keychain if not already set
if [ -z "$ANTHROPIC_API_KEY" ]; then
  ANTHROPIC_API_KEY=$(security find-generic-password -s ANTHROPIC_API_KEY -w 2>/dev/null)
  export ANTHROPIC_API_KEY
fi

log() {
  echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# --- Slack alert helper (additive 2026-06-14) ---
# Reuses the existing keychain webhook (SLACK_WEBHOOK_PIPELINE) and posts to #事務.
# Kill switch: set DAILY_UPDATE_ALERTS=0 (env or .env) to disable all Slack notifications.
slack_alert() {
  local msg="$1"
  if [ "${DAILY_UPDATE_ALERTS:-1}" = "0" ]; then
    log "  (Slack alert suppressed by DAILY_UPDATE_ALERTS=0): $msg"
    return 0
  fi
  local webhook
  webhook=$(security find-generic-password -s SLACK_WEBHOOK_PIPELINE -w 2>/dev/null || true)
  if [ -z "$webhook" ]; then
    log "  (no SLACK_WEBHOOK_PIPELINE in keychain; alert not sent): $msg"
    return 0
  fi
  # Build JSON safely (escape quotes/newlines) via python in the active interpreter.
  local body
  body=$(MSG="$msg" python3 - <<'PYEOF' 2>/dev/null
import json, os
print(json.dumps({"text": os.environ.get("MSG", "")}))
PYEOF
)
  [ -z "$body" ] && body="{\"text\":\"future-insight daily pipeline alert\"}"
  curl -s -X POST -H 'Content-type: application/json' --data "$body" "$webhook" >/dev/null 2>&1 \
    && log "  Slack notified (#事務)." \
    || log "  WARNING: Slack notification failed to POST."
}

# --- Preflight: dependency self-heal (additive 2026-06-14) ---
# Mirrors the daily-pipeline healthcheck H4 self-repair philosophy. If feedparser/requests
# (the collect_news critical deps) cannot be imported, rebuild the venv ONCE from
# requirements.txt. If it still fails, alert #事務 and mark collect_news as skip-able so the
# rest of the pipeline (AI analysis, historical, reports) continues instead of dying silently.
PREFLIGHT_OK=1
preflight_check() {
  python3 -c "import feedparser, requests" >/dev/null 2>&1
}
log "=== Future Insight Daily Update ==="
log "Date: $(date '+%Y-%m-%d %H:%M')"
log "Preflight: checking collect_news dependencies (feedparser, requests)..."
if preflight_check; then
  log "  Preflight OK (interpreter: $(command -v python3))."
else
  log "  [WARN] feedparser/requests not importable. Attempting venv rebuild (1 try)..."
  # Rebuild the dedicated venv from requirements.txt.
  /opt/homebrew/bin/python3 -m venv "$VENV_DIR" >> "$LOG_FILE" 2>&1 || \
    python3 -m venv "$VENV_DIR" >> "$LOG_FILE" 2>&1 || true
  if [ -x "$VENV_PY" ]; then
    "$VENV_PY" -m pip install --quiet --upgrade pip >> "$LOG_FILE" 2>&1 || true
    "$VENV_PY" -m pip install --quiet -r "$PROJECT_DIR/requirements.txt" >> "$LOG_FILE" 2>&1 || true
    # Re-prefer the freshly built venv on PATH.
    export PATH="$VENV_DIR/bin:$PATH"
  fi
  if preflight_check; then
    log "  Preflight self-heal succeeded (venv rebuilt)."
    slack_alert "[future-insight] daily pipeline: collect_news deps were missing; venv auto-rebuilt and recovered. ($(date '+%Y-%m-%d %H:%M'))"
  else
    PREFLIGHT_OK=0
    log "  [ERROR] Preflight self-heal FAILED. collect_news will be SKIPPED this run (historical/AI steps continue)."
    slack_alert "[future-insight] daily pipeline: collect_news deps STILL missing after venv rebuild. Current-news collection SKIPPED today; needs manual fix (python3 -m venv .venv && .venv/bin/pip install -r requirements.txt). ($(date '+%Y-%m-%d %H:%M'))"
  fi
fi

cd "$SCRIPT_DIR"

# Report run start to Firestore (non-fatal)
python3 report_status.py --run-start 2>/dev/null || true
PIPELINE_START=$(date +%s)

# --- Step outcome accumulators (additive 2026-06-14) ---
# These collect per-run success/failure so the end-of-run summary can post ONE Slack
# digest (not one alert per step). The existing run_step logic is unchanged; we only
# append bookkeeping after each branch.
STEP_SUCCESS_COUNT=0
STEP_FAIL_COUNT=0
STEP_SKIP_COUNT=0
FAILED_STEPS=""
SKIPPED_STEPS=""

# Helper: run a step with timing and Firestore reporting
run_step() {
  local STEP_NAME="$1"
  local SCRIPT="$2"
  local STEP_START=$(date +%s)
  log "  Running $STEP_NAME ($SCRIPT)..."
  if python3 "$SCRIPT" >> "$LOG_FILE" 2>&1; then
    local STEP_DURATION=$(($(date +%s) - STEP_START))
    log "  $STEP_NAME done. (${STEP_DURATION}s)"
    python3 report_status.py --step "$STEP_NAME" --status success --duration "$STEP_DURATION" 2>/dev/null || true
    STEP_SUCCESS_COUNT=$((STEP_SUCCESS_COUNT + 1))
  else
    local RC=$?
    local STEP_DURATION=$(($(date +%s) - STEP_START))
    log "  [ERROR] $STEP_NAME failed! (${STEP_DURATION}s)"
    python3 report_status.py --step "$STEP_NAME" --status error --duration "$STEP_DURATION" --error "Exit code $RC" 2>/dev/null || true
    STEP_FAIL_COUNT=$((STEP_FAIL_COUNT + 1))
    FAILED_STEPS="$FAILED_STEPS $STEP_NAME"
  fi
}

# Helper: mark a step as deliberately skipped (e.g. collect_news when deps unrepairable).
mark_skipped() {
  local STEP_NAME="$1"
  local REASON="$2"
  log "  [SKIP] $STEP_NAME skipped: $REASON"
  STEP_SKIP_COUNT=$((STEP_SKIP_COUNT + 1))
  SKIPPED_STEPS="$SKIPPED_STEPS $STEP_NAME"
}

# Step 1: Collect PESTLE news (2000 articles/day)
log "Step 1: Collecting PESTLE news..."
if [ "$PREFLIGHT_OK" -eq 1 ]; then
  run_step "collect_news" "collect_news.py"
else
  mark_skipped "collect_news" "preflight dependency self-heal failed (feedparser/requests unavailable)"
fi

# Step 1.5: Collect academic papers (1000 papers/day)
log "Step 1.5: Collecting academic papers..."
run_step "collect_papers" "collect_papers.py"

# Step 2: AI analysis
log "Step 2: Running AI analysis..."
run_step "ai_analyze" "ai_analyze.py"

# Step 2.3: Generate daily CLA report
log "Step 2.3: Generating daily report..."
run_step "generate_daily_report" "generate_daily_report.py"

# Step 2.5: Curate daily papers
log "Step 2.5: Curating daily papers..."
run_step "curate_daily_papers" "curate_daily_papers.py"

# Step 2.7: Generate insight reports
log "Step 2.7: Generating insight reports..."
run_step "generate_insight_reports" "generate_insight_reports.py"

# Step 3: Detect alerts
log "Step 3: Detecting alerts..."
run_step "detect_alerts" "detect_alerts.py"

# Step 3.5: Generate scenarios (weekly, on Mondays)
if [ "$(date +%u)" -eq 1 ]; then
  log "Step 3.5: Generating scenarios (weekly)..."
  python3 generate_scenarios.py >> "$LOG_FILE" 2>&1
  log "  Scenario generation done."
fi

# Step 4: Collect historical data
log "Step 4: Collecting historical PESTLE data..."
run_step "collect_historical_daily" "collect_historical_daily.py"

# Step 5: Update history files from database
log "Step 5: Updating history from database..."
run_step "update_history" "update_history.py"

# Step 5.5: Collect historical academic papers & generate field history reports (weekly, on Wednesdays)
# - collect_historical_papers.py: Fetch 500 highly-cited papers (1990-2025) from Semantic Scholar API
# - generate_field_history.py: Generate historical development reports per field using Claude API
# Uncomment to enable:
# if [ "$(date +%u)" -eq 3 ]; then
#   log "Step 5.5a: Collecting historical academic papers..."
#   python3 collect_historical_papers.py >> "$LOG_FILE" 2>&1
#   log "  Historical paper collection done."
#
#   log "Step 5.5b: Generating field history reports..."
#   python3 generate_field_history.py >> "$LOG_FILE" 2>&1
#   log "  Field history reports done."
# fi

# Step 6: Git commit and push
log "Step 6: Deploying to GitHub Pages..."
cd "$PROJECT_DIR"

# Only commit if data files changed
if git diff --quiet data/ 2>/dev/null; then
  log "  No data changes detected. Skipping deploy."
else
  git add data/
  git commit -m "chore: daily PESTLE update $(date +%Y-%m-%d)"
  git push
  log "  Pushed to GitHub Pages."
fi

# Step 7: Sync to PESTLE + Signal DB
log "Step 7: Syncing to PESTLE + Signal DB..."
if python3 ~/projects/research/pestle-signal-db/scripts/daily_sync.py >> "$LOG_FILE" 2>&1; then
  log "  PESTLE + Signal DB sync complete."
else
  log "  WARNING: PESTLE + Signal DB sync failed (non-fatal)."
  STEP_FAIL_COUNT=$((STEP_FAIL_COUNT + 1))
  FAILED_STEPS="$FAILED_STEPS pestle_signal_sync"
fi

# Cleanup old logs (keep 30 days)
find "$LOG_DIR" -name "daily_*.log" -mtime +30 -delete 2>/dev/null || true

# --- Aggregate summary alert (additive 2026-06-14 / silent-failure surfacing) ---
# run_step writes failures to the log but otherwise continues (set -e is intentionally
# not propagated through run_step). Previously a step could fail every morning with nobody
# noticing. We now post ONE daily Slack digest to #事務 — but only when there is at least
# one failure or skip (success-only runs stay silent to avoid alert fatigue).
# Kill switch: DAILY_UPDATE_ALERTS=0 disables the Slack POST (still logged locally).
log "--- Run summary: ${STEP_SUCCESS_COUNT} ok / ${STEP_FAIL_COUNT} failed / ${STEP_SKIP_COUNT} skipped ---"
if [ "$STEP_FAIL_COUNT" -gt 0 ] || [ "$STEP_SKIP_COUNT" -gt 0 ]; then
  SUMMARY="[future-insight] daily pipeline $(date '+%Y-%m-%d'): ${STEP_SUCCESS_COUNT} ok / ${STEP_FAIL_COUNT} failed / ${STEP_SKIP_COUNT} skipped."
  [ -n "$FAILED_STEPS" ]  && SUMMARY="$SUMMARY  Failed:$FAILED_STEPS."
  [ -n "$SKIPPED_STEPS" ] && SUMMARY="$SUMMARY  Skipped:$SKIPPED_STEPS."
  SUMMARY="$SUMMARY  Log: $LOG_FILE"
  log "  $SUMMARY"
  slack_alert "$SUMMARY"
else
  log "  All steps succeeded; no Slack digest sent (success-only runs stay quiet)."
fi

# Report run completion and upload log to Firestore
PIPELINE_DURATION=$(($(date +%s) - PIPELINE_START))
python3 report_status.py --run-end --status success --duration "$PIPELINE_DURATION" 2>/dev/null || true
python3 report_status.py --upload-log "$LOG_FILE" 2>/dev/null || true

log "=== Daily update complete (${PIPELINE_DURATION}s) ==="
