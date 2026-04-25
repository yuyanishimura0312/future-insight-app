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

# Ensure PATH includes Homebrew python and venv
export PATH="/Users/nishimura+/.venv/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

# Get API key from macOS keychain if not already set
if [ -z "$ANTHROPIC_API_KEY" ]; then
  ANTHROPIC_API_KEY=$(security find-generic-password -s ANTHROPIC_API_KEY -w 2>/dev/null)
  export ANTHROPIC_API_KEY
fi

log() {
  echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

cd "$SCRIPT_DIR"

log "=== Future Insight Daily Update ==="
log "Date: $(date '+%Y-%m-%d %H:%M')"

# Report run start to Firestore (non-fatal)
python3 report_status.py --run-start 2>/dev/null || true
PIPELINE_START=$(date +%s)

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
  else
    local STEP_DURATION=$(($(date +%s) - STEP_START))
    log "  [ERROR] $STEP_NAME failed! (${STEP_DURATION}s)"
    python3 report_status.py --step "$STEP_NAME" --status error --duration "$STEP_DURATION" --error "Exit code $?" 2>/dev/null || true
  fi
}

# Step 1: Collect PESTLE news (2000 articles/day)
log "Step 1: Collecting PESTLE news..."
run_step "collect_news" "collect_news.py"

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
fi

# Cleanup old logs (keep 30 days)
find "$LOG_DIR" -name "daily_*.log" -mtime +30 -delete 2>/dev/null || true

# Report run completion and upload log to Firestore
PIPELINE_DURATION=$(($(date +%s) - PIPELINE_START))
python3 report_status.py --run-end --status success --duration "$PIPELINE_DURATION" 2>/dev/null || true
python3 report_status.py --upload-log "$LOG_FILE" 2>/dev/null || true

log "=== Daily update complete (${PIPELINE_DURATION}s) ==="
