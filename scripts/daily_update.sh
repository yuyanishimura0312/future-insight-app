#!/bin/bash
# daily_update.sh — Collect news, run AI analysis, and deploy to GitHub Pages
# Runs as a daily cron/launchd job

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

# Ensure PATH includes Homebrew python
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

log() {
  echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

cd "$SCRIPT_DIR"

log "=== Future Insight Daily Update ==="
log "Date: $(date '+%Y-%m-%d %H:%M')"

# Step 1: Collect PESTLE news (10 per category)
log "Step 1: Collecting PESTLE news..."
python3 collect_news.py >> "$LOG_FILE" 2>&1
log "  News collection done."

# Step 2: AI analysis (translation + CLA + weak signals)
log "Step 2: Running AI analysis..."
python3 ai_analyze.py >> "$LOG_FILE" 2>&1
log "  AI analysis done."

# Step 3: Detect alerts
log "Step 3: Detecting alerts..."
python3 detect_alerts.py >> "$LOG_FILE" 2>&1
log "  Alert detection done."

# Step 4: Collect historical data (1 year per day, 1900-2025 cycling)
log "Step 4: Collecting historical PESTLE data..."
python3 collect_historical_daily.py >> "$LOG_FILE" 2>&1
log "  Historical collection done."

# Step 5: Update history files from database
log "Step 5: Updating history from database..."
python3 update_history.py >> "$LOG_FILE" 2>&1
log "  History update done."

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

# Cleanup old logs (keep 30 days)
find "$LOG_DIR" -name "daily_*.log" -mtime +30 -delete 2>/dev/null || true

log "=== Daily update complete ==="
