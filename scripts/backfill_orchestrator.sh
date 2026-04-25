#!/bin/bash
# PESTLE Historical Backfill Orchestrator
#
# Splits 2000-2026 into parallel workers, each writing to a separate
# SQLite DB to avoid lock contention, then merges into the main DB.
#
# Architecture:
#   Worker 1: 2000-2004 → data/backfill_2000_2004.db
#   Worker 2: 2005-2009 → data/backfill_2005_2009.db
#   Worker 3: 2010-2014 → data/backfill_2010_2014.db
#   Worker 4: 2015-2019 → data/backfill_2015_2019.db
#   Worker 5: 2020-2024 → data/backfill_2020_2024.db
#   Worker 6: 2025-2026 → data/backfill_2025_2026.db
#   → merge_backfill.py → future_insight.db + pestle.db
#
# Usage:
#   ./backfill_orchestrator.sh          # Launch all workers
#   ./backfill_orchestrator.sh --dry-run # Preview mode
#   ./backfill_orchestrator.sh --status  # Check progress

set -e
cd "$(dirname "$0")/.."

SCRIPT_DIR="scripts"
DATA_DIR="data"
LOG_DIR="logs/backfill"

mkdir -p "$LOG_DIR"

DRY_RUN=""
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN="--dry-run"
    echo "=== DRY RUN MODE ==="
fi

if [[ "$1" == "--status" ]]; then
    echo "=== Backfill Worker Status ==="
    for log in "$LOG_DIR"/worker_*.log; do
        if [[ -f "$log" ]]; then
            worker=$(basename "$log" .log)
            last_line=$(tail -1 "$log" 2>/dev/null || echo "no output")
            pid_file="$LOG_DIR/${worker}.pid"
            if [[ -f "$pid_file" ]]; then
                pid=$(cat "$pid_file")
                if kill -0 "$pid" 2>/dev/null; then
                    status="RUNNING (PID $pid)"
                else
                    status="COMPLETED"
                fi
            else
                status="UNKNOWN"
            fi
            echo "  $worker: $status"
            echo "    Last: $last_line"
        fi
    done
    # Show DB sizes
    echo ""
    echo "=== Backfill DB Sizes ==="
    for db in "$DATA_DIR"/backfill_*.db; do
        if [[ -f "$db" ]]; then
            size=$(du -h "$db" | cut -f1)
            count=$(sqlite3 "$db" "SELECT COUNT(*) FROM articles;" 2>/dev/null || echo "0")
            echo "  $(basename $db): $size ($count articles)"
        fi
    done
    exit 0
fi

echo "=== PESTLE Historical Backfill Orchestrator ==="
echo "Time: $(date)"
echo "Workers: 6 parallel processes"
echo ""

# Define worker ranges
declare -A WORKERS
WORKERS[1]="2000 2004"
WORKERS[2]="2005 2009"
WORKERS[3]="2010 2014"
WORKERS[4]="2015 2019"
WORKERS[5]="2020 2024"
WORKERS[6]="2025 2026"

PIDS=()

for i in 1 2 3 4 5 6; do
    read START END <<< "${WORKERS[$i]}"
    DB_FILE="$DATA_DIR/backfill_${START}_${END}.db"
    LOG_FILE="$LOG_DIR/worker_${i}_${START}_${END}.log"
    PID_FILE="$LOG_DIR/worker_${i}_${START}_${END}.pid"

    echo "Starting Worker $i: ${START}-${END} → $(basename $DB_FILE)"

    python3 "$SCRIPT_DIR/backfill_historical.py" \
        --start "$START" --end "$END" \
        --db "$DB_FILE" \
        $DRY_RUN \
        > "$LOG_FILE" 2>&1 &

    PID=$!
    echo "$PID" > "$PID_FILE"
    PIDS+=($PID)
    echo "  PID: $PID, Log: $(basename $LOG_FILE)"

    # Stagger starts by 3 seconds to avoid hitting GDELT rate limit simultaneously
    sleep 3
done

echo ""
echo "All workers launched. Monitoring..."
echo "Check progress: $0 --status"
echo ""

# Wait for all workers
FAILED=0
for i in "${!PIDS[@]}"; do
    PID=${PIDS[$i]}
    if wait "$PID"; then
        echo "Worker $((i+1)) (PID $PID): COMPLETED"
    else
        echo "Worker $((i+1)) (PID $PID): FAILED (exit code $?)"
        FAILED=$((FAILED+1))
    fi
done

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo "WARNING: $FAILED worker(s) failed. Check logs in $LOG_DIR/"
fi

echo ""
echo "=== Worker Summary ==="
for db in "$DATA_DIR"/backfill_*.db; do
    if [[ -f "$db" ]]; then
        count=$(sqlite3 "$db" "SELECT COUNT(*) FROM articles;" 2>/dev/null || echo "0")
        ja=$(sqlite3 "$db" "SELECT COUNT(*) FROM articles WHERE lang='ja';" 2>/dev/null || echo "0")
        en=$(sqlite3 "$db" "SELECT COUNT(*) FROM articles WHERE lang='en';" 2>/dev/null || echo "0")
        echo "  $(basename $db): $count articles (EN:$en, JA:$ja)"
    fi
done

if [[ -z "$DRY_RUN" ]]; then
    echo ""
    echo "Merging into main database..."
    python3 "$SCRIPT_DIR/merge_backfill.py"
    echo "Done!"
fi
