#!/usr/bin/env python3
"""
Report pipeline step status to Firestore.

Usage:
  python3 report_status.py --step collect_news --status success --duration 45
  python3 report_status.py --step ai_analyze --status error --duration 120 --error "API timeout"
  python3 report_status.py --run-start
  python3 report_status.py --run-end --status success --duration 342
  python3 report_status.py --upload-log /path/to/logfile.log

Writes to Firestore collections:
  pipeline_status/current  — live pipeline state
  pipeline_logs/{date}     — log lines per day
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    print("[WARN] firebase-admin not installed, skipping status report")
    sys.exit(0)

SCRIPTS_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPTS_DIR.parent
SA_KEY_PATH = PROJECT_DIR / ".firebase-service-account.json"

_app = None
_db = None


def get_db():
    """Initialize Firestore client (lazy, singleton)."""
    global _app, _db
    if _db is not None:
        return _db
    if not SA_KEY_PATH.exists():
        print(f"[WARN] Service account key not found at {SA_KEY_PATH}, skipping")
        return None
    try:
        cred = credentials.Certificate(str(SA_KEY_PATH))
        _app = firebase_admin.initialize_app(cred, {"projectId": "miratuku-afa2c"})
        _db = firestore.client()
        return _db
    except Exception as e:
        print(f"[WARN] Firestore init failed: {e}")
        return None


def report_step(step: str, status: str, duration: int = 0, error: str = ""):
    """Report a single step's result to pipeline_status/current."""
    db = get_db()
    if not db:
        return
    doc_ref = db.collection("pipeline_status").document("current")
    doc_ref.set({
        "steps": {
            step: {
                "status": status,
                "completedAt": firestore.SERVER_TIMESTAMP,
                "durationSec": duration,
                "error": error,
            }
        }
    }, merge=True)


def report_run_start():
    """Mark the start of a full pipeline run."""
    db = get_db()
    if not db:
        return
    doc_ref = db.collection("pipeline_status").document("current")
    doc_ref.set({
        "lastRunDate": datetime.now().strftime("%Y-%m-%d"),
        "lastRunStatus": "running",
        "lastRunStartedAt": firestore.SERVER_TIMESTAMP,
    }, merge=True)


def report_run_end(status: str, duration: int = 0):
    """Mark the end of a full pipeline run."""
    db = get_db()
    if not db:
        return
    doc_ref = db.collection("pipeline_status").document("current")
    doc_ref.set({
        "lastRunStatus": status,
        "lastRunCompletedAt": firestore.SERVER_TIMESTAMP,
        "lastRunDuration": duration,
    }, merge=True)


def upload_log(log_path: str):
    """Upload log file contents to Firestore pipeline_logs/{date}."""
    db = get_db()
    if not db:
        return
    path = Path(log_path)
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8", errors="replace")
    # Truncate to ~500KB to stay under Firestore 1MB limit
    lines = text.split("\n")
    if len(lines) > 2000:
        lines = lines[-2000:]
    date_str = datetime.now().strftime("%Y-%m-%d")
    db.collection("pipeline_logs").document(date_str).set({
        "date": date_str,
        "lines": lines,
        "lineCount": len(lines),
        "uploadedAt": firestore.SERVER_TIMESTAMP,
    })


def main():
    parser = argparse.ArgumentParser(description="Report pipeline status to Firestore")
    parser.add_argument("--step", help="Step name (e.g., collect_news)")
    parser.add_argument("--status", help="Status: success or error")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds")
    parser.add_argument("--error", default="", help="Error message")
    parser.add_argument("--run-start", action="store_true", help="Mark run start")
    parser.add_argument("--run-end", action="store_true", help="Mark run end")
    parser.add_argument("--upload-log", help="Upload log file path")
    args = parser.parse_args()

    if args.run_start:
        report_run_start()
    elif args.run_end:
        report_run_end(args.status or "success", args.duration)
    elif args.upload_log:
        upload_log(args.upload_log)
    elif args.step and args.status:
        report_step(args.step, args.status, args.duration, args.error)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
