#!/usr/bin/env python3
"""
Pipeline Daemon — watches Firestore for commands and executes pipeline steps.

Runs as a persistent process via launchd. Listens to the
pipeline_commands collection for pending commands, executes the
corresponding script, and reports results back to Firestore.

Also sends a heartbeat every 60 seconds to pipeline_status/current.

Usage:
  python3 pipeline_daemon.py

Requires:
  - firebase-admin Python package
  - Service account key at ../.firebase-service-account.json
  - ANTHROPIC_API_KEY in environment or macOS Keychain
"""

import os
import sys
import time
import signal
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

# Add dotenv support
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import firebase_admin
from firebase_admin import credentials, firestore

SCRIPTS_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPTS_DIR.parent
SA_KEY_PATH = PROJECT_DIR / ".firebase-service-account.json"
LOG_DIR = PROJECT_DIR / "logs"

# Step name -> script file mapping
STEPS = {
    "collect_news": "collect_news.py",
    "ai_analyze": "ai_analyze.py",
    "curate_daily_papers": "curate_daily_papers.py",
    "generate_insight_reports": "generate_insight_reports.py",
    "detect_alerts": "detect_alerts.py",
    "collect_historical_daily": "collect_historical_daily.py",
    "update_history": "update_history.py",
    "generate_daily_report": "generate_daily_report.py",
    "deploy": None,  # handled specially
}

# Global state
running_lock = threading.Lock()
is_running = False
shutdown_flag = False


def init_firestore():
    """Initialize Firebase Admin SDK."""
    if not SA_KEY_PATH.exists():
        print(f"[ERROR] Service account key not found: {SA_KEY_PATH}")
        print("Download from Firebase Console > Project Settings > Service Accounts")
        sys.exit(1)
    cred = credentials.Certificate(str(SA_KEY_PATH))
    firebase_admin.initialize_app(cred, {"projectId": "miratuku-afa2c"})
    return firestore.client()


def get_env():
    """Build environment for subprocess execution."""
    env = os.environ.copy()
    # Try to get API key from keychain if not in env
    if "ANTHROPIC_API_KEY" not in env:
        try:
            result = subprocess.run(
                ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                env["ANTHROPIC_API_KEY"] = result.stdout.strip()
        except Exception:
            pass
    return env


def run_step(step_name: str) -> tuple[bool, str, int]:
    """Execute a single pipeline step. Returns (success, output, duration_sec)."""
    script = STEPS.get(step_name)
    if script is None and step_name == "deploy":
        # Git deploy
        start = time.time()
        result = subprocess.run(
            ["bash", "-c", "cd {} && git add data/ && git diff --quiet data/ || (git commit -m 'chore: manual pipeline update' && git push)".format(PROJECT_DIR)],
            capture_output=True, text=True, timeout=120, env=get_env()
        )
        duration = int(time.time() - start)
        output = result.stdout + result.stderr
        return result.returncode == 0, output[-2000:], duration

    if script is None:
        return False, f"Unknown step: {step_name}", 0

    script_path = SCRIPTS_DIR / script
    if not script_path.exists():
        return False, f"Script not found: {script_path}", 0

    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True, text=True,
            timeout=600,  # 10 min timeout per step
            cwd=str(SCRIPTS_DIR),
            env=get_env()
        )
        duration = int(time.time() - start)
        output = result.stdout + result.stderr
        return result.returncode == 0, output[-2000:], duration
    except subprocess.TimeoutExpired:
        duration = int(time.time() - start)
        return False, "Timeout after 600 seconds", duration
    except Exception as e:
        duration = int(time.time() - start)
        return False, str(e), duration


def run_all() -> tuple[bool, str, int]:
    """Execute the full pipeline via daily_update.sh."""
    script_path = SCRIPTS_DIR / "daily_update.sh"
    start = time.time()
    try:
        result = subprocess.run(
            ["/bin/bash", str(script_path)],
            capture_output=True, text=True,
            timeout=3600,  # 1 hour timeout for full pipeline
            cwd=str(SCRIPTS_DIR),
            env=get_env()
        )
        duration = int(time.time() - start)
        output = result.stdout + result.stderr
        return result.returncode == 0, output[-3000:], duration
    except subprocess.TimeoutExpired:
        duration = int(time.time() - start)
        return False, "Timeout after 3600 seconds", duration
    except Exception as e:
        duration = int(time.time() - start)
        return False, str(e), duration


def handle_command(db, doc_snapshot):
    """Process a pending pipeline command."""
    global is_running

    doc = doc_snapshot
    data = doc.to_dict()
    cmd_type = data.get("type", "")
    step = data.get("step", "")
    doc_ref = db.collection("pipeline_commands").document(doc.id)
    status_ref = db.collection("pipeline_status").document("current")

    with running_lock:
        if is_running:
            doc_ref.update({
                "status": "error",
                "error": "Another command is already running",
                "completedAt": firestore.SERVER_TIMESTAMP,
            })
            return
        is_running = True

    try:
        # Mark as running
        doc_ref.update({
            "status": "running",
            "startedAt": firestore.SERVER_TIMESTAMP,
        })

        if cmd_type == "run_all":
            status_ref.set({"lastRunStatus": "running", "lastRunStartedAt": firestore.SERVER_TIMESTAMP}, merge=True)
            success, output, duration = run_all()
            status_ref.set({
                "lastRunStatus": "success" if success else "error",
                "lastRunCompletedAt": firestore.SERVER_TIMESTAMP,
                "lastRunDuration": duration,
                "lastRunDate": datetime.now().strftime("%Y-%m-%d"),
            }, merge=True)

        elif cmd_type == "run_step":
            success, output, duration = run_step(step)
            status_ref.set({
                "steps": {
                    step: {
                        "status": "success" if success else "error",
                        "completedAt": firestore.SERVER_TIMESTAMP,
                        "durationSec": duration,
                        "error": "" if success else output[-500:],
                    }
                }
            }, merge=True)

        elif cmd_type == "change_schedule":
            hour = data.get("hour", 4)
            minute = data.get("minute", 0)
            success = change_schedule(hour, minute)
            output = f"Schedule changed to {hour:02d}:{minute:02d}" if success else "Failed to change schedule"
            duration = 0
            if success:
                status_ref.set({"scheduleHour": hour, "scheduleMinute": minute}, merge=True)

        else:
            success, output, duration = False, f"Unknown command type: {cmd_type}", 0

        # Mark command as complete
        doc_ref.update({
            "status": "success" if success else "error",
            "completedAt": firestore.SERVER_TIMESTAMP,
            "durationSec": duration,
            "error": "" if success else output[-500:],
            "output": output[-1000:],
        })

        # Upload today's log if exists
        today_log = LOG_DIR / f"daily_{datetime.now().strftime('%Y-%m-%d')}.log"
        if today_log.exists():
            lines = today_log.read_text(encoding="utf-8", errors="replace").split("\n")
            if len(lines) > 2000:
                lines = lines[-2000:]
            db.collection("pipeline_logs").document(datetime.now().strftime("%Y-%m-%d")).set({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "lines": lines,
                "lineCount": len(lines),
                "uploadedAt": firestore.SERVER_TIMESTAMP,
            })

    finally:
        with running_lock:
            is_running = False


def change_schedule(hour: int, minute: int) -> bool:
    """Change the launchd schedule by rewriting the plist."""
    plist_path = Path.home() / "Library" / "LaunchAgents" / "com.futureinsight.daily-update.plist"
    if not plist_path.exists():
        return False
    try:
        import plistlib
        # Unload current
        subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True, timeout=10)
        # Read and modify
        with open(plist_path, "rb") as f:
            plist = plistlib.load(f)
        plist["StartCalendarInterval"] = {"Hour": hour, "Minute": minute}
        with open(plist_path, "wb") as f:
            plistlib.dump(plist, f)
        # Reload
        subprocess.run(["launchctl", "load", str(plist_path)], capture_output=True, timeout=10)
        return True
    except Exception as e:
        print(f"[ERROR] Schedule change failed: {e}")
        return False


def heartbeat_loop(db):
    """Send heartbeat every 60 seconds."""
    while not shutdown_flag:
        try:
            db.collection("pipeline_status").document("current").set({
                "daemonAlive": firestore.SERVER_TIMESTAMP,
                "daemonPid": os.getpid(),
            }, merge=True)
        except Exception as e:
            print(f"[WARN] Heartbeat failed: {e}")
        for _ in range(60):
            if shutdown_flag:
                break
            time.sleep(1)


def on_snapshot(col_snapshot, changes, read_time):
    """Firestore snapshot listener callback."""
    for change in changes:
        if change.type.name == "ADDED":
            doc = change.document
            data = doc.to_dict()
            if data.get("status") == "pending":
                print(f"[CMD] Received: {data.get('type')} {data.get('step', '')}")
                # Run in a thread to not block the listener
                threading.Thread(
                    target=handle_command,
                    args=(_db, doc),
                    daemon=True
                ).start()


_db = None


def main():
    global _db, shutdown_flag

    print(f"{'=' * 50}")
    print(f"  Pipeline Daemon")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  PID: {os.getpid()}")
    print(f"{'=' * 50}")

    _db = init_firestore()

    # Initialize status document with schedule info
    plist_path = Path.home() / "Library" / "LaunchAgents" / "com.futureinsight.daily-update.plist"
    schedule = {"scheduleHour": 4, "scheduleMinute": 0}
    if plist_path.exists():
        try:
            import plistlib
            with open(plist_path, "rb") as f:
                plist = plistlib.load(f)
            cal = plist.get("StartCalendarInterval", {})
            schedule["scheduleHour"] = cal.get("Hour", 4)
            schedule["scheduleMinute"] = cal.get("Minute", 0)
        except Exception:
            pass
    _db.collection("pipeline_status").document("current").set(schedule, merge=True)

    # Clean up any stale "running" commands from previous crashes
    stale = _db.collection("pipeline_commands").where("status", "==", "running").get()
    for doc in stale:
        doc.reference.update({
            "status": "error",
            "error": "Daemon restarted — command was interrupted",
            "completedAt": firestore.SERVER_TIMESTAMP,
        })
        print(f"[CLEANUP] Marked stale command {doc.id} as error")

    # Start heartbeat
    hb_thread = threading.Thread(target=heartbeat_loop, args=(_db,), daemon=True)
    hb_thread.start()
    print("[OK] Heartbeat started")

    # Listen for pending commands
    query = _db.collection("pipeline_commands").where("status", "==", "pending")
    watch = query.on_snapshot(on_snapshot)
    print("[OK] Listening for commands...")

    # Handle graceful shutdown
    def shutdown(sig, frame):
        global shutdown_flag
        print(f"\n[SHUTDOWN] Received signal {sig}")
        shutdown_flag = True
        watch.unsubscribe()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Keep alive
    while not shutdown_flag:
        time.sleep(1)


if __name__ == "__main__":
    main()
