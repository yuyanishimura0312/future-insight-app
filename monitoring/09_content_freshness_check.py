#!/usr/bin/env python3
"""
09_content_freshness_check.py — Content Freshness SLI monitor (additive, 2026-06-14)

WHY THIS EXISTS
---------------
The daily-pipeline healthcheck H5 and steps/07_fih_freshness_check.py both judge freshness
by `created_at` (when a row landed in the DB). That is a blind spot: the historical collector
(collect_historical_daily.py) keeps writing rows every morning with OLD published_date values
(e.g. 1931, 1028), so `created_at` stays fresh even when the *current-news* collector
(collect_news.py) has been dead for weeks. On 2026-04-24 the shared ~/.venv vanished and
collect_news has failed silently every morning since — yet created_at-based monitors saw
"fresh" the whole time.

This monitor measures the content date (`published_date`) of each DB and splits PESTLE into
TWO independent SLIs so that "historical inflow alive, current-news dead" is correctly STALE:

  * Current-news SLI  : MAX(published_date) WHERE published_date >= today-90d   -> target 48h
  * Historical SLI    : MAX(created_at)                                          -> target 36h

This is a pure observation layer. It does NOT touch the daily-pipeline body, does NOT write to
any DB, and opens every DB read-only. It applies the ORE SLI/SLO + symptom-based alerting
principle: alert on the user-visible symptom (current news has gone stale), not on a proxy.

OUTPUT
------
  * Human-readable lines to stdout (captured by launchd log)
  * A JSON artifact at monitoring/state/content_freshness_latest.json (for dashboards/history)
  * ONE Slack digest to #事務 (via SLACK_WEBHOOK_PIPELINE) only when >=1 SLI is STALE.
    Kill switch: env CONTENT_FRESHNESS_ALERTS=0 disables the Slack POST (still logged).

EXIT CODE
---------
  0 always (a monitor must not crash a launchd chain). STALE state is reported, not raised.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

HOME = Path.home()
HERE = Path(__file__).resolve().parent
STATE_DIR = HERE / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "content_freshness_latest.json"

NOW = datetime.now()
RECENT_WINDOW_DAYS = 90  # window that isolates "current" content from historical backfill

# Accept the many date formats seen across these DBs.
_DATE_FMTS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def parse_dt(raw: str):
    """Parse a date string into a naive datetime, or None if unparseable."""
    if not raw:
        return None
    s = str(raw).strip().replace("Z", "+00:00")
    for fmt in _DATE_FMTS:
        try:
            t = datetime.strptime(s, fmt)
            if t.tzinfo is not None:
                t = t.replace(tzinfo=None)
            return t
        except ValueError:
            continue
    # last resort: leading YYYY-MM-DD
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError:
        return None


def query_scalar(db_path: Path, sql: str):
    """Run a read-only scalar query; return the value or None (never raises)."""
    if not db_path.exists():
        return None, "DB missing"
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute(sql).fetchone()
        finally:
            con.close()
        if not row or row[0] is None:
            return None, "no data"
        return row[0], None
    except Exception as e:  # noqa: BLE001 — monitor must be resilient
        return None, f"query error: {e}"


# ----------------------------------------------------------------------------
# SLI definitions.
#   metric = "published_date"  -> content recency, windowed to last 90d (current-news track)
#   metric = "created_at"      -> inflow recency (historical track / collector-liveness)
# target_hours = SLO budget for that SLI.
# ----------------------------------------------------------------------------
SLIS = [
    {
        "name": "PESTLE current-news",
        "db": HOME / "projects/research/pestle-signal-db/data/pestle.db",
        "metric": "published_date",
        "sql": (
            "SELECT MAX(published_date) FROM articles "
            f"WHERE published_date >= date('now','-{RECENT_WINDOW_DAYS} day')"
        ),
        "target_hours": 48,
        "note": "current-news collector liveness (collect_news.py). Ignores historical backfill.",
    },
    {
        "name": "PESTLE historical-inflow",
        "db": HOME / "projects/research/pestle-signal-db/data/pestle.db",
        "metric": "created_at",
        "sql": "SELECT MAX(created_at) FROM articles",
        "target_hours": 36,
        "note": "row inflow liveness (collect_historical_daily.py + sync). Independent of content age.",
    },
    {
        "name": "future_insight current-news (upstream)",
        "db": HOME / "projects/apps/future-insight-app/data/future_insight.db",
        "metric": "published_date",
        "sql": (
            "SELECT MAX(published_date) FROM articles "
            f"WHERE published_date >= date('now','-{RECENT_WINDOW_DAYS} day')"
        ),
        "target_hours": 48,
        "note": "upstream source-of-truth for PESTLE current news.",
    },
    {
        "name": "future_insight historical-inflow (upstream)",
        "db": HOME / "projects/apps/future-insight-app/data/future_insight.db",
        "metric": "created_at",
        "sql": "SELECT MAX(created_at) FROM articles",
        "target_hours": 36,
        "note": "upstream row inflow liveness.",
    },
    {
        "name": "Signal content",
        "db": HOME / "projects/research/pestle-signal-db/data/signal.db",
        "metric": "detected_date",
        "sql": "SELECT MAX(detected_date) FROM signals",
        "target_hours": 72,
        "note": "signal extraction recency (depends on PESTLE current news).",
    },
]


def evaluate():
    results = []
    for sli in SLIS:
        raw, err = query_scalar(sli["db"], sli["sql"])
        entry = {
            "name": sli["name"],
            "db": str(sli["db"]),
            "metric": sli["metric"],
            "target_hours": sli["target_hours"],
            "latest": raw,
            "note": sli["note"],
        }
        if err:
            entry["status"] = "UNKNOWN"
            entry["detail"] = err
            results.append(entry)
            continue
        dt = parse_dt(raw)
        if dt is None:
            entry["status"] = "UNKNOWN"
            entry["detail"] = f"unparseable date: {raw!r}"
            results.append(entry)
            continue
        age = NOW - dt
        age_hours = age.total_seconds() / 3600.0
        entry["age_hours"] = round(age_hours, 1)
        entry["age_days"] = round(age_hours / 24.0, 1)
        if age_hours <= sli["target_hours"]:
            entry["status"] = "FRESH"
        else:
            entry["status"] = "STALE"
        results.append(entry)
    return results


def slack_alert(text: str):
    """Post a single digest to #事務 via SLACK_WEBHOOK_PIPELINE. Kill switch + safe."""
    if os.environ.get("CONTENT_FRESHNESS_ALERTS", "1") == "0":
        print("  (Slack alert suppressed by CONTENT_FRESHNESS_ALERTS=0)")
        return
    try:
        webhook = subprocess.run(
            ["security", "find-generic-password", "-s", "SLACK_WEBHOOK_PIPELINE", "-w"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except Exception:
        webhook = ""
    if not webhook:
        print("  (no SLACK_WEBHOOK_PIPELINE in keychain; alert not sent)")
        return
    try:
        subprocess.run(
            ["curl", "-s", "-X", "POST", "-H", "Content-type: application/json",
             "--data", json.dumps({"text": text}), webhook],
            capture_output=True, text=True, timeout=15,
        )
        print("  Slack notified (#事務).")
    except Exception as e:  # noqa: BLE001
        print(f"  WARNING: Slack POST failed: {e}")


def main():
    print(f"=== Content Freshness SLI check — {NOW:%Y-%m-%d %H:%M} ===")
    print(f"(current-news window = last {RECENT_WINDOW_DAYS}d via published_date; "
          f"historical via created_at)")
    results = evaluate()

    stale = [r for r in results if r["status"] == "STALE"]
    unknown = [r for r in results if r["status"] == "UNKNOWN"]

    for r in results:
        latest = r.get("latest")
        age = r.get("age_days")
        age_s = f" age={age}d" if age is not None else ""
        print(f"  [{r['status']:<6}] {r['name']:<42} metric={r['metric']:<14} "
              f"latest={latest}{age_s} (SLO {r['target_hours']}h)")
        if r["status"] in ("STALE", "UNKNOWN"):
            print(f"           -> {r.get('detail', r['note'])}")

    summary = {
        "generated_at": NOW.isoformat(timespec="seconds"),
        "recent_window_days": RECENT_WINDOW_DAYS,
        "stale_count": len(stale),
        "unknown_count": len(unknown),
        "slis": results,
    }
    STATE_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  wrote {STATE_FILE}")

    if stale:
        names = ", ".join(f"{r['name']} (age {r.get('age_days','?')}d)" for r in stale)
        msg = (f"[future-insight] content freshness ALERT {NOW:%Y-%m-%d}: "
               f"{len(stale)} STALE SLI — {names}. "
               f"Note: created_at-based monitors can read FRESH while current news is dead "
               f"(historical backfill masks it). Detail: {STATE_FILE}")
        print(f"  STALE detected: {msg}")
        slack_alert(msg)
    else:
        print("  All content-freshness SLIs within SLO (no Slack digest sent).")

    # Monitor never fails the chain.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
