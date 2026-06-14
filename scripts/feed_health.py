#!/usr/bin/env python3
"""
feed_health.py — per-feed health ledger for collect_news.py (additive, 2026-06-14)

WHY THIS EXISTS
---------------
collect_news.py fetches ~124 RSS/Atom feeds sequentially. Some are dead, some
return zero entries (empty fetch), some throw. Until now the only signal was a
[WARN] line in a daily log that nobody reads. When the shared venv vanished the
whole step died; before that, individual feeds had been quietly rotting with no
record. This module gives the reliability layer a queryable answer to
"which feeds are alive / dead / empty, and when did each last succeed?".

DESIGN (ORE: symptom-based, stability-first, additive, read-only w.r.t. pipeline)
---------------------------------------------------------------------------------
- Writes ONLY to its own DB: data/feed_health.db. It never touches
  future_insight.db, pestle.db, signal.db, or the daily-pipeline body.
- Fail-safe: every public call is wrapped so that a health-ledger error can
  NEVER break collection. If the ledger is unavailable, collect_news proceeds
  exactly as before (the recorder degrades to a no-op).
- No new runtime dependency (stdlib sqlite3 only).
- One row per feed: latest state is UPSERTed; consecutive_failures and lifetime
  counters accumulate so slow-rotting feeds surface before they go fully dead.

USAGE (in collect_news.fetch_all_feeds)
---------------------------------------
    from feed_health import FeedHealthRecorder
    rec = FeedHealthRecorder()                      # safe even if DB can't open
    ...
    rec.record(name, url, status="ok", entries=n)   # status in ok|empty|error
    ...
    rec.finish()                                    # writes a run summary row

QUERY
-----
    sqlite3 data/feed_health.db \
      "SELECT name, status, entries_last, consecutive_failures, last_success_at
       FROM feed_health ORDER BY consecutive_failures DESC, status;"
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DEFAULT_DB = _HERE.parent / "data" / "feed_health.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS feed_health (
    name                  TEXT PRIMARY KEY,
    url                   TEXT,
    status                TEXT,          -- 'ok' | 'empty' | 'error'
    entries_last          INTEGER DEFAULT 0,
    last_error            TEXT,
    last_success_at       TEXT,          -- ISO, last time status was 'ok'
    last_checked_at       TEXT,          -- ISO, every observation
    consecutive_failures  INTEGER DEFAULT 0,  -- resets to 0 on 'ok'; counts empty+error
    total_ok              INTEGER DEFAULT 0,
    total_empty           INTEGER DEFAULT 0,
    total_error           INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS feed_health_runs (
    run_at        TEXT PRIMARY KEY,
    feeds_total   INTEGER,
    feeds_ok      INTEGER,
    feeds_empty   INTEGER,
    feeds_error   INTEGER
);
"""


class FeedHealthRecorder:
    """Records per-feed outcomes. Every method is fail-safe (never raises)."""

    def __init__(self, db_path: Path | str | None = None):
        self._enabled = os.environ.get("FEED_HEALTH_ENABLED", "1") != "0"
        self._db_path = Path(db_path) if db_path else _DEFAULT_DB
        self._con: sqlite3.Connection | None = None
        self._counts = {"ok": 0, "empty": 0, "error": 0}
        if not self._enabled:
            return
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._con = sqlite3.connect(str(self._db_path), timeout=10)
            self._con.executescript(_SCHEMA)
            self._con.commit()
        except Exception as e:  # noqa: BLE001 — ledger must never break collection
            print(f"  [feed_health] disabled (cannot open ledger): {e}")
            self._con = None

    def record(self, name: str, url: str, status: str, entries: int = 0,
               error: str | None = None) -> None:
        """Record one feed outcome. status: 'ok' | 'empty' | 'error'."""
        if status in self._counts:
            self._counts[status] += 1
        if self._con is None:
            return
        now = datetime.now().isoformat(timespec="seconds")
        try:
            cur = self._con.execute(
                "SELECT consecutive_failures, total_ok, total_empty, total_error, "
                "last_success_at FROM feed_health WHERE name = ?",
                (name,),
            ).fetchone()
            if cur is None:
                consec, t_ok, t_empty, t_err, last_ok = 0, 0, 0, 0, None
            else:
                consec, t_ok, t_empty, t_err, last_ok = cur

            if status == "ok":
                consec = 0
                t_ok += 1
                last_ok = now
            elif status == "empty":
                consec += 1
                t_empty += 1
            else:  # error
                consec += 1
                t_err += 1

            self._con.execute(
                "INSERT INTO feed_health "
                "(name, url, status, entries_last, last_error, last_success_at, "
                " last_checked_at, consecutive_failures, total_ok, total_empty, total_error) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "  url=excluded.url, status=excluded.status, "
                "  entries_last=excluded.entries_last, last_error=excluded.last_error, "
                "  last_success_at=excluded.last_success_at, "
                "  last_checked_at=excluded.last_checked_at, "
                "  consecutive_failures=excluded.consecutive_failures, "
                "  total_ok=excluded.total_ok, total_empty=excluded.total_empty, "
                "  total_error=excluded.total_error",
                (name, url, status, int(entries or 0),
                 (error or "")[:500] if status == "error" else None,
                 last_ok, now, consec, t_ok, t_empty, t_err),
            )
            self._con.commit()
        except Exception as e:  # noqa: BLE001
            print(f"  [feed_health] record skipped for {name}: {e}")

    def finish(self) -> None:
        """Write a per-run summary row and close. Fail-safe."""
        if self._con is None:
            return
        try:
            now = datetime.now().isoformat(timespec="seconds")
            self._con.execute(
                "INSERT OR REPLACE INTO feed_health_runs "
                "(run_at, feeds_total, feeds_ok, feeds_empty, feeds_error) VALUES (?,?,?,?,?)",
                (now,
                 sum(self._counts.values()),
                 self._counts["ok"], self._counts["empty"], self._counts["error"]),
            )
            self._con.commit()
            ok, empty, err = (self._counts["ok"], self._counts["empty"], self._counts["error"])
            print(f"  [feed_health] run summary: {ok} ok / {empty} empty / {err} error "
                  f"(ledger: {self._db_path})")
        except Exception as e:  # noqa: BLE001
            print(f"  [feed_health] finish skipped: {e}")
        finally:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None
