"""Durable state for the sync service (sqlite, stdlib only).

All progress that matters after a crash lives here:

state (kv)        -- durable sync watermark and current mode/cursor.
target            -- idempotent target table, unique on (biz_key, version).
batches           -- one row per applied batch; the resume checkpoint.
request_log       -- one row per *source request attempt* (cursor, window,
                     response summary, retry reason).  The observability
                     backbone required by the spec.
windows           -- compensation windows and their lifecycle status.
conflicts         -- stale-version / conflicting writes observed at target.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable

from .source import Record

SCHEMA = """
CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS target (
    biz_key      TEXT NOT NULL,
    version      INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    applied_at   TEXT NOT NULL,
    PRIMARY KEY (biz_key, version)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    mode         TEXT NOT NULL,              -- cursor | window
    window_id    TEXT,
    cursor       TEXT,
    next_cursor  TEXT,
    record_count INTEGER NOT NULL,
    dedup_count  INTEGER NOT NULL,
    conflict_count INTEGER NOT NULL,
    status       TEXT NOT NULL,              -- applied
    applied_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS request_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    mode        TEXT NOT NULL,               -- cursor | window
    window_id   TEXT,
    batch_id    TEXT,
    attempt     INTEGER NOT NULL,
    cursor      TEXT,
    window_start INTEGER,
    window_end   INTEGER,
    page_offset  INTEGER,
    row_count   INTEGER,
    has_more    INTEGER,
    next_cursor TEXT,
    summary     TEXT NOT NULL,
    result      TEXT NOT NULL,               -- ok | error
    error_type  TEXT,
    retry_reason TEXT
);
CREATE TABLE IF NOT EXISTS windows (
    window_id   TEXT PRIMARY KEY,
    start_ts    INTEGER NOT NULL,
    end_ts      INTEGER NOT NULL,
    status      TEXT NOT NULL,               -- pending | running | done | failed
    last_offset INTEGER NOT NULL DEFAULT 0,
    last_cursor TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS conflicts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    detected_at  TEXT NOT NULL,
    biz_key      TEXT NOT NULL,
    incoming_version INTEGER NOT NULL,
    existing_version INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    payload      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_target_key ON target(biz_key);
CREATE INDEX IF NOT EXISTS idx_log_ts ON request_log(ts);
CREATE INDEX IF NOT EXISTS idx_conflict_key ON conflicts(biz_key);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Storage:
    """Thin, explicit-transaction wrapper around sqlite3."""

    def __init__(self, path: str) -> None:
        # check_same_thread=False: the console thread may read while the
        # engine thread writes; sqlite locking plus short transactions keep
        # this safe.  Autocommit (isolation_level=None) so we control
        # transaction boundaries ourselves.
        self.conn = sqlite3.connect(path, check_same_thread=False,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ #
    # Key/value state (watermark, mode, cursors)
    # ------------------------------------------------------------------ #
    def get_state(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute(
            "SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default

    def set_state(self, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT INTO state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )

    def get_sync_state(self) -> dict:
        return self.get_state(
            "sync",
            {
                "mode": "cursor",
                "cursor": None,
                "committed_cursor": None,
                "safe_updated_at": 0,
                "in_compensation": False,
                "resume_window_id": None,
                "last_run_at": None,
                "runs": 0,
            },
        )

    def save_sync_state(self, state: dict) -> None:
        state = dict(state)
        state["last_run_at"] = utc_now_iso()
        self.set_state("sync", state)

    # ------------------------------------------------------------------ #
    # Request log
    # ------------------------------------------------------------------ #
    def log_request(self, **fields: Any) -> int:
        fields.setdefault("ts", utc_now_iso())
        fields.setdefault("summary", "")
        cols = ["ts", "mode", "window_id", "batch_id", "attempt", "cursor",
                "window_start", "window_end", "page_offset", "row_count",
                "has_more", "next_cursor", "summary", "result", "error_type",
                "retry_reason"]
        values = [fields.get(c) for c in cols]
        cur = self.conn.execute(
            f"INSERT INTO request_log ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' * len(cols))})", values)
        return int(cur.lastrowid)

    def list_requests(self, limit: int = 100) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM request_log ORDER BY id DESC LIMIT ?",
            (limit,)).fetchall())

    def count_requests(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) c FROM request_log").fetchone()["c"]

    # ------------------------------------------------------------------ #
    # Batches (the idempotent resume checkpoint)
    # ------------------------------------------------------------------ #
    def batch_exists(self, batch_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone() is not None

    def list_batches(self) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM batches ORDER BY applied_at, batch_id"))

    # ------------------------------------------------------------------ #
    # Target + conflicts
    # ------------------------------------------------------------------ #
    def upsert_record(self, rec: Record, now: str) -> tuple[bool, bool]:
        """Apply one record version idempotently.

        Returns (deduped, conflict):
          (False, False) -- inserted a new (biz_key, version) row
          (True,  False) -- exact version already present (redelivery)
          (True,  True)  -- stale version, target holds a newer one
        """
        row = self.conn.execute(
            "SELECT version FROM target WHERE biz_key=? AND version=?",
            (rec.biz_key, rec.version)).fetchone()
        if row is not None:
            return True, False  # exact (key, version) already applied

        newer = self.conn.execute(
            "SELECT version FROM target WHERE biz_key=? AND version > ? "
            "ORDER BY version DESC LIMIT 1",
            (rec.biz_key, rec.version)).fetchone()
        if newer is not None:
            self.conn.execute(
                "INSERT INTO conflicts(detected_at, biz_key, "
                "incoming_version, existing_version, reason, payload) "
                "VALUES(?,?,?,?,?,?)",
                (now, rec.biz_key, rec.version, newer["version"],
                 "stale_version", rec.to_json()))
            return True, True

        self.conn.execute(
            "INSERT INTO target(biz_key, version, updated_at, payload, "
            "applied_at) VALUES(?,?,?,?,?)",
            (rec.biz_key, rec.version, rec.updated_at, rec.to_json(), now))
        return False, False

    def target_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) c FROM target").fetchone()["c"]

    def list_target(self, limit: int = 200) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM target ORDER BY biz_key, version LIMIT ?",
            (limit,)).fetchall())

    def list_conflicts(self, limit: int = 100) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM conflicts ORDER BY id DESC LIMIT ?",
            (limit,)).fetchall())

    def water_level(self) -> int:
        """Sync water level = highest updated_at durably in the target."""
        row = self.conn.execute(
            "SELECT MAX(updated_at) m FROM target").fetchone()
        return int(row["m"] or 0)

    # ------------------------------------------------------------------ #
    # Compensation windows
    # ------------------------------------------------------------------ #
    def add_window(self, window_id: str, start_ts: int, end_ts: int) -> None:
        now = utc_now_iso()
        self.conn.execute(
            "INSERT INTO windows(window_id, start_ts, end_ts, status, "
            "last_offset, created_at, updated_at) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(window_id) DO NOTHING",
            (window_id, start_ts, end_ts, "pending", 0, now, now))

    def list_windows(self) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM windows ORDER BY start_ts"))

    def get_window(self, window_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM windows WHERE window_id=?",
            (window_id,)).fetchone()

    def update_window(self, window_id: str, **fields: Any) -> None:
        fields["updated_at"] = utc_now_iso()
        assignments = ", ".join(f"{k}=?" for k in fields)
        self.conn.execute(
            f"UPDATE windows SET {assignments} WHERE window_id=?",
            (*fields.values(), window_id))

    # ------------------------------------------------------------------ #
    # Explicit transactions
    # ------------------------------------------------------------------ #
    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")

    def apply_batch(
        self,
        batch_id: str,
        mode: str,
        records: Iterable[Record],
        *,
        window_id: str | None = None,
        cursor: str | None = None,
        next_cursor: str | None = None,
        crash_hook: Any = None,
    ) -> dict:
        """Apply one page of records as a single atomic transaction.

        Idempotent: a batch_id that already exists is skipped wholesale,
        which is what makes resume safe whether the crash happened before
        commit, after commit but before the state update, or mid-run.

        ``crash_hook(batch_id, phase)`` is a test-only seam.  Raising from
        phase="before_commit" simulates a crash before durable commit;
        raising from "after_commit" simulates losing the response after the
        batch is already durable.
        """
        if self.batch_exists(batch_id):
            row = self.conn.execute(
                "SELECT record_count, dedup_count, conflict_count "
                "FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            return {"skipped": True, "inserted": 0,
                    "deduped": row["record_count"] - row["conflict_count"],
                    "conflicts": row["conflict_count"],
                    "count": row["record_count"]}

        now = utc_now_iso()
        records = list(records)
        self.begin()
        try:
            inserted = deduped = conflicts = 0
            for rec in records:
                was_dup, was_conflict = self.upsert_record(rec, now)
                if was_conflict:
                    conflicts += 1
                    deduped += 1
                elif was_dup:
                    deduped += 1
                else:
                    inserted += 1
            self.conn.execute(
                "INSERT INTO batches(batch_id, mode, window_id, cursor, "
                "next_cursor, record_count, dedup_count, conflict_count, "
                "status, applied_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (batch_id, mode, window_id, cursor, next_cursor,
                 len(records), deduped, conflicts, "applied", now))
            if crash_hook is not None:
                crash_hook(batch_id, "before_commit")
            self.commit()
        except BaseException:
            self.rollback()
            raise
        if crash_hook is not None:
            crash_hook(batch_id, "after_commit")
        return {"skipped": False, "inserted": inserted, "deduped": deduped,
                "conflicts": conflicts, "count": len(records)}
