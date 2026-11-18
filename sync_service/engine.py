"""Sync orchestration engine.

Two modes, one durable state machine:

* **cursor mode** pages through the source's (unreliable) cursor API.
  Every response is checked for the three cursor pathologies:

    - expired/invalid cursor -> source raises ``CursorExpiredError``
    - duplicate cursor       -> ``next_cursor == current cursor``
    - cursor rollback        -> ``next_cursor`` points behind the current one

  Any of them immediately suspends cursor mode and starts **compensation**.

* **compensation mode** re-reads the affected data through the reliable
  time-window API, using half-open windows ``[start_ts, end_ts)`` anchored at
  the last *durable* watermark.  Windows may overlap (or be retried whole);
  the target's (biz_key, version) primary key makes overlap harmless, while
  half-open boundaries guarantee edge rows are neither skipped nor doubled.

Every batch is applied in one sqlite transaction with a deterministic
batch id, so re-applying a batch after any kind of interruption is a
no-op: that single property gives us crash-safe resume for free.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field

from .source import (
    CursorExpiredError,
    Record,
    ScriptedSource,
    TransientSourceError,
)
from .storage import Storage


@dataclass
class RunSummary:
    reason: str = ""
    cursor_batches: int = 0
    window_batches: int = 0
    inserted: int = 0
    deduped: int = 0
    conflicts: int = 0
    windows: list[str] = field(default_factory=list)
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "reason": self.reason, "cursor_batches": self.cursor_batches,
            "window_batches": self.window_batches, "inserted": self.inserted,
            "deduped": self.deduped, "conflicts": self.conflicts,
            "windows": self.windows, "error": self.error,
        }


class SyncEngine:
    WINDOW_PENDING_STATUSES = ("pending", "running", "failed")

    def __init__(
        self,
        source: ScriptedSource,
        storage: Storage,
        *,
        window_size: int = 10,
        max_retries: int = 3,
        crash_hook=None,
    ) -> None:
        self.source = source
        self.storage = storage
        self.window_size = window_size
        self.max_retries = max_retries
        # Test-only seam: crash_hook(batch_id, "before_commit"|"after_commit")
        self.crash_hook = crash_hook
        self._run_lock = threading.Lock()
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------ #
    # Public entry point (also the console's "continue" action)
    # ------------------------------------------------------------------ #
    def run(self) -> RunSummary:
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("a sync run is already in progress")
        self._running = True
        summary = RunSummary()
        try:
            state = self.storage.get_sync_state()
            state["runs"] = int(state.get("runs", 0)) + 1
            self.storage.save_sync_state(state)

            # Compensation left unfinished by an earlier interrupted run is
            # always finished before cursor mode resumes.
            if state.get("in_compensation"):
                ok = self._run_compensation(summary)
                if not ok:
                    summary.reason = summary.reason or "compensation_failed"
                    return summary

            self._run_cursor_cycle(summary)
            return summary
        finally:
            self._running = False
            self._run_lock.release()

    # ------------------------------------------------------------------ #
    # Cursor mode
    # ------------------------------------------------------------------ #
    def _run_cursor_cycle(self, summary: RunSummary) -> None:
        state = self.storage.get_sync_state()
        cursor = state["cursor"]

        while True:
            page, log_fields = self._fetch_cursor_page(cursor, summary)
            if page is None:
                # Either a hard transient failure, or an expired-cursor
                # compensation that could not finish.  Stop and keep the
                # durable state for a later console "continue".
                if summary.reason in ("transient_retries_exhausted",
                                      "compensation_failed"):
                    return
                # Expired cursor whose compensation finished inside the
                # fetch: re-anchor and keep paging in the same run.
                state = self.storage.get_sync_state()
                cursor = state["cursor"]
                continue

            # Deterministic batch id: the same cursor always produces the
            # same batch id, so a retried/redelivered page is skipped on
            # resume instead of being re-applied.
            batch_id = f"cur::{cursor or 'INIT'}"
            stats = self.storage.apply_batch(
                batch_id, "cursor", page.records, cursor=cursor,
                next_cursor=page.next_cursor, crash_hook=self.crash_hook)
            self._accumulate(summary, stats, window=False)

            # Advance the durable watermark from data we actually hold.
            page_max = max((r.updated_at for r in page.records),
                           default=state["safe_updated_at"])
            state["safe_updated_at"] = max(state["safe_updated_at"], page_max)

            anomaly = self._detect_cursor_anomaly(cursor, page)
            if anomaly is not None:
                # Annotate the (already observed) response with the anomaly
                # reason, then suspend cursor mode.
                self.storage.log_request(
                    **{**log_fields, "retry_reason": anomaly})
                self._start_compensation(anomaly, summary)
                ok = self._run_compensation(summary)
                if not ok:
                    # Keep durable state in compensation mode so the console's
                    # "continue" retries the failed/pending windows instead
                    # of skipping ahead in cursor mode.
                    summary.reason = "compensation_failed"
                    return
                # Compensation re-anchored the cursor; reload and continue.
                state = self.storage.get_sync_state()
                cursor = state["cursor"]
                continue

            self.storage.log_request(**log_fields)

            if not page.has_more or page.next_cursor is None:
                state["cursor"] = page.next_cursor
                state["committed_cursor"] = page.next_cursor
                self.storage.save_sync_state(state)
                summary.reason = "caught_up"
                return

            state["cursor"] = page.next_cursor
            state["committed_cursor"] = page.next_cursor
            self.storage.save_sync_state(state)
            cursor = page.next_cursor

    def _fetch_cursor_page(self, cursor, summary: RunSummary):
        """Fetch one cursor page with transient retries.

        Returns (FetchResult, log_fields), or (None, None) when an
        unrecoverable cursor anomaly triggered compensation.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                page = self.source.fetch_page(cursor)
                return page, {
                    "mode": "cursor", "cursor": cursor,
                    "attempt": attempt, "row_count": len(page.records),
                    "has_more": int(page.has_more),
                    "next_cursor": page.next_cursor,
                    "summary": _page_summary(page.records),
                    "result": "ok",
                }
            except CursorExpiredError as exc:
                self.storage.log_request(
                    mode="cursor", cursor=cursor, attempt=attempt,
                    row_count=0, has_more=0, summary="",
                    result="error", error_type="CursorExpiredError",
                    retry_reason=f"cursor_expired: {exc}")
                self._start_compensation("cursor_expired", summary)
                if not self._run_compensation(summary):
                    summary.reason = "compensation_failed"
                return None, None
            except TransientSourceError as exc:
                self.storage.log_request(
                    mode="cursor", cursor=cursor, attempt=attempt,
                    row_count=0, has_more=0, summary="",
                    result="error", error_type="TransientSourceError",
                    retry_reason=f"transient: {exc}")
                if attempt > self.max_retries:
                    summary.reason = "transient_retries_exhausted"
                    summary.error = str(exc)
                    return None, None
                continue

    def _detect_cursor_anomaly(self, current, page) -> str | None:
        # Note: has_more reflects the source's natural paging; a tampered
        # next_cursor can point backwards even on a "terminal" page, so the
        # cursor position itself is the only trustworthy signal.
        if page.next_cursor is None:
            return None
        nxt = page.next_cursor
        try:
            next_off = ScriptedSource.decode_cursor(nxt)
        except CursorExpiredError:
            return "unparseable_cursor"
        cur_off = ScriptedSource.decode_cursor(current or "cur:0")
        if next_off == cur_off:
            return "duplicate_cursor"
        if next_off < cur_off:
            return "cursor_rollback"
        return None

    # ------------------------------------------------------------------ #
    # Compensation mode (time windows)
    # ------------------------------------------------------------------ #
    def _start_compensation(self, reason: str, summary: RunSummary) -> None:
        state = self.storage.get_sync_state()
        anchor = int(state["safe_updated_at"])
        upper = int(self.source.max_updated_at()) + 1  # exclusive bound

        if upper <= anchor:
            # Nothing beyond the durable watermark: just re-anchor cursor.
            state["cursor"] = self.source.cursor_at_timestamp(anchor)
            state["committed_cursor"] = state["cursor"]
            state["in_compensation"] = False
            self.storage.save_sync_state(state)
            return

        start = anchor
        ids: list[str] = []
        while start < upper:
            end = min(start + self.window_size, upper)
            window_id = f"w:{start}-{end}"
            # ON CONFLICT DO NOTHING: retried/resumed runs keep the same
            # windows and their progress.
            self.storage.add_window(window_id, start, end)
            ids.append(window_id)
            start = end

        state["in_compensation"] = True
        state["mode"] = "window"
        state["resume_window_id"] = ids[0]
        self.storage.save_sync_state(state)
        summary.windows.extend(ids)

        self.storage.log_request(
            mode="window", window_id=",".join(ids), attempt=0,
            window_start=anchor, window_end=upper, row_count=0,
            has_more=0, summary=json.dumps(
                {"reason": reason, "anchor": anchor, "upper": upper,
                 "window_count": len(ids)}, sort_keys=True),
            result="compensation", retry_reason=reason)

    def _run_compensation(self, summary: RunSummary) -> bool:
        """Process all unfinished windows. Returns False on hard failure."""
        state = self.storage.get_sync_state()
        windows = [w for w in self.storage.list_windows()
                   if w["status"] in self.WINDOW_PENDING_STATUSES]
        if not windows:
            self._finish_compensation()
            return True

        for w in windows:
            state["resume_window_id"] = w["window_id"]
            self.storage.save_sync_state(state)
            self.storage.update_window(w["window_id"], status="running")

            offset = int(w["last_offset"])
            attempt = 0
            while True:
                attempt += 1
                try:
                    page = self.source.fetch_window_page(
                        w["window_id"], w["start_ts"], w["end_ts"], offset)
                except TransientSourceError as exc:
                    self.storage.log_request(
                        mode="window", window_id=w["window_id"],
                        attempt=attempt, window_start=w["start_ts"],
                        window_end=w["end_ts"], page_offset=offset,
                        row_count=0, has_more=0, summary="",
                        result="error", error_type="TransientSourceError",
                        retry_reason=f"transient: {exc}")
                    if attempt > self.max_retries:
                        self.storage.update_window(
                            w["window_id"], status="failed", error=str(exc))
                        summary.error = str(exc)
                        return False
                    continue

                batch_id = f"win:{w['window_id']}:{offset}"
                stats = self.storage.apply_batch(
                    batch_id, "window", page.records,
                    window_id=w["window_id"], cursor=page.next_cursor,
                    crash_hook=self.crash_hook)
                self._accumulate(summary, stats, window=True)

                self.storage.log_request(
                    mode="window", window_id=w["window_id"],
                    batch_id=batch_id, attempt=attempt,
                    window_start=w["start_ts"], window_end=w["end_ts"],
                    page_offset=offset, row_count=len(page.records),
                    has_more=int(page.has_more),
                    next_cursor=page.next_cursor,
                    summary=_page_summary(page.records), result="ok")

                offset += len(page.records)
                self.storage.update_window(
                    w["window_id"], last_offset=offset,
                    last_cursor=page.next_cursor)
                if not page.has_more:
                    break
                attempt = 0  # fresh page: its own retry budget

            self.storage.update_window(
                w["window_id"], status="done", error=None)

        self._finish_compensation()
        return True

    def _finish_compensation(self) -> None:
        state = self.storage.get_sync_state()
        anchor = int(state["safe_updated_at"])
        # Position the cursor past every record at or below the watermark.
        new_cursor = self.source.cursor_at_timestamp(anchor)
        state.update({
            "mode": "cursor", "in_compensation": False,
            "resume_window_id": None, "cursor": new_cursor,
            "committed_cursor": new_cursor,
        })
        self.storage.save_sync_state(state)

    # ------------------------------------------------------------------ #
    # Misc
    # ------------------------------------------------------------------ #
    @staticmethod
    def _accumulate(summary: RunSummary, stats: dict, *, window: bool) -> None:
        summary.inserted += stats["inserted"]
        summary.deduped += stats["deduped"]
        summary.conflicts += stats["conflicts"]
        if window:
            summary.window_batches += 0 if stats["skipped"] else 1
        else:
            summary.cursor_batches += 0 if stats["skipped"] else 1


def _page_summary(records: list[Record]) -> str:
    """Stable fingerprint of a source response for the request log."""
    return json.dumps(
        {"count": len(records),
         "ids": sorted(f"{r.biz_key}#v{r.version}" for r in records)},
        sort_keys=True)
