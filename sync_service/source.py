"""Source-side abstractions.

The real-world source behind this abstraction exposes a cursor API whose
cursors are not trustworthy: they can expire, be repeated by the server or
move backwards.  It also exposes a *reliable* time-window query which the
engine falls back to when cursor anomalies are detected.

`ScriptedSource` is a deterministic in-memory implementation whose behaviour
is fully scripted, so tests can deterministically reproduce every failure
mode (expired cursor, duplicate cursor, cursor rollback, transient network
error, redelivered rows, stale-version rows).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Callable, Iterable


@dataclass(frozen=True)
class Record:
    """One business record.

    The pair (biz_key, version) uniquely identifies a record version.
    `updated_at` is the source-side update time used by window queries.
    """

    biz_key: str
    version: int
    updated_at: int
    payload: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {"biz_key": self.biz_key, "version": self.version,
             "updated_at": self.updated_at, "payload": self.payload},
            sort_keys=True,
        )


class CursorExpiredError(RuntimeError):
    """The server rejected the cursor as expired/invalid."""


class TransientSourceError(RuntimeError):
    """A retriable transport/server failure."""


@dataclass
class FetchResult:
    records: list[Record]
    next_cursor: str | None  # None means: source has no more pages right now
    has_more: bool


class ScriptedSource:
    """Deterministic fake source with scriptable anomalies.

    Parameters
    ----------
    records:
        The full, ordered record set living in the source.  Cursor pagination
        walks this list; time-window queries filter it by ``updated_at``.
    page_size:
        Number of records returned per cursor / window page.

    Scripting hooks (each maps a 0-based fetch attempt number to an effect):
      cursor_fail_at   -- attempt raises CursorExpiredError
      transient_at     -- attempt raises TransientSourceError
      next_cursor_at   -- attempt returns the given cursor instead of the
                          natural one (used to force duplicates / rollbacks)
      extra_records_at -- attempt additionally re-serves these records
                          (redelivery inside an otherwise good page)
      time_fail_at     -- maps a window_id to the number of consecutive
                          transient failures before that window starts
                          succeeding (persistent across restarts only when
                          the test rebuilds the same script)
    """

    def __init__(
        self,
        records: Iterable[Record],
        page_size: int = 3,
        *,
        cursor_fail_at: dict[int, str] | None = None,
        transient_at: dict[int, str] | None = None,
        next_cursor_at: dict[int, str] | None = None,
        extra_records_at: dict[int, list[Record]] | None = None,
        time_fail_at: dict | None = None,
    ) -> None:
        # Keep a stable insertion order; callers may also include duplicates.
        self._records: list[Record] = list(records)
        self.page_size = page_size
        self.cursor_fail_at = cursor_fail_at or {}
        self.transient_at = transient_at or {}
        self.next_cursor_at = next_cursor_at or {}
        self.extra_records_at = extra_records_at or {}
        self.time_fail_at = time_fail_at or {}
        # Number of times each API was attempted (counters are part of state
        # so resume runs continue the same script).
        self.cursor_attempts = 0
        self._time_attempts: dict[str, int] = {}

    # ------------------------------------------------------------------ #
    # Cursor-based API (unreliable)
    # ------------------------------------------------------------------ #
    @staticmethod
    def encode_cursor(offset: int) -> str:
        return f"cur:{offset}"

    @staticmethod
    def decode_cursor(cursor: str) -> int:
        # A corrupted/foreign cursor is treated as expired by the source.
        if not cursor.startswith("cur:"):
            raise CursorExpiredError(f"unparseable cursor: {cursor!r}")
        try:
            return int(cursor[4:])
        except ValueError as exc:
            raise CursorExpiredError(str(exc)) from exc

    def fetch_page(self, cursor: str | None) -> FetchResult:
        attempt = self.cursor_attempts
        self.cursor_attempts += 1

        if attempt in self.cursor_fail_at:
            raise CursorExpiredError(self.cursor_fail_at[attempt])
        if attempt in self.transient_at:
            raise TransientSourceError(self.transient_at[attempt])

        offset = 0 if cursor is None else self.decode_cursor(cursor)
        page = self._records[offset:offset + self.page_size]
        end = offset + len(page)
        has_more = end < len(self._records)
        natural_next = self.encode_cursor(end) if has_more else None
        next_cursor = self.next_cursor_at.get(attempt, natural_next)

        if attempt in self.extra_records_at:
            # Redelivery: the server appends rows the client has already seen.
            page = page + list(self.extra_records_at[attempt])
        return FetchResult(page, next_cursor, has_more)

    # ------------------------------------------------------------------ #
    # Time-window API (reliable)
    # ------------------------------------------------------------------ #
    def fetch_window_page(
        self, window_id: str, start_ts: int, end_ts: int,
        offset: int,
    ) -> FetchResult:
        """Half-open window [start_ts, end_ts), paginated by row offset.

        Row-offset pagination inside a fixed, immutable time window is
        stable: the filtered row set cannot change between pages because the
        window never moves.  New writes land in later windows.
        """
        in_window = [r for r in self._records
                     if start_ts <= r.updated_at < end_ts]
        attempt = self._time_attempts.get(window_id, 0)
        self._time_attempts[window_id] = attempt + 1

        # Two scripting styles are accepted:
        #   {"w:25-50": 3}                      -> fail first 3 attempts
        #   {("w:25-50", 0): "msg", ...}       -> fail specific attempts
        rule = self.time_fail_at.get(window_id)
        if isinstance(rule, int):
            if attempt < rule:
                raise TransientSourceError(
                    f"window {window_id} transient failure "
                    f"(attempt {attempt + 1}/{rule})")
        else:
            key = (window_id, attempt)
            if key in self.time_fail_at:
                raise TransientSourceError(self.time_fail_at[key])

        page = in_window[offset:offset + self.page_size]
        new_offset = offset + len(page)
        has_more = new_offset < len(in_window)
        # Window cursors are self-describing and fully validated by the
        # engine before use, so they cannot silently roll back.
        next_cursor = (
            json.dumps({"w": window_id, "o": new_offset}) if has_more else None
        )
        return FetchResult(page, next_cursor, has_more)

    # ------------------------------------------------------------------ #
    # Introspection helpers used by the engine / tests
    # ------------------------------------------------------------------ #
    def max_updated_at(self) -> int:
        return max((r.updated_at for r in self._records), default=0)

    def cursor_at_timestamp(self, ts: int) -> str:
        """Return the natural cursor positioned just after records <= ts.

        Used when resuming a compensating run from scratch and when a
        compensation finishes and we re-enter cursor mode.
        """
        offset = sum(1 for r in self._records if r.updated_at <= ts)
        return self.encode_cursor(offset)

    def record_count(self) -> int:
        return len(self._records)


SourceFactory = Callable[[], ScriptedSource]
