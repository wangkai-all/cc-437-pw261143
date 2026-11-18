"""Deterministic fixtures shared by all test modules.

A fixture builds (1) a record set with boundary timestamps and duplicate /
stale rows and (2) a ScriptedSource preloaded with the exact failure
script each scenario needs.  Sources are stateless to rebuild: after a
simulated crash the test constructs a fresh source (same script) against
the same sqlite file, mirroring a process restart.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

# Allow `python -I -m unittest` from the repo root without packaging.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from sync_service.source import Record, ScriptedSource  # noqa: E402
from sync_service.storage import Storage  # noqa: E402
from sync_service.engine import SyncEngine  # noqa: E402


def make_records(n: int = 12, *, step: int = 10,
                 start: int = 10) -> list[Record]:
    """n records at timestamps start, start+step, ... (boundary-friendly)."""
    return [
        Record(f"key-{i:02d}", 1, start + i * step, {"seq": i})
        for i in range(n)
    ]


def records_with_versions() -> list[Record]:
    """Includes a higher re-write and a late stale redelivery.

    The stale v1 of key-02 is the *last* record in the source so it lands
    in a fresh cursor page that was never applied before, exercising the
    real conflict path rather than a whole-batch dedup skip.
    """
    records = make_records(8)
    records[2] = Record("key-02", 2, 30, {"seq": 2, "note": "rewritten"})
    stale = Record("key-02", 1, 95, {"seq": 2, "note": "stale redelivery"})
    return records[:-1] + [stale] + [records[-1]]


class SyncTestCase(unittest.TestCase):
    """Base case wiring a temp sqlite db and engine factories."""

    WINDOW_SIZE = 25  # forces multiple windows over a 0..120 timeline

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "sync.db")
        self.storage = Storage(self.db_path)

    def tearDown(self) -> None:
        self.storage.close()
        self._tmp.cleanup()

    def make_engine(self, source: ScriptedSource, **kwargs) -> SyncEngine:
        kwargs.setdefault("window_size", self.WINDOW_SIZE)
        return SyncEngine(source, self.storage, **kwargs)

    def fresh_source(self, records, **script) -> ScriptedSource:
        return ScriptedSource(records, page_size=3, **script)

    # ------------------------------------------------------------------ #
    # Assertions
    # ------------------------------------------------------------------ #
    def assert_target_has_all_versions(self, records: list[Record],
                                       *, except_keys: set | None = None
                                       ) -> None:
        """Every distinct (biz_key, version) must exist exactly once.

        Versions listed in ``except_keys`` are expected to have been
        rejected as conflicts and therefore must NOT be in the target.
        """
        distinct = {(r.biz_key, r.version) for r in records
                    if (r.biz_key, r.version) not in (except_keys or set())}
        rows = self.storage.list_target(limit=10_000)
        got = {(r["biz_key"], r["version"]) for r in rows}
        self.assertEqual(got, distinct)
        self.assertEqual(self.storage.target_count(), len(distinct))

    def assert_no_duplicate_batches(self) -> None:
        batches = self.storage.list_batches()
        ids = [b["batch_id"] for b in batches]
        self.assertEqual(len(ids), len(set(ids)),
                         "batch ids must be unique")


class CrashAfter:
    """Crash hook factory: simulates process death around commit points.

    before_commit: the transaction is rolled back; on resume the batch is
                   fully re-applied.
    after_commit:  the batch is durable but the caller never sees the
                   response; on resume apply_batch skips it by batch_id.
    """

    def __init__(self, batch_id: str, phase: str) -> None:
        self.batch_id = batch_id
        self.phase = phase
        self.fired = False

    def __call__(self, batch_id: str, phase: str) -> None:
        if not self.fired and batch_id == self.batch_id and phase == self.phase:
            self.fired = True
            raise RuntimeError(
                f"SIMULATED CRASH phase={phase} batch={batch_id}")
