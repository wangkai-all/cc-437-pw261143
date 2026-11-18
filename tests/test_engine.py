"""Engine behaviour: cursor anomalies, idempotency, boundaries, conflicts."""

from __future__ import annotations

import os

from tests.fixtures import (
    SyncTestCase,
    make_records,
    records_with_versions,
)
from sync_service.source import (
    CursorExpiredError,
    Record,
    ScriptedSource,
    TransientSourceError,
)
from sync_service.storage import Storage
from sync_service.engine import SyncEngine


class HappyPathTests(SyncTestCase):
    def test_full_sync_applies_every_version_once(self):
        records = make_records(12)
        engine = self.make_engine(self.fresh_source(records))
        summary = engine.run()

        self.assertEqual(summary.reason, "caught_up")
        self.assert_target_has_all_versions(records)
        # 12 records / page_size 3 = 4 cursor batches.
        self.assertEqual(summary.cursor_batches, 4)
        self.assertEqual(self.storage.get_sync_state()["mode"], "cursor")

    def test_rerun_is_a_noop(self):
        records = make_records(9)
        engine = self.make_engine(self.fresh_source(records))
        engine.run()
        rows_after_first = self.storage.target_count()

        # A brand-new source instance (e.g. restart) re-serves everything.
        summary2 = self.make_engine(self.fresh_source(records)).run()
        self.assertEqual(self.storage.target_count(), rows_after_first)
        self.assertEqual(summary2.inserted, 0)
        self.assertEqual(summary2.deduped, 9)
        self.assert_no_duplicate_batches()

    def test_redelivered_rows_inside_a_page_are_deduped(self):
        records = make_records(6)
        source = self.fresh_source(
            records, extra_records_at={1: [records[0], records[1]]})
        summary = self.make_engine(source).run()

        self.assert_target_has_all_versions(records)
        self.assertGreaterEqual(summary.deduped, 2)
        # The redelivery must be visible in the response summary log.
        log = self.storage.list_requests()
        self.assertTrue(any(
            "key-00#v1" in (r["summary"] or "") and r["row_count"] > 3
            for r in log))


class CursorAnomalyTests(SyncTestCase):
    def test_expired_cursor_triggers_window_compensation(self):
        records = make_records(12)
        source = self.fresh_source(
            records, cursor_fail_at={2: "TTL expired"})
        summary = self.make_engine(source).run()

        self.assert_target_has_all_versions(records)
        self.assertGreater(len(self.storage.list_windows()), 0)
        # The expired-cursor error and its retry reason must be auditable.
        log = self.storage.list_requests()
        self.assertTrue(any(r["error_type"] == "CursorExpiredError" for r in log))
        self.assertTrue(any(
            "cursor_expired" in (r["retry_reason"] or "") for r in log))
        # Compensated rows end in cursor mode again, fully caught up.
        state = self.storage.get_sync_state()
        self.assertFalse(state["in_compensation"])
        self.assertEqual(state["mode"], "cursor")
        self.assertTrue(all(w["status"] == "done"
                            for w in self.storage.list_windows()))

    def test_duplicate_cursor_is_detected(self):
        records = make_records(9)
        # Source fetch attempts are 0-based. Natural sequence:
        # attempt 0 -> cur:3, attempt 1 (inbound cur:3) -> cur:6,
        # attempt 2 -> None.  Force attempt 1 to echo its inbound cursor
        # cur:3 instead of advancing: no progress -> duplicate anomaly.
        source = self.fresh_source(
            records,
            next_cursor_at={1: ScriptedSource.encode_cursor(3)})
        self.make_engine(source).run()

        self.assert_target_has_all_versions(records)
        log = self.storage.list_requests()
        self.assertTrue(any(
            r["retry_reason"] == "duplicate_cursor" for r in log))

    def test_cursor_rollback_is_detected_and_compensated(self):
        records = make_records(12)
        # Attempt 3 hands back cur:6 although we are already at cur:9.
        source = self.fresh_source(
            records,
            next_cursor_at={3: ScriptedSource.encode_cursor(6)})
        summary = self.make_engine(source).run()

        self.assert_target_has_all_versions(records)
        log = self.storage.list_requests()
        self.assertTrue(any(
            r["retry_reason"] == "cursor_rollback" for r in log))
        self.assertGreater(summary.window_batches, 0)
        # No row may be applied twice despite re-reading overlapping ranges.
        self.assert_no_duplicate_batches()

    def test_unparseable_cursor_is_treated_as_rollback(self):
        records = make_records(9)
        # A foreign/garbage cursor the client cannot decode at all.
        source = self.fresh_source(
            records, next_cursor_at={1: "opaque-token-xyz"})
        self.make_engine(source).run()

        self.assert_target_has_all_versions(records)
        log = self.storage.list_requests()
        self.assertTrue(any(
            r["retry_reason"] == "unparseable_cursor" for r in log))
        self.assertTrue(all(w["status"] == "done"
                            for w in self.storage.list_windows()))

    def test_transient_error_is_retried_and_logged(self):
        records = make_records(6)
        source = self.fresh_source(
            records, transient_at={1: "connection reset"})
        summary = self.make_engine(source).run()

        self.assertEqual(summary.reason, "caught_up")
        self.assert_target_has_all_versions(records)
        log = self.storage.list_requests()
        errors = [r for r in log
                  if r["error_type"] == "TransientSourceError"]
        self.assertEqual(len(errors), 1)
        self.assertIn("transient", errors[0]["retry_reason"])

    def test_exhausted_retries_leave_durable_resume_state(self):
        records = make_records(6)
        # Every attempt on the second logical page fails -> give up cleanly.
        source = self.fresh_source(
            records, transient_at={1: "x", 2: "x", 3: "x", 4: "x"})
        engine = self.make_engine(source, max_retries=3)
        summary = engine.run()

        self.assertEqual(summary.reason, "transient_retries_exhausted")
        rows_before = self.storage.target_count()

        # After the source recovers, "continue" finishes the job.
        source2 = self.fresh_source(records)
        summary2 = self.make_engine(source2, max_retries=3).run()
        self.assertEqual(summary2.reason, "caught_up")
        self.assert_target_has_all_versions(records)
        self.assertEqual(self.storage.target_count(),
                         rows_before + (len(records) - rows_before))


class BoundaryAndOverlapTests(SyncTestCase):
    def test_window_boundaries_are_half_open_and_complete(self):
        # Timestamps 10,20,...,120; window size 25 -> boundaries at 25,50,...
        # Rows at exactly 100 must land in [100,125), never skipped/doubled.
        records = make_records(12)
        source = self.fresh_source(
            records, cursor_fail_at={1: "expired immediately"})
        self.make_engine(source).run()
        self.assert_target_has_all_versions(records)

        windows = self.storage.list_windows()
        self.assertTrue(windows)
        # Adjacent windows share no row: end of one == start of next.
        for a, b in zip(windows, windows[1:]):
            self.assertEqual(a["end_ts"], b["start_ts"])
        # Every source timestamp beyond the durable anchor falls inside
        # exactly one window; nothing is skipped or doubled.
        anchor = self.storage.get_sync_state()["safe_updated_at"]
        for rec in records:
            if rec.updated_at <= anchor:
                continue
            containing = [w for w in windows
                          if w["start_ts"] <= rec.updated_at < w["end_ts"]]
            self.assertEqual(len(containing), 1,
                             f"{rec.updated_at} not in exactly one window")

    def test_overlapping_windows_create_no_duplicates(self):
        # First run: expire the cursor early, compensate, cover up to ~60.
        records = make_records(6)
        source1 = self.fresh_source(
            records, cursor_fail_at={1: "expired"})
        self.make_engine(source1).run()
        self.assert_target_has_all_versions(records)
        first_batches = {b["batch_id"] for b in self.storage.list_batches()}

        # Force a *second* compensation over an overlapping range by feeding
        # another expired cursor to a fresh source at the next run.
        source2 = self.fresh_source(
            records, cursor_fail_at={0: "expired again"})
        summary2 = self.make_engine(source2).run()

        self.assert_target_has_all_versions(records)
        # The second run's batches may reuse ids (same offsets) — all skips.
        self.assertEqual(summary2.inserted, 0)
        self.assertGreater(summary2.deduped, 0)
        self.assert_no_duplicate_batches()
        self.assertTrue(first_batches.issubset(
            {b["batch_id"] for b in self.storage.list_batches()}))

    def test_window_transient_failure_marks_window_failed_then_resumes(self):
        records = make_records(12)
        window_size = 25
        # Probe the deterministic window ids before the failing run.
        probe_storage = Storage(os.path.join(self._tmp.name, "probe.db"))
        SyncEngine(
            self.fresh_source(records, cursor_fail_at={1: "expired"}),
            probe_storage, window_size=window_size).run()
        first_window = probe_storage.list_windows()[0]["window_id"]
        probe_storage.close()

        # The first window fails persistently (beyond the retry budget).
        source = self.fresh_source(
            records,
            cursor_fail_at={1: "expired"},
            time_fail_at={first_window: 5})
        engine = self.make_engine(source, window_size=window_size,
                                  max_retries=2)
        summary = engine.run()
        self.assertEqual(summary.reason, "compensation_failed")
        failed = [w for w in self.storage.list_windows()
                  if w["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["window_id"], first_window)
        self.assertIn("transient failure", failed[0]["error"])

        # Recovered source: the failed window retries and the run completes.
        source2 = self.fresh_source(
            records, cursor_fail_at={1: "expired"})
        summary2 = self.make_engine(source2, window_size=window_size,
                                    max_retries=2).run()
        self.assertIsNone(summary2.error)
        self.assert_target_has_all_versions(records)
        self.assertTrue(all(w["status"] == "done"
                            for w in self.storage.list_windows()))


class ConflictTests(SyncTestCase):
    def test_stale_version_is_conflict_not_overwrite(self):
        records = records_with_versions()  # key-02 v2 then late v1
        summary = self.make_engine(self.fresh_source(records)).run()

        # The stale v1 is rejected: only the newest version survives as a
        # current row, and the rejection is recorded for audit.
        self.assert_target_has_all_versions(
            records, except_keys={("key-02", 1)})
        conflicts = self.storage.list_conflicts()
        self.assertEqual(len(conflicts), 1)
        c = conflicts[0]
        self.assertEqual(c["biz_key"], "key-02")
        self.assertEqual(c["incoming_version"], 1)
        self.assertEqual(c["existing_version"], 2)
        self.assertEqual(c["reason"], "stale_version")
        self.assertEqual(summary.conflicts, 1)

    def test_higher_version_replaces_latest_but_keeps_history(self):
        records = [
            Record("k", 1, 10, {}),
            Record("k", 2, 20, {}),
            Record("k", 3, 30, {}),
        ]
        self.make_engine(self.fresh_source(records)).run()
        rows = [r for r in self.storage.list_target() if r["biz_key"] == "k"]
        self.assertEqual([r["version"] for r in rows], [1, 2, 3])
        self.assertEqual(self.storage.list_conflicts(), [])
