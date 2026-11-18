"""Resume / interruption fixtures: crashes and partial batch failures."""

from __future__ import annotations

import os

from tests.fixtures import CrashAfter, SyncTestCase, make_records
from sync_service.engine import SyncEngine
from sync_service.storage import Storage


class CrashResumeTests(SyncTestCase):
    def test_crash_before_commit_reapplies_batch_on_resume(self):
        records = make_records(9)
        hook = CrashAfter("cur::cur:3", "before_commit")

        engine1 = self.make_engine(
            self.fresh_source(records), crash_hook=hook)
        with self.assertRaises(RuntimeError):
            engine1.run()
        self.assertTrue(hook.fired)

        # Nothing from that batch is durable (rolled back).
        self.assertFalse(self.storage.batch_exists("cur::cur:3"))
        rows_after_crash = self.storage.target_count()
        self.assertEqual(rows_after_crash, 3)  # only the first page

        # Restart: fresh source object, same db file.
        summary = self.make_engine(self.fresh_source(records)).run()
        self.assertEqual(summary.reason, "caught_up")
        self.assert_target_has_all_versions(records)

    def test_crash_after_commit_skips_batch_on_resume(self):
        records = make_records(9)
        hook = CrashAfter("cur::cur:3", "after_commit")
        engine1 = self.make_engine(
            self.fresh_source(records), crash_hook=hook)
        with self.assertRaises(RuntimeError):
            engine1.run()
        self.assertTrue(hook.fired)

        # The batch *is* durable even though the caller crashed afterwards.
        self.assertTrue(self.storage.batch_exists("cur::cur:3"))
        rows_after_crash = self.storage.target_count()
        self.assertEqual(rows_after_crash, 6)

        summary = self.make_engine(self.fresh_source(records)).run()
        self.assertEqual(summary.reason, "caught_up")
        # Exactly one physical row per version; the durable batch was skipped.
        self.assert_target_has_all_versions(records)
        self.assert_no_duplicate_batches()

    def test_crash_during_compensation_resumes_mid_window(self):
        records = make_records(12)
        # Expire the cursor on the second page -> compensation starts; then
        # kill the process after the first window page is durable.
        first_window_batch: list[str] = []

        def hook(batch_id: str, phase: str) -> None:
            if phase == "after_commit" and batch_id.startswith("win:") \
                    and not first_window_batch:
                first_window_batch.append(batch_id)
                raise RuntimeError(f"SIMULATED CRASH after {batch_id}")

        source1 = self.fresh_source(
            records, cursor_fail_at={1: "expired"})
        engine1 = self.make_engine(source1, crash_hook=hook)
        with self.assertRaises(RuntimeError):
            engine1.run()
        self.assertEqual(len(first_window_batch), 1)

        state = self.storage.get_sync_state()
        self.assertTrue(state["in_compensation"])
        running = [w for w in self.storage.list_windows()
                   if w["status"] == "running"]
        self.assertEqual(len(running), 1)

        # Resume with a recovered source; windows pick up at last_offset.
        source2 = self.fresh_source(
            records, cursor_fail_at={1: "expired"})
        summary = self.make_engine(source2).run()
        self.assertIsNone(summary.error)
        self.assert_target_has_all_versions(records)
        self.assertTrue(all(w["status"] == "done"
                            for w in self.storage.list_windows()))
        self.assertFalse(
            self.storage.get_sync_state()["in_compensation"])

    def test_interrupted_then_repeated_resumes_converge(self):
        """Two crashes in a row must still converge with no duplicates."""
        records = make_records(12)
        hook1 = CrashAfter("cur::cur:3", "after_commit")
        with self.assertRaises(RuntimeError):
            self.make_engine(
                self.fresh_source(records), crash_hook=hook1).run()

        hook2 = CrashAfter("cur::cur:6", "before_commit")
        with self.assertRaises(RuntimeError):
            self.make_engine(
                self.fresh_source(records), crash_hook=hook2).run()

        summary = self.make_engine(self.fresh_source(records)).run()
        self.assertEqual(summary.reason, "caught_up")
        self.assert_target_has_all_versions(records)
        self.assert_no_duplicate_batches()


class PartialBatchFailureTests(SyncTestCase):
    def test_failed_window_does_not_poison_good_windows_then_retries(self):
        records = make_records(12)
        # Generate the exact window ids the engine will use by running a
        # throwaway probe in an isolated database; window ids are
        # deterministic functions of anchor/window_size.
        probe_db = os.path.join(self._tmp.name, "probe.db")
        probe_storage = Storage(probe_db)
        probe_engine = SyncEngine(
            self.fresh_source(records, cursor_fail_at={1: "expired"}),
            probe_storage, window_size=self.WINDOW_SIZE)
        probe_engine.run()
        window_ids = [w["window_id"] for w in probe_storage.list_windows()]
        probe_storage.close()
        self.assertGreaterEqual(len(window_ids), 3)
        middle = window_ids[1]

        source1 = self.fresh_source(
            records, cursor_fail_at={1: "expired"},
            time_fail_at={middle: 6})
        engine1 = self.make_engine(source1, max_retries=2)
        summary1 = engine1.run()

        self.assertEqual(summary1.reason, "compensation_failed")
        windows = {w["window_id"]: w["status"]
                   for w in self.storage.list_windows()}
        self.assertEqual(windows[middle], "failed")
        # Earlier window completed; the engine stops at the failed one, so
        # later windows stay pending — nothing fabricated, nothing skipped.
        self.assertEqual(windows[window_ids[0]], "done")
        self.assertEqual(windows[window_ids[2]], "pending")

        # Every retry reason is recorded for operators.
        retries = [r for r in self.storage.list_requests()
                   if r["retry_reason"] and "transient" in r["retry_reason"]
                   and r["window_id"] == middle]
        self.assertEqual(len(retries), 3)  # initial + 2 retries

        # Continue after recovery: failed window retries, pending ones run.
        source2 = self.fresh_source(
            records, cursor_fail_at={1: "expired"})
        summary2 = self.make_engine(source2, max_retries=2).run()
        self.assertEqual(summary2.reason, "caught_up")
        self.assert_target_has_all_versions(records)
        statuses = [w["status"] for w in self.storage.list_windows()]
        self.assertTrue(all(s == "done" for s in statuses))

    def test_cursor_page_with_mixed_redelivery_still_commits_once(self):
        records = make_records(6)
        # Page 1 contains 3 fresh rows + 2 redelivered; atomic batch apply
        # must count dedup but never double-insert.
        source = self.fresh_source(
            records, extra_records_at={1: [records[3], records[4]]})
        summary = self.make_engine(source).run()
        self.assertEqual(summary.reason, "caught_up")
        self.assert_target_has_all_versions(records)
        batch = next(b for b in self.storage.list_batches()
                     if b["batch_id"] == "cur::cur:3")
        self.assertEqual(batch["record_count"], 5)
        self.assertEqual(batch["dedup_count"], 2)
