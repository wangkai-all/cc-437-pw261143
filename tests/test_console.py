"""Console tests: dashboard rendering, JSON APIs and the continue entry."""

from __future__ import annotations

import json
import urllib.request

from tests.fixtures import SyncTestCase, make_records
from sync_service.console import ConsoleServer


class ConsoleTests(SyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        records = make_records(9)
        # One expired cursor so the dashboard has windows + retry rows.
        self.source = self.fresh_source(
            records, cursor_fail_at={1: "expired in test"})
        engine = self.make_engine(self.source)
        engine.run()
        self.server = ConsoleServer(
            self.make_engine(self.source), self.storage, port=0)
        self.base = self.server.start()

    def tearDown(self) -> None:
        self.server.stop()
        super().tearDown()

    def _get(self, path: str):
        with urllib.request.urlopen(self.base + path.lstrip("/"),
                                    timeout=5) as resp:
            return resp.status, resp.headers.get("Content-Type"), resp.read()

    def test_dashboard_renders_all_four_views(self):
        status, ctype, body = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", ctype)
        text = body.decode()
        for needle in ("Water level", "Compensation windows",
                       "Conflict records", "Request log",
                       "Continue execution"):
            self.assertIn(needle, text)
        # Window rows and request summaries are actually populated.
        self.assertIn("w:", text)
        self.assertIn("cursor_expired", text)

    def test_state_api_reports_water_level_and_counts(self):
        _, _, body = self._get("/api/state")
        payload = json.loads(body)
        self.assertEqual(payload["mode"], "cursor")
        self.assertFalse(payload["in_compensation"])
        self.assertEqual(payload["target_rows"], 9)
        self.assertEqual(payload["water_level_updated_at"], 90)
        self.assertGreaterEqual(payload["runs"], 1)

    def test_windows_and_requests_apis(self):
        _, _, body = self._get("/api/windows")
        windows = json.loads(body)
        self.assertTrue(windows)
        self.assertTrue(all(w["status"] == "done" for w in windows))

        _, _, body = self._get("/api/requests")
        requests = json.loads(body)
        # Every source request is audited with cursor/window + summary.
        self.assertTrue(all("summary" in r for r in requests))
        self.assertTrue(any(r["mode"] == "cursor" for r in requests))
        self.assertTrue(any(r["mode"] == "window" for r in requests))

    def test_continue_endpoint_resumes_a_caught_up_engine(self):
        req = urllib.request.Request(
            self.base + "continue",
            data=b"", method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            payload = json.loads(resp.read())
        # Nothing new to apply: all batches skipped as dedup.
        self.assertEqual(payload["inserted"], 0)
        self.assertEqual(payload["deduped"], 9)

    def test_conflicts_api_lists_stale_versions(self):
        from tests.fixtures import records_with_versions
        # Separate storage for this scenario.
        import os
        import tempfile
        from sync_service.storage import Storage
        from sync_service.engine import SyncEngine

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = os.path.join(tmp.name, "c.db")
        storage = Storage(db)
        source = self.fresh_source(records_with_versions())
        SyncEngine(source, storage, window_size=self.WINDOW_SIZE).run()

        server = ConsoleServer(
            SyncEngine(source, storage, window_size=self.WINDOW_SIZE),
            storage, port=0)
        base = server.start()
        try:
            with urllib.request.urlopen(base + "api/conflicts",
                                        timeout=5) as resp:
                conflicts = json.loads(resp.read())
        finally:
            server.stop()
            storage.close()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["reason"], "stale_version")
