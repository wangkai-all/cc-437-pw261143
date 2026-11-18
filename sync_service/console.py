"""HTTP console for the sync service (stdlib only).

Shows the four things operators need:

  * sync water level (target row count, max updated_at, mode/cursor state)
  * compensation windows and their lifecycle status
  * conflict records
  * per-request log (cursor, window, response summary, retry reason)

and exposes a "continue execution" entry (button + JSON API) that resumes
the durable state machine after an interruption or partial batch failure.
"""

from __future__ import annotations

import html
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .engine import SyncEngine
from .storage import Storage, utc_now_iso


def _esc(value) -> str:
    return html.escape("" if value is None else str(value), quote=True)


class ConsoleServer:
    def __init__(self, engine: SyncEngine, storage: Storage,
                 host: str = "127.0.0.1", port: int = 0) -> None:
        self.engine = engine
        self.storage = storage
        self.host = host
        self.port = port
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> str:
        handler = _make_handler(self.engine, self.storage)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True, name="sync-console")
        self._thread.start()
        return f"http://{self.host}:{self.port}/"

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None


def _make_handler(engine: SyncEngine, storage: Storage):
    class Handler(BaseHTTPRequestHandler):
        # Silence default stderr logging; the request_log table is the
        # authoritative audit trail instead.
        def log_message(self, fmt, *args):  # noqa: A003
            return

        # ---------------------------------------------------------- #
        # Routing
        # ---------------------------------------------------------- #
        def do_GET(self):  # noqa: N802
            route = urlparse(self.path).path
            if route == "/":
                self._html(200, _render_dashboard(engine, storage))
            elif route == "/api/state":
                self._json(200, _state_payload(engine, storage))
            elif route == "/api/windows":
                self._json(200, [dict(w) for w in storage.list_windows()])
            elif route == "/api/conflicts":
                self._json(200, [dict(c) for c in storage.list_conflicts()])
            elif route == "/api/requests":
                self._json(200, [dict(r) for r in storage.list_requests()])
            else:
                self._html(404, "<h1>404 Not Found</h1>")

        def do_POST(self):  # noqa: N802
            if urlparse(self.path).path != "/continue":
                self._html(404, "<h1>404 Not Found</h1>")
                return
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else b""
            wants_json = (
                "application/json" in (self.headers.get("Content-Type", ""))
                or b"json=1" in body)
            try:
                summary = engine.run()
            except RuntimeError as exc:
                if wants_json:
                    self._json(409, {"error": str(exc)})
                else:
                    self._html(409, f"<h1>Busy</h1><p>{_esc(exc)}</p>")
                return
            if wants_json:
                self._json(200, summary.as_dict())
            else:
                self._redirect("/")

        # ---------------------------------------------------------- #
        # Response helpers
        # ---------------------------------------------------------- #
        def _html(self, status: int, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _json(self, status: int, payload) -> None:
            data = json.dumps(payload, default=str, indent=2).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _redirect(self, location: str) -> None:
            self.send_response(303)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

    return Handler


# ---------------------------------------------------------------------- #
# Payloads / HTML
# ---------------------------------------------------------------------- #
def _state_payload(engine: SyncEngine, storage: Storage) -> dict:
    state = storage.get_sync_state()
    return {
        "running": engine.running,
        "mode": state.get("mode"),
        "in_compensation": state.get("in_compensation"),
        "cursor": state.get("cursor"),
        "committed_cursor": state.get("committed_cursor"),
        "safe_updated_at": state.get("safe_updated_at"),
        "water_level_updated_at": storage.water_level(),
        "target_rows": storage.target_count(),
        "conflicts": len(storage.list_conflicts()),
        "windows_total": len(storage.list_windows()),
        "runs": state.get("runs"),
        "last_run_at": state.get("last_run_at"),
        "server_time": utc_now_iso(),
    }


def _render_dashboard(engine: SyncEngine, storage: Storage) -> str:
    state = storage.get_sync_state()
    windows = storage.list_windows()
    conflicts = storage.list_conflicts()
    requests = storage.list_requests(limit=50)
    targets = storage.list_target(limit=20)

    win_rows = "".join(
        f"<tr><td>{_esc(w['window_id'])}</td>"
        f"<td>{w['start_ts']}</td><td>{w['end_ts']}</td>"
        f"<td class='st st-{_esc(w['status'])}'>{_esc(w['status'])}</td>"
        f"<td>{_esc(w['last_offset'])}</td><td>{_esc(w['error'] or '')}</td></tr>"
        for w in windows) or "<tr><td colspan=6>no compensation windows</td></tr>"

    con_rows = "".join(
        f"<tr><td>{_esc(c['detected_at'])}</td><td>{_esc(c['biz_key'])}</td>"
        f"<td>v{c['incoming_version']} → v{c['existing_version']}</td>"
        f"<td>{_esc(c['reason'])}</td><td><pre>{_esc(c['payload'])}</pre></td></tr>"
        for c in conflicts) or "<tr><td colspan=5>no conflicts</td></tr>"

    req_rows = "".join(
        f"<tr class='res-{_esc(r['result'])}'>"
        f"<td>{r['id']}</td><td>{_esc(r['ts'][11:23])}</td>"
        f"<td>{_esc(r['mode'])}</td><td>{_esc(r['window_id'] or '')}</td>"
        f"<td>{_esc(r['cursor'] or '')}</td>"
        f"<td>[{_esc(r['window_start'])},{_esc(r['window_end'])}) "
        f"off={_esc(r['page_offset'])}</td>"
        f"<td>{r['attempt']}</td><td>{r['row_count']}</td>"
        f"<td>{_esc(r['next_cursor'] or '')}</td>"
        f"<td><pre>{_esc(r['summary'])}</pre></td>"
        f"<td>{_esc(r['error_type'] or '')}</td>"
        f"<td>{_esc(r['retry_reason'] or '')}</td></tr>"
        for r in requests) or "<tr><td colspan=12>no requests yet</td></tr>"

    tgt_rows = "".join(
        f"<tr><td>{_esc(t['biz_key'])}</td><td>v{t['version']}</td>"
        f"<td>{t['updated_at']}</td><td><pre>{_esc(t['payload'])}</pre></td></tr>"
        for t in targets) or "<tr><td colspan=4>target is empty</td></tr>"

    busy = engine.running
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Sync Console</title>
<style>
 body {{ font-family: ui-monospace, Menlo, Consolas, monospace;
        margin: 24px; color: #1a2330; background: #f7f8fa; }}
 h1 {{ font-size: 20px; }} h2 {{ font-size: 15px; margin-top: 28px; }}
 table {{ border-collapse: collapse; width: 100%; background: #fff;
         font-size: 12px; }}
 th, td {{ border: 1px solid #d6dae0; padding: 5px 8px;
          text-align: left; vertical-align: top; }}
 th {{ background: #eef1f5; }}
 pre {{ margin: 0; white-space: pre-wrap; word-break: break-all;
       max-width: 420px; }}
 .cards {{ display: flex; gap: 12px; flex-wrap: wrap; }}
 .card {{ background: #fff; border: 1px solid #d6dae0; border-radius: 8px;
         padding: 12px 16px; min-width: 150px; }}
 .card b {{ display: block; font-size: 22px; margin-top: 4px; }}
 .st {{ font-weight: 700; padding: 1px 6px; border-radius: 10px; }}
 .st-done {{ background: #d9f2e3; color: #146c3b; }}
 .st-running {{ background: #fff0c9; color: #8a6100; }}
 .st-pending {{ background: #e7ecf3; color: #3a4658; }}
 .st-failed {{ background: #fde0e0; color: #9c1f1f; }}
 .res-error, .res-compensation {{ background: #fff4f4; }}
 form {{ margin: 16px 0; }}
 button {{ padding: 10px 22px; font-size: 14px; border-radius: 8px;
          border: 1px solid #2457c5; background: #2457c5; color: #fff;
          cursor: pointer; }}
 button:disabled {{ background: #9db2d8; border-color: #9db2d8;
                   cursor: not-allowed; }}
 code {{ background: #eef1f5; padding: 1px 5px; border-radius: 4px; }}
</style></head><body>
<h1>Incremental Sync Console</h1>
<div class="cards">
  <div class="card">Mode<b>{_esc(state.get('mode'))}</b></div>
  <div class="card">Compensating
    <b>{'yes' if state.get('in_compensation') else 'no'}</b></div>
  <div class="card">Water level (updated_at)
    <b>{storage.water_level()}</b></div>
  <div class="card">Target rows<b>{storage.target_count()}</b></div>
  <div class="card">Conflicts<b>{len(conflicts)}</b></div>
  <div class="card">Windows<b>{len(windows)}</b></div>
  <div class="card">Runs<b>{_esc(state.get('runs'))}</b></div>
  <div class="card">Engine
    <b>{'running' if busy else 'idle'}</b></div>
</div>

<form method="post" action="/continue">
  <button type="submit" {'disabled' if busy else ''}>
    ▶ Continue execution (resume)</button>
  <code>POST /continue</code> &nbsp;
  <code>POST /continue (JSON) → run summary</code>
</form>

<h2>Durable sync state</h2>
<table><tr><th>cursor</th><th>committed_cursor</th>
  <th>safe_updated_at</th><th>resume_window_id</th><th>last_run_at</th></tr>
<tr><td>{_esc(state.get('cursor'))}</td>
  <td>{_esc(state.get('committed_cursor'))}</td>
  <td>{_esc(state.get('safe_updated_at'))}</td>
  <td>{_esc(state.get('resume_window_id'))}</td>
  <td>{_esc(state.get('last_run_at'))}</td></tr></table>

<h2>Compensation windows (half-open [start, end))</h2>
<table><tr><th>window</th><th>start_ts</th><th>end_ts</th><th>status</th>
  <th>last_offset</th><th>error</th></tr>{win_rows}</table>

<h2>Conflict records</h2>
<table><tr><th>detected_at</th><th>biz_key</th><th>versions</th>
  <th>reason</th><th>incoming payload</th></tr>{con_rows}</table>

<h2>Target preview (first 20 rows, unique on biz_key+version)</h2>
<table><tr><th>biz_key</th><th>version</th><th>updated_at</th>
  <th>payload</th></tr>{tgt_rows}</table>

<h2>Request log (latest 50; full audit trail in request_log)</h2>
<table><tr><th>#</th><th>time</th><th>mode</th><th>window</th>
  <th>cursor</th><th>window / offset</th><th>attempt</th>
  <th>rows</th><th>next_cursor</th><th>response summary</th>
  <th>error</th><th>retry reason</th></tr>{req_rows}</table>
</body></html>"""
