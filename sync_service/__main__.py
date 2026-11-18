"""Command-line entry points.

Usage:
    python -m sync_service demo --db data/demo.db [--port 8080]
        Build a scripted source full of cursor pathologies, run the sync
        (with a simulated crash mid-way), resume it, then serve the console.

    python -m sync_service console --db data/demo.db [--port 8080]
        Serve the console against an existing database.
"""

from __future__ import annotations

import argparse
import time

from .console import ConsoleServer
from .engine import SyncEngine
from .source import Record, ScriptedSource
from .storage import Storage


def build_demo_source() -> ScriptedSource:
    # 12 records, updated_at from t=10..120, page_size=3.
    records = [
        Record(f"key-{i:02d}", 1, 10 * (i + 1),
               {"seq": i, "note": "initial version"})
        for i in range(12)
    ]
    # key-03 gets a newer version; key-07 will later arrive *stale*.
    records[3] = Record("key-03", 2, 40, {"note": "updated"})
    stale = Record("key-03", 1, 95, {"note": "late redelivery of old version"})

    return ScriptedSource(
        records + [stale],
        page_size=3,
        # Attempt 2: server rejects the cursor (expired).
        cursor_fail_at={2: "cursor TTL elapsed on source side"},
        # Attempt 5: transient network blip, succeeds on retry.
        transient_at={5: "connection reset by peer"},
        # Attempt 7: server hands back a cursor pointing backwards.
        next_cursor_at={7: ScriptedSource.encode_cursor(6)},
        # Attempt 4: server redelivers rows the client already applied.
        extra_records_at={4: [records[0], records[1]]},
        # The window covering t=90..100 fails twice, then succeeds.
        time_fail_at={
            ("w:90-100", 0): "window read timeout",
            ("w:90-100", 1): "window read timeout",
        },
    )


def _crash_demo(crash_after_batches: int):
    """Return a hook that raises after N batches have committed."""
    state = {"committed": 0}

    def hook(batch_id: str, phase: str) -> None:
        if phase == "after_commit":
            state["committed"] += 1
            if state["committed"] == crash_after_batches:
                raise RuntimeError(
                    f"SIMULATED CRASH after committing {batch_id}")

    return hook


def run_demo(db_path: str, port: int | None) -> None:
    storage = Storage(db_path)
    source = build_demo_source()

    print("=== run 1: crash after the 2nd committed batch ===")
    engine = SyncEngine(source, storage, window_size=30,
                        crash_hook=_crash_demo(2))
    try:
        engine.run()
    except RuntimeError as exc:
        print(f"   interrupted: {exc}")
    print(f"   target rows after crash: {storage.target_count()}")

    print("=== run 2: resume (same source script, same durable state) ===")
    engine2 = SyncEngine(source, storage, window_size=30)
    summary = engine2.run()
    print(f"   finished: {summary.as_dict()}")
    print(f"   target rows: {storage.target_count()} "
          f"(source rows incl. redelivery: {source.record_count()})")
    print(f"   conflicts recorded: {len(storage.list_conflicts())}")
    print(f"   request log entries: {storage.count_requests()}")

    if port is not None:
        server = ConsoleServer(SyncEngine(source, storage, window_size=30),
                               storage, port=port)
        url = server.start()
        print(f"\nconsole: {url}  (Ctrl-C to stop)")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop()
    storage.close()


def run_console(db_path: str, port: int) -> None:
    storage = Storage(db_path)
    engine = SyncEngine(ScriptedSource([], page_size=3), storage)
    server = ConsoleServer(engine, storage, port=port)
    url = server.start()
    print(f"console: {url}  (Ctrl-C to stop)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
    storage.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sync_service")
    sub = parser.add_subparsers(dest="cmd", required=True)

    demo = sub.add_parser("demo", help="run the scripted demo scenario")
    demo.add_argument("--db", default="data/demo.db")
    demo.add_argument("--port", type=int, default=8080)
    demo.add_argument("--no-serve", action="store_true")

    console = sub.add_parser("console", help="serve console only")
    console.add_argument("--db", required=True)
    console.add_argument("--port", type=int, default=8080)

    args = parser.parse_args(argv)
    if args.cmd == "demo":
        run_demo(args.db, None if args.no_serve else args.port)
    elif args.cmd == "console":
        run_console(args.db, args.port)


if __name__ == "__main__":
    main()
