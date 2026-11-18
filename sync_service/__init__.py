"""Incremental sync service with unreliable cursors and idempotent target.

Modules:
    source  -- source API abstraction and a scriptable anomaly-injecting fake.
    storage -- durable sqlite state (watermark, target rows, windows, logs...).
    engine  -- sync orchestration: cursor mode, rollback compensation, resume.
    console -- HTTP dashboard and "continue execution" entry point.
"""

__version__ = "1.0.0"
