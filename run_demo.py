#!/usr/bin/env python3
"""Convenience launcher runnable under `python -I` from the repo root.

Equivalent to `python -m sync_service demo ...` but works in isolated mode
(which omits the cwd from sys.path).  Forwards all arguments.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sync_service.__main__ import main  # noqa: E402

if __name__ == "__main__":
    main()
