#!/usr/bin/env python3
"""Test runner compatible with `python -I` (isolated mode).

Usage:
    python3 -I run_tests.py                 # whole suite, verbose
    python3 -I run_tests.py TestName        # dotted names or substrings
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main() -> int:
    loader = unittest.TestLoader()
    if len(sys.argv) > 1:
        suite = unittest.TestSuite()
        for name in sys.argv[1:]:
            suite.addTests(loader.loadTestsFromName(name))
    else:
        suite = loader.discover("tests")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
