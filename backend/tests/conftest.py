"""Conftest for backend/tests/ — ensures sys.path includes the project root
so `from backend...` imports resolve when pytest is invoked with this dir
explicitly (e.g., `pytest backend/tests/`). The top-level `tests/conftest.py`
is not loaded for this path because `testpaths = tests` in pytest.ini scopes
the default run; this file makes the aux suite trustworthy on cold-run.
"""
import os
import sys

# telegram-catalog/ is two levels up from backend/tests/
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ.setdefault("ADMIN_API_KEY", "test-admin-key")
