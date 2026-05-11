#!/usr/bin/env python3
"""Enforce the frontend bundle-size budget set in CLAUDE.md.

Per Hard Constraints (foundation audit 2026-05-10):
    < 400K JS + < 50K CSS for the main chunk.

Why these specific numbers: the Tailwind ratification capped the discussion
of "what bundle discipline means now." 400K JS leaves headroom for the
Mini-App's seven pages without budget-creep enabling a UI framework slide.
50K CSS reflects what Tailwind tree-shakes to today (28K) with headroom.

Code-split chunks (MapPicker, leaflet) are intentionally excluded from
this check — they only load on the Cart page's location picker.

Exit codes:
    0   all main-chunk files within budget
    1   one or more files over budget (fail CI / pre-commit)
    2   frontend/dist/assets/ not found — operator forgot to run vite build
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DIST = REPO_ROOT / "frontend" / "dist" / "assets"

# Budget in bytes. Match the values stated in CLAUDE.md "Hard Constraints".
MAX_JS_BYTES = 400 * 1024
MAX_CSS_BYTES = 50 * 1024


def _kb(n: int) -> str:
    return f"{n / 1024:.1f}K"


def main() -> int:
    if not DIST.exists():
        print(f"ERROR: {DIST} not found. Run `cd frontend && npm run build` first.", file=sys.stderr)
        return 2

    main_js = sorted(DIST.glob("index-*.js"))
    main_css = sorted(DIST.glob("index-*.css"))

    if not main_js:
        print(f"ERROR: no main JS chunk (index-*.js) found in {DIST}", file=sys.stderr)
        return 2

    over_budget: list[str] = []
    print(f"Bundle size check (budget: {_kb(MAX_JS_BYTES)} JS / {_kb(MAX_CSS_BYTES)} CSS for main chunk):")

    for f in main_js:
        size = f.stat().st_size
        over = size > MAX_JS_BYTES
        marker = "❌" if over else "✅"
        print(f"  {marker} {f.name}: {_kb(size)} (budget {_kb(MAX_JS_BYTES)})")
        if over:
            over_budget.append(f"{f.name} ({_kb(size)} > {_kb(MAX_JS_BYTES)})")

    for f in main_css:
        size = f.stat().st_size
        over = size > MAX_CSS_BYTES
        marker = "❌" if over else "✅"
        print(f"  {marker} {f.name}: {_kb(size)} (budget {_kb(MAX_CSS_BYTES)})")
        if over:
            over_budget.append(f"{f.name} ({_kb(size)} > {_kb(MAX_CSS_BYTES)})")

    if over_budget:
        print()
        print("❌ Bundle budget exceeded:")
        for v in over_budget:
            print(f"    {v}")
        print()
        print("Per CLAUDE.md Hard Constraints (foundation audit 2026-05-10):")
        print("    <400K JS + <50K CSS for the main chunk.")
        print()
        print("Options to bring it back under budget, in order of preference:")
        print("  1. Code-split the heavy import (see MapPicker for the pattern)")
        print("  2. Remove an unused dependency from package.json")
        print("  3. Tree-shake aggressively (check for `import *` and barrel files)")
        print("  4. If genuinely justified, edit the budget in BOTH this script AND CLAUDE.md")
        print("     — but never silently. The budget is a load-bearing decision, not a default.")
        return 1

    print()
    print("✅ Bundle within budget.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
